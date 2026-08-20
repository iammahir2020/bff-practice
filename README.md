# bff-practice

A BFF (Backend-For-Frontend) on AWS: a React client reads orders over a REST API and stays live via a GraphQL subscription, while the read model in DynamoDB is kept in sync by a change-data-capture pipeline — a DynamoDB Streams trigger that republishes every write through AppSync to any subscribed client. Everything is provisioned with AWS CDK.

On top of that, a second, decoupled write path is being built incrementally: an EventBridge bus → SQS → Lambda pipeline that writes into the same read model, eventually fed by a real EC2-hosted microservice instead of manual CLI calls. See [Producer pipeline](#producer-pipeline-eventbridge--sqs--lambda) below and [`backend_implementation_steps.md`](backend_implementation_steps.md) for the step-by-step build log.

## Architecture

```
                                ┌──────────────────────────────────────────┐
                                │        BROWSER  (React + Amplify)        │
                                │  • sign in → holds Cognito JWT           │
                                │  • Channel 1: fetch() initial list       │
                                │  • Channel 2: subscribe() for live delta │
                                └────┬────────────────────────────▲────────┘
                                     │                             │
                 ┌───────────────────┘                             └──────────────────┐
                 │ (1) GET /prod + JWT                     (6) push order delta        │
                 │     [ HTTPS ]                               [ WebSocket / wss ]     │
                 ▼                                                                     │
      ┌──────────────────────┐                                        ┌────────────────┴───────────┐
      │     API GATEWAY      │                                        │          APPSYNC            │
      │      (BffApi)        │                                        │       (BffGraphApi)         │
      │  ┌─────────────────┐ │                                        │  ┌────────────────────────┐ │
      │  │ Cognito         │ │◄───── validates JWT ─────┐              │  │ subscription           │ │
      │  │ Authorizer      │ │                          │              │  │ onOrderUpdate          │ │
      │  └─────────────────┘ │                          │              │  │ (Cognito user-pool auth)│ │
      └──────────┬────────────┘                         │              │  └────────────▲───────────┘ │
                 │ (2) forward request                  │              │  ┌────────────┴───────────┐ │
                 ▼                                       │              │  │ mutation                │ │
      ┌──────────────────────┐                  ┌────────┴───────┐     │  │ publishOrderUpdate      │ │
      │       LAMBDA          │                  │    COGNITO     │     │  │ → NONE data source      │ │
      │     GetOrdersFn       │                  │   User Pool    │     │  │   (echoes input back,   │ │
      │    (handler.js)       │                  │  (BffUserPool) │     │  │    writes nothing)      │ │
      │  scan + return JSON   │                  └────────────────┘     │  └────────────▲───────────┘ │
      └──────────┬─────────────┘                                       └───────────────┼──────────────┘
                 │ (3) Scan  [IAM: table.grantReadData]                                 │
                 ▼                                                       (5) signed publish mutation
      ┌───────────────────────────────────────┐                            [HTTPS + SigV4 / IAM]
      │              DYNAMODB                 │                                       │
      │         PriceProjection table         │                          ┌────────────┴───────────┐
      │      (the read model / projection)    │                          │        LAMBDA           │
      └──────┬─────────────────────▲──────────┘                          │    StreamHandlerFn      │
             │                     │                                     │      (stream.js)        │
             │ (b) Stream          │ (a) write                           │  reads NewImage, SigV4-  │
             │  (NEW_IMAGE)        │  put-item / update-item             │  signs + calls the       │
             ▼                     │  — manual AWS CLI/Console, or the   │  mutation                │
   ┌──────────────────────┐        │  producer pipeline below            └────────────▲─────────────┘
   │ EVENT SOURCE MAPPING  │        │  (EventBridge → SQS → Lambda)                   │
   │ (AWS-managed; polls   │────────┼───────────────(4) invokes StreamHandlerFn───────┘
   │ the stream, invokes   │        │                with the changed records
   │ StreamHandlerFn)      │
   └───────────────────────┘
```

### Request/update flow

1. The browser calls `GET /` on API Gateway with a Cognito ID token in the `Authorization` header.
2. API Gateway's built-in Cognito authorizer validates the token and forwards the request.
3. `GetOrdersFn` scans the `PriceProjection` DynamoDB table and returns the items as JSON.
4. Any write to `PriceProjection` — today, either a manual `put-item`/`update-item`, or an event sent through the [producer pipeline](#producer-pipeline-eventbridge--sqs--lambda) below — emits a DynamoDB Stream record (`NEW_IMAGE`). An AWS-managed event source mapping polls the stream and invokes `StreamHandlerFn` with the changed records.
5. `StreamHandlerFn` SigV4-signs a `publishOrderUpdate` mutation (using `aws4` + its own Lambda execution-role credentials — no API key) and posts it straight to AppSync.
6. AppSync's `NONE` data source resolver echoes the mutation input straight back out through the `onOrderUpdate` subscription — nothing is persisted by AppSync itself, DynamoDB is the only store.
7. Every browser subscribed over the Cognito-authenticated WebSocket receives the delta live, no polling.

Both the REST API and the AppSync subscription trust the *same* Cognito user pool — one login, two live channels. AppSync also accepts an IAM auth mode, but only `StreamHandlerFn` uses it (scoped narrowly via `graph.grantMutation` to just the `publishOrderUpdate` mutation) — browsers only ever authenticate with the user pool.

## Producer pipeline (EventBridge → SQS → Lambda)

Built incrementally per [`backend_implementation_steps.md`](backend_implementation_steps.md) (Steps 1–6 complete as of this writing). This is a second, decoupled write path into `PriceProjection`, designed to eventually be fed by a real EC2-hosted microservice (Steps 7–12, not yet built) instead of manual CLI calls.

```
      ┌─────────────────────────────┐
      │   AWS CLI (`put-events`)     │
      │   — stand-in for the EC2     │
      │   publisher, which is        │
      │   Steps 7–12, not yet built  │
      └──────────────┬────────────────┘
                     │ source: bff.microservice
                     ▼
      ┌──────────────────────────────┐
      │          EVENTBRIDGE          │
      │    bus: bff-microservice-bus  │
      │  ┌──────────────────────────┐ │
      │  │  OrderUpdatedRule         │ │
      │  │  pattern: source =        │ │
      │  │    bff.microservice       │ │
      │  └────────────┬─────────────┘ │
      └───────────────┼────────────────┘
                      ▼
      ┌──────────────────────────────┐        ┌───────────────────────┐
      │              SQS               │─3 fails▶│     ProjectionDLQ      │
      │        ProjectionQueue         │        │ (14-day retention)     │
      │   visibilityTimeout: 60s       │        └───────────────────────┘
      └───────────────┬────────────────┘
                      │ SqsEventSource (batchSize 5,
                      │ reportBatchItemFailures: true)
                      ▼
      ┌──────────────────────────────┐
      │             LAMBDA             │
      │        EventConsumerFn         │
      │  UpdateItem — only touches     │
      │  fields present in the event,  │
      │  so a partial update doesn't   │
      │  clobber the rest of the item  │
      └───────────────┬────────────────┘
                      ▼
      ┌──────────────────────────────┐
      │           DYNAMODB             │
      │        PriceProjection         │──▶ same table as above — everything
      └──────────────────────────────┘     downstream (Stream → AppSync →
                                            client) triggers exactly as usual
```

### Producer flow

1. A publisher — currently `aws events put-events` via the CLI; eventually the EC2 microservice (Steps 7–12) — sends a domain event to the `bff-microservice-bus` EventBridge bus with `source: bff.microservice`.
2. `OrderUpdatedRule` matches the event pattern (`source = bff.microservice`) and routes it to `ProjectionQueue` (SQS). Events that don't match any rule's pattern are silently dropped — nothing downstream ever sees them.
3. `ProjectionQueue` durably buffers the message until `EventConsumerFn` consumes it. If processing fails `maxReceiveCount` (3) times, SQS moves that message to `ProjectionDLQ` instead of retrying forever or losing it.
4. An SQS event source invokes `EventConsumerFn` in batches (up to 5 messages). Each record's `body` is the full EventBridge event, JSON-stringified by SQS — the handler parses `detail.orderId`/`status`/`customer` back out of it.
5. `EventConsumerFn` runs a dynamically built `UpdateItem` against `PriceProjection`, only setting the attributes actually present in the event — a status-only update leaves `customer` (or any other existing field) untouched rather than overwriting the whole item.
6. That write lands in the exact same `PriceProjection` table the read model already uses, so it triggers the identical DynamoDB Stream → `StreamHandlerFn` → AppSync → subscribed clients chain described in steps 4–7 of the [Request/update flow](#requestupdate-flow) above — no separate live-update path to maintain.
7. If a batch partially fails, `reportBatchItemFailures: true` means only the failed message IDs go back to SQS for retry — one bad message doesn't force the whole batch to be reprocessed.

Trigger it manually (until the EC2 publisher exists):
```
aws events put-events --entries '[{
  "EventBusName": "bff-microservice-bus",
  "Source": "bff.microservice",
  "DetailType": "OrderUpdated",
  "Detail": "{\"orderId\":\"order-001\",\"status\":\"SHIPPED\",\"customer\":\"arafat\"}"
}]'
```
Fields omitted from `Detail` (e.g. sending `status` only) are left untouched on the existing item — verified by sending a status-only update and confirming `customer` survives.

`EventConsumerFn`'s own permissions are scoped narrowly: it can `PutItem`/`UpdateItem`/`DeleteItem` on `PriceProjection` and consume/delete messages on `ProjectionQueue` — nothing else.

## Project structure

```
bin/bff-practice.ts                  CDK app entry point
lib/bff-practice-stack.ts            The whole stack: DynamoDB, Lambdas, API Gateway, Cognito,
                                      AppSync, X-Ray tracing, CloudWatch dashboard
lib/microservice-source-construct.ts Producer pipeline: EventBridge bus + rule, SQS queue + DLQ,
                                      EventConsumerFn (Steps 1–6 of backend_implementation_steps.md;
                                      EC2 publisher itself is Steps 7–12, not yet built)
lambda/
  handler.js                GetOrdersFn — scans PriceProjection, returns JSON
  stream.js                 StreamHandlerFn — SigV4-signs & posts publishOrderUpdate to AppSync
  eventConsumer.js          EventConsumerFn — SQS-triggered, UpdateItem into PriceProjection
                             (partial update; reports per-message batch failures)
  package.json               aws4 dependency — installed automatically before every cdk command
graphql/schema.graphql      AppSync schema (Query/Mutation/Subscription + auth directives)
client/                     React + Vite frontend (Amplify Auth + Amplify GraphQL subscription)
test/                       Jest scaffold for the CDK stack (currently a stub, no real coverage yet)
backendPlan.md                       Original end-state design doc for the full EC2 microservice
backend_implementation_steps.md      Step-by-step incremental build log/checklist derived from it —
                                      the source of truth for what's actually been built so far
```

## Prerequisites

- Node.js 20+
- An AWS account and credentials configured locally (`aws configure` or equivalent)
- AWS CDK bootstrapped in the target account/region (`cdk bootstrap`)
- If you want to trigger the [producer pipeline](#producer-pipeline-eventbridge--sqs--lambda) directly via `aws events put-events` (rather than only through `cdk deploy`, which uses CloudFormation's own execution role), your CLI IAM user needs `events:PutEvents` permission. In this project that was hit as a real gap: the deploying user (`bff-project-cli`) had full-access policies for every other service but none for EventBridge, and was already at AWS's 10-managed-policy-per-user quota — worked around with an inline policy instead of a managed one:
  ```
  aws iam put-user-policy --user-name bff-project-cli --policy-name EventBridgeFullAccessInline \
    --policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"events:*","Resource":"*"}]}'
  ```

## Deploy the backend

```
npm install
npx cdk deploy    # provisions everything in lib/bff-practice-stack.ts
```

Note: `cdk.json`'s app command also runs `npm install --prefix lambda` automatically before every `synth`/`diff`/`deploy`. This installs `lambda/`'s own dependencies (currently just `aws4`) into `lambda/node_modules` so they're bundled into the deployment zip — omitting this step previously caused `StreamHandlerFn` to crash on every invocation with `Cannot find module 'aws4'`, since the Lambda runtime doesn't expose third-party packages on its own.

The stack outputs `ApiUrl`, `GraphqlUrl`, `UserPoolId`, `UserPoolClientId`, `DashboardUrl`, and `MicroserviceEventBusName` (the producer pipeline's bus, currently `bff-microservice-bus`) — you'll need the first four for the frontend `.env`.

Other useful commands:

```
npm run watch      # tsc in watch mode (type-check only; the app itself runs via tsx, nothing is emitted)
npm test           # run Jest — currently a stub, not real coverage
npx cdk diff        # compare deployed stack with current state
npx cdk synth        # emit the synthesized CloudFormation template
```

## Run the frontend

```
cd client
cp .env.example .env   # fill in with the CDK stack outputs
npm install
npm run dev
```

`client/.env` expects:

```
VITE_API_URL=              # ApiUrl stack output
VITE_GRAPHQL_URL=          # GraphqlUrl stack output
VITE_USER_POOL_ID=         # UserPoolId stack output
VITE_USER_POOL_CLIENT_ID=  # UserPoolClientId stack output
VITE_AWS_REGION=           # AWS region the stack was deployed to
```

Sign up a user (self sign-up is enabled on the user pool, email as the sign-in alias) to log in and see the initial order list load, then watch it update live as writes land in `PriceProjection` — see [`client/README.md`](client/README.md) for the full walkthrough, including how to trigger a live update manually via the CLI.

## Monitoring

The stack provisions a single CloudWatch dashboard (`BFF-Live-Health`) covering both pipelines — find its link in the `DashboardUrl` stack output:
- **Subscriber side:** Lambda invocations/errors/duration (`GetOrdersFn`, `StreamHandlerFn`), API Gateway requests/errors, DynamoDB read/write capacity.
- **Producer side:** `OrderUpdatedRule` invocations/failures, `EventConsumerFn` invocations/errors, `ProjectionQueue`/`ProjectionDLQ` depth, and a `DLQ Backlog (current)` single-value widget.

More widgets get added here incrementally as later steps introduce new resources (EC2 instance metrics in Step 7, RDS ACU capacity in Step 11) — see `backend_implementation_steps.md`'s `Dashboard` notes on those steps.

`GetOrdersFn` and `StreamHandlerFn` (plus the REST API stage) have X-Ray tracing enabled (AppSync tracing is intentionally left off — enabling it forces CloudFormation to replace the GraphQL API and its dependent resources, changing the URL). `EventConsumerFn` doesn't have X-Ray tracing enabled yet. No alarms or email notifications are wired up yet.

## Roadmap

[`backendPlan.md`](backendPlan.md) is the original end-state design for an EC2-hosted microservice that publishes domain events through EventBridge to a consumer Lambda, writing into `PriceProjection` as a second, decoupled write path — with its own Aurora Serverless v2 Postgres database as the microservice's source of truth.

[`backend_implementation_steps.md`](backend_implementation_steps.md) breaks that into small, incrementally deployable/verifiable steps. Progress so far:

- [x] **Steps 1–6 (Phase A + B)** — EventBridge bus, rule, SQS queue + DLQ, and `EventConsumerFn` writing into `PriceProjection`. This is everything described in [Producer pipeline](#producer-pipeline-eventbridge--sqs--lambda) above, currently triggered manually via `aws events put-events`.
- [ ] **Steps 7–10 (Phase C)** — the actual EC2 publisher: bare instance reachable only via SSM, a Node HTTP server delivered via UserData, full CRUD (`/orders`), publishing real events instead of manual CLI calls.
- [ ] **Steps 11–12 (Phase D)** — Aurora Serverless v2 Postgres as the microservice's own database, so RDS becomes the source of truth instead of an in-memory object.
- [ ] **Phase E (optional)** — fix live-delete propagation (`stream.js` currently skips DynamoDB Stream `REMOVE` events), DLQ depth alarms, cost hygiene.
