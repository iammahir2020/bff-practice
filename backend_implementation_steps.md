# Backend Implementation Steps — Microservice Producer (Learning Path)

This breaks [`backendPlan.md`](backendPlan.md) into small, independently deployable
increments. The goal isn't to get to the finished architecture fast — it's to
deploy one new piece at a time, watch it work (or fail) in isolation, and
understand *why* it's there before stacking the next piece on top.

Each step lists:
- **Goal** — the one new thing this step adds
- **Build** — what to write/change
- **Concept** — what AWS mechanism you're actually learning here
- **Verify** — how to observe it working before moving on
- **Rollback-safe?** — whether `cdk destroy`-ing just this piece is clean

Do them in order. Don't start a step until the previous one is deployed and
verified — the whole point is seeing each layer light up on its own.

---

## Phase A — Event bus, with no producer or real consumer yet

### Step 1: Create the EventBridge bus alone
- **Goal:** stand up `bff-microservice-bus` with nothing publishing to it and
  nothing subscribed yet.
- **Build:** new `lib/microservice-source-construct.ts` with just:
  ```ts
  const bus = new events.EventBus(this, 'MicroserviceEventBus', {
    eventBusName: 'bff-microservice-bus',
  });
  ```
  Instantiate it from `lib/bff-practice-stack.ts`, output the bus name.
- **Concept:** an event bus is just a named router — it exists independently
  of anything publishing or subscribing to it. Buses are cheap/free to create.
- **Verify:**
  ```
  aws events put-events --entries '[{"EventBusName":"bff-microservice-bus","Source":"manual.test","DetailType":"Ping","Detail":"{}"}]'
  ```
  Should return `FailedEntryCount: 0`. Nothing consumes it yet — that's fine,
  you're just confirming the bus accepts events.
- **Rollback-safe?** Yes — trivial to remove.
- **Console:** **EventBridge → Buses** — `bff-microservice-bus` should be
  listed. Click into it; there's nothing under Rules yet, which is expected.

### Step 2: Add a rule targeting CloudWatch Logs (throwaway target)
- **Goal:** see event *pattern matching* work before wiring anything
  permanent (SQS/Lambda) downstream.
- **Build:** a `logs.LogGroup` + `events.Rule` with
  `eventPattern: { source: ['bff.microservice'] }` and
  `targets: [new targets.CloudWatchLogGroup(logGroup)]`.
- **Concept:** rules match on `source`/`detail-type`/`detail` fields against
  incoming events; unmatched events are silently dropped. This is the
  cheapest way to *see* a rule fire without building a consumer yet.
- **Verify:** `put-events` again with `Source: 'bff.microservice'`, then check
  the log group in CloudWatch Logs Insights — the raw event should appear
  within seconds. Try a mismatched source and confirm nothing shows up.
- **Rollback-safe?** Yes.
- **Console:** **EventBridge → Buses → `bff-microservice-bus` → Rules** —
  your rule appears with its event pattern. **CloudWatch → Log groups** —
  find the new log group (name comes from the `logs.LogGroup` construct id)
  and open its latest log stream to see the raw matched event JSON.

---

## Phase B — Durable delivery: SQS, then Lambda, then real writes

### Step 3: Swap the rule's target to an SQS queue
- **Goal:** replace the CloudWatch Logs target with `ProjectionQueue`
  (no DLQ yet — add that in Step 6, once you've seen the happy path).
- **Build:**
  ```ts
  const projectionQueue = new sqs.Queue(this, 'ProjectionQueue');
  new events.Rule(this, 'OrderUpdatedRule', {
    eventBus: bus,
    eventPattern: { source: ['bff.microservice'] },
    targets: [new targets.SqsQueue(projectionQueue)],
  });
  ```
- **Concept:** `targets.SqsQueue(...)` auto-grants EventBridge permission to
  send to the queue — no manual IAM policy. This is the "durability buffer"
  layer: even with no consumer at all, messages just sit in the queue.
- **Verify:** `put-events` again, then in the SQS console (or
  `aws sqs receive-message`) confirm the message body contains your event.
  Delete it manually to clear the queue.
- **Rollback-safe?** Yes.
- **Console:** **SQS → Queues → `ProjectionQueue`** — the "Messages
  available" count ticks up after `put-events`. Use the **Send and receive
  messages** tab to poll/view/delete a message body directly. The rule's
  target is also visible under **EventBridge → Buses → ... → Rules →
  `OrderUpdatedRule` → Targets**.

### Step 4: Add a consumer Lambda that only logs
- **Goal:** wire `EventConsumerFn` to the queue via `SqsEventSource`, but
  have the handler do nothing but `console.log` each record — no DynamoDB
  writes yet.
- **Build:** `lambda/eventConsumer.js`:
  ```js
  exports.handler = async (event) => {
    for (const record of event.Records) console.log(record.body);
    return {};
  };
  ```
  `eventConsumerFn.addEventSource(new SqsEventSource(projectionQueue, { batchSize: 5 }))`.
- **Concept:** `SqsEventSource.bind()` auto-grants the Lambda
  `sqs:ReceiveMessage`/`DeleteMessage` on the queue — this is the
  poll-and-invoke wiring, distinct from EventBridge's push model in Step 3.
- **Verify:** `put-events` → CloudWatch Logs for `EventConsumerFn` shows the
  message body → the queue's `ApproximateNumberOfMessagesVisible` drops back
  to 0 (Lambda deleted the message after successful processing).
- **Rollback-safe?** Yes.
- **Console:** **Lambda → Functions → `EventConsumerFn`** — the
  **Monitor** tab shows invocation count ticking up; **Monitor → View
  CloudWatch logs** takes you to the log group with your `console.log`
  output. The **Configuration → Triggers** tab shows `ProjectionQueue` as an
  SQS trigger.

### Step 5: Make the consumer actually write to `PriceProjection`
- **Goal:** connect this whole chain back into the *existing*, already-deployed
  BFF stack — this is the first step where you'll see a live update reach
  the React client.
- **Build:** flesh out `eventConsumer.js` to parse
  `orderId`/`status`/`customer` from `detail` and `UpdateItemCommand` into
  the table (start simple — full `PutItem` is fine here, partial-update
  correctness comes in Step 10). Grant access: `props.table.grantWriteData(eventConsumerFn)`.
- **Concept:** this is the moment the new "publisher" path and the old
  "subscriber" path (DynamoDB Stream → `StreamHandlerFn` → AppSync) connect.
  Everything downstream of `PriceProjection` was already built and doesn't
  change at all.
- **Verify:**
  ```
  aws events put-events --entries '[{"EventBusName":"bff-microservice-bus","Source":"bff.microservice","DetailType":"OrderUpdated","Detail":"{\"orderId\":\"test-1\",\"status\":\"PLACED\"}"}]'
  ```
  Then watch the running `client/` app — `test-1` should appear/update live,
  no manual `put-item` CLI call needed for the first time.
- **Rollback-safe?** Yes, but this is the first step that touches the shared
  table — double check you're not clobbering real test data.
- **Console:** **DynamoDB → Tables → `PriceProjection` → Explore table
  items** — `test-1` should appear/update. **AppSync → APIs →
  `BffGraphApi` → Queries** — you can also manually run the
  `onOrderUpdate` subscription here and watch it fire live, no client app
  needed.

### Step 6: Add the dead-letter queue
- **Goal:** add `ProjectionDLQ` and `maxReceiveCount: 3`, plus
  `reportBatchItemFailures: true` on the event source, and update the
  handler to return `batchItemFailures`.
- **Build:** as in `backendPlan.md`'s DLQ section. Also set an explicit
  `timeout` on the Lambda and `visibilityTimeout` on the queue (6x rule).
- **Concept:** failure isolation — a bad message shouldn't block the whole
  queue or retry forever silently.
- **Verify — deliberately break it:** temporarily point `TABLE_NAME` at a
  nonexistent table, send 3 test events, confirm they land in
  `ProjectionDLQ` (check `ApproximateNumberOfMessagesVisible` on the DLQ).
  Then revert and redeploy. This is worth doing once, on purpose, so you've
  actually seen a DLQ populate rather than trusting it in theory.
- **Rollback-safe?** Yes.
- **Console:** **SQS → Queues → `ProjectionDLQ`** — "Messages available"
  rises after the deliberate-break test. **Lambda → `EventConsumerFn` →
  Monitor → CloudWatch logs** shows the errors that triggered each retry.
  **SQS → `ProjectionQueue` → Lambda triggers tab** shows the batch
  failure/report config.

---

## Phase C — The EC2 publisher, built up in isolation

### Step 7: Bare EC2 instance, SSM-only, no app
- **Goal:** stand up the instance with the zero-inbound security group and
  SSM-only reachability — and confirm you can actually reach it — before
  putting any application code on it.
- **Build:** `ec2.Vpc.fromLookup`, `ec2.SecurityGroup` (no `addIngressRule`
  calls), `iam.Role` with just `AmazonSSMManagedInstanceCore`,
  `ec2.Instance` with `vpcSubnets: { subnetType: ec2.SubnetType.PUBLIC }`
  and **no UserData yet**.
- **Concept:** the "zero-ingress + SSM" reachability model, and the default
  VPC's public-subnet quirk (`ec2.InstanceProps.vpcSubnets` defaults to
  private, which a default VPC doesn't have).
- **Verify:**
  ```
  aws ssm start-session --target <instance-id>
  ```
  You should land in a shell with no SSH involved. Confirm from your own
  machine that the instance has no reachable open ports (nothing to curl).
- **Rollback-safe?** Yes — stop or destroy the instance freely, nothing
  depends on it yet.
- **Console:** **EC2 → Instances** — find it by name/tag, check "Status
  check" is 2/2 passed. **EC2 → Security Groups** — confirm the inbound
  rules tab is genuinely empty. **IAM → Roles** — open the instance role and
  confirm it only has `AmazonSSMManagedInstanceCore` attached so far. You
  can also start the SSM session from here: select the instance → **Connect
  → Session Manager → Connect**, instead of the CLI.

### Step 8: Minimal "hello world" HTTP server via UserData
- **Goal:** get *any* Node process running and reachable through an SSM
  port-forward — no business logic, no AWS SDK calls yet.
- **Build:** `microservice/server.js` = a `http.createServer` that responds
  `200 OK` to everything. `microservice/package.json` with no dependencies.
  UserData: `dnf install nodejs20`, download+unzip the S3 asset, systemd
  unit running `node server.js`.
- **Concept:** the app-delivery mechanism (`s3-assets.Asset` +
  `addS3DownloadCommand` + systemd) — decoupled here from any business logic
  so you can debug *just* "did my code make it onto the box and start"
  before adding complexity.
- **Verify:**
  ```
  aws ssm start-session --target <instance-id> \
    --document-name AWS-StartPortForwardingSession \
    --parameters '{"portNumber":["3000"],"localPortNumber":["8080"]}'
  curl localhost:8080
  ```
  If this fails, `journalctl -u microservice -n 50` on the instance (via a
  plain SSM shell session) is where to look.
- **Rollback-safe?** Yes.
- **Console:** **EC2 → Instances → (your instance) → Actions → Monitor and
  troubleshoot → Get system log** if boot-time UserData failed silently.
  **S3 → Buckets** — find the CDK asset bucket (name starts
  `cdk-...-assets-...`) and confirm your `microservice/` zip was uploaded
  under its key. There's no console view of the running process itself —
  that's what the SSM shell + `journalctl` is for.

### Step 9: EC2 publishes its first real event — the chain lights up end to end
- **Goal:** add one endpoint, `POST /orders`, that calls
  `PutEventsCommand` — nothing else yet (no DB, no GET/PUT/DELETE).
- **Build:** add `@aws-sdk/client-eventbridge` to `microservice/package.json`,
  `bus.grantPutEventsTo(instanceRole)` in CDK (scoped IAM — confirm this is
  the *only* permission the instance role has beyond SSM), `EVENT_BUS_NAME`
  env var in the systemd unit.
- **Concept:** this is the full loop closing for the first time:
  `curl` → EC2 → EventBridge → SQS → Lambda → DynamoDB → Stream →
  AppSync → WebSocket → browser. Everything from Steps 1–6 and 7–8 meets
  here.
- **Verify:**
  ```
  curl -X POST localhost:8080/orders -d '{"orderId":"order-001","status":"PLACED"}'
  ```
  Watch it appear live in the client app. Also check the instance role in
  IAM — confirm it genuinely cannot touch DynamoDB directly (try
  `aws dynamodb scan` from inside the SSM session using the instance's own
  credentials and confirm it's denied) — this proves the publisher/BFF IAM
  boundary from the architecture diagram is real, not just documented.
- **Rollback-safe?** Yes.
- **Console:** **IAM → Roles → (instance role) → Permissions** — confirm
  only `AmazonSSMManagedInstanceCore` + the scoped `events:PutEvents`
  inline/managed policy are attached, nothing DynamoDB-related.
  **DynamoDB → `PriceProjection` → Explore table items** and the running
  **client app** — both should show `order-001` live. **CloudWatch →
  Log groups → `EventConsumerFn`** for the processing log line.

### Step 10: Full CRUD + partial-update correctness
- **Goal:** add `GET /orders`, `GET /orders/:orderId`, `PUT /orders/:orderId`
  (partial update), `DELETE /orders/:orderId` — still no database, in-memory
  object is fine for now.
- **Build:** as in `backendPlan.md`'s `server.js`, minus the Postgres calls.
  Update `eventConsumer.js` to switch from `PutItem` to `UpdateItem`
  (dynamic expression, only touching fields present in the event) and add
  `DeleteItemCommand` for `OrderDeleted`.
- **Concept:** this is where the *reason* for `UpdateItem` over `PutItem`
  becomes concrete — do a partial `PUT` (`{"status":"SHIPPED"}` only) and
  watch a naive `PutItem` implementation silently wipe the `customer` field,
  then fix it and watch the field survive.
- **Verify:** run the full CRUD curl sequence from `backendPlan.md` step 5;
  confirm the partial update preserves untouched fields; confirm delete
  removes the DynamoDB item (`aws dynamodb get-item`) even though — note
  this known gap — it won't show live in the client yet (`stream.js` skips
  `REMOVE` events; that's a separate future change, not part of this plan).
- **Rollback-safe?** Yes.
- **Console:** **DynamoDB → `PriceProjection` → Explore table items** —
  watch a partial `PUT` update only the targeted attribute, and confirm the
  item disappears entirely after `DELETE`. **CloudWatch → Log groups →
  `EventConsumerFn`** — should show `Removed projection for: order-001`
  after the delete call.

---

## Phase D — Give the microservice its own database

### Step 11: Aurora Serverless v2 cluster, unconnected
- **Goal:** stand up `MicroserviceDb` and confirm you can reach it from the
  EC2 box with `psql` *manually*, before wiring the app to it.
- **Build:** `rds.DatabaseCluster` with `serverlessV2` writer,
  `publiclyAccessible: false`, `rds.Credentials.fromGeneratedSecret(...)`,
  a dedicated `dbSecurityGroup` allowing 5432 only from `instanceSg`.
  Grant `dbCluster.secret!.grantRead(instanceRole)`.
- **Concept:** Secrets Manager as the credential-delivery mechanism (never
  plaintext in code/UserData), and security-group-to-security-group
  ingress rules (as opposed to CIDR-based rules).
- **Verify:** from an SSM shell on the instance:
  ```
  dnf install -y postgresql16
  aws secretsmanager get-secret-value --secret-id <arn> --query SecretString --output text
  psql "host=<endpoint> port=5432 dbname=orders user=<user> password=<pw> sslmode=require"
  ```
  Confirm you can connect and `SELECT 1;` before any app code touches it.
- **Rollback-safe?** Removing this needs `cdk destroy`/removing the
  construct — not truly instant since RDS teardown takes a few minutes, but
  no other resource depends on it yet.
- **Console:** **RDS → Databases → `MicroserviceDb`** — check status is
  "Available", and its **Serverless v2 capacity** metric (should sit near 0
  ACUs when idle). **Secrets Manager → Secrets** — find the
  auto-generated secret, "Retrieve secret value" to see the generated
  username (password is shown but treat this as sensitive). **VPC →
  Security groups** — confirm the DB security group's inbound rule sources
  the EC2 security group, not a CIDR range.

### Step 12: Wire the app to Postgres — RDS becomes the source of truth
- **Goal:** `initDb()` in `server.js` fetches the secret, connects via `pg`,
  creates the `orders` table, and every CRUD handler writes to Postgres
  *before* publishing to EventBridge.
- **Build:** as in `backendPlan.md`'s final `server.js` + `DB_HOST`/`DB_PORT`/
  `DB_NAME`/`DB_SECRET_ARN` env vars in the systemd unit.
- **Concept:** the "service owns its data" pattern completed — Postgres is
  now the transactional source of truth, EventBridge is purely a
  "something changed" notification, matching how `PriceProjection` and its
  Stream already relate to AppSync.
- **Verify:** repeat the full CRUD curl sequence; after each call, connect
  via `psql` and confirm the row matches; confirm the DynamoDB projection
  and the live client still update as in Step 10 — now driven by real
  persisted state instead of an in-memory object.
- **Rollback-safe?** No — this is the finished shape. From here, changes are
  refinements (see Phase E), not structural additions.
- **Console:** **RDS → Databases → `MicroserviceDb` → Serverless v2
  capacity** graph — ACUs should tick up momentarily on each write, proving
  real traffic (rather than just trusting `psql` output). If your RDS
  engine version supports it, **RDS → Query Editor** can run `SELECT *
  FROM orders;` straight from the console without an SSM session at all.

---

## Phase E — Optional follow-ups (not required to consider the plan "done")

- Fix the "deletes don't show up live" gap noted in Step 10 — requires a
  schema change to `graphql/schema.graphql` and a change to
  `lambda/stream.js`, which currently skips `REMOVE` stream events.
- Add CloudWatch alarms on `ProjectionDLQ` depth (currently just noted as
  "no alarms wired up yet" in the README).
- Cost hygiene: `aws ec2 stop-instances` when not testing (t3.micro bills
  hourly); Aurora already scales to 0 ACUs via
  `serverlessV2AutoPauseDuration`, but storage bills continuously regardless.

---

## Quick reference: what each step teaches

| Step | AWS concept |
|---|---|
| 1 | Event bus as a standalone router |
| 2 | EventBridge rule pattern matching |
| 3 | EventBridge → SQS auto-granted permissions |
| 4 | SQS → Lambda polling/invoke wiring |
| 5 | Connecting a new path into an existing pipeline |
| 6 | DLQ / failure isolation, `batchItemFailures` |
| 7 | Zero-ingress security groups + SSM Session Manager |
| 8 | UserData + S3 asset app delivery + systemd |
| 9 | Scoped IAM (`grantPutEventsTo`) + end-to-end verification |
| 10 | `UpdateItem` vs `PutItem` semantics under partial updates |
| 11 | Secrets Manager + security-group-to-security-group ingress |
| 12 | Service-owns-its-data pattern; DB as source of truth vs. event bus as notification |
