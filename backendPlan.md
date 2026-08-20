# Microservice event source: EC2 → EventBridge → Lambda → DynamoDB

## Context

The BFF stack already implements the "subscriber" half of the AWS BFF reference pattern (https://aws.amazon.com/blogs/mobile/backends-for-frontends-pattern/): DynamoDB `PriceProjection` table → Stream → `StreamHandlerFn` → AppSync `publishOrderUpdate` mutation (IAM) → `onOrderUpdate` subscription (Cognito) → `client/` React app. Right now the only way to trigger an update is a manual `aws dynamodb put-item` CLI call. The goal is to build the "publisher" half from the reference diagrams — a real microservice event source running on EC2 — so we can learn the full pattern end to end: a service call → an event bus → a BFF event consumer → the projection table → everything already built downstream.

Architecture decisions already confirmed:
1. **Event delivery:** EC2 → EventBridge (custom bus) → consumer Lambda → DynamoDB `PutItem`. The EC2 role gets *only* `events:PutEvents` scoped to that bus — no DynamoDB access, preserving the publisher/BFF boundary shown in the diagrams.
2. **Trigger:** a small HTTP server on EC2 exposing full CRUD for orders (`POST /orders`, `GET /orders`, `GET /orders/:orderId`, `PUT /orders/:orderId`, `DELETE /orders/:orderId`) that gets curled to simulate real business operations.
3. **IAM prerequisite:** `bff-project-cli` (the deploying IAM user) currently has zero EC2 permissions (confirmed via `aws iam list-attached-user-policies` — has API Gateway/Cognito/DynamoDB/S3/CloudFormation/Lambda/IAM/CloudWatch/SSM full-access policies, nothing EC2). Run the attach-policy command manually before any deploy touches EC2.
4. **Reachability:** SSM Session Manager port-forwarding only — the security group has **zero inbound rules**. No SSH keys, nothing internet-reachable.
5. **VPC:** use the existing default VPC (`vpc-06f6bae0cfd3858de`, confirmed present via `aws ec2 describe-vpcs`) via `ec2.Vpc.fromLookup`. No new VPC/NAT gateway.
6. **Sizing:** `t3.micro` — ongoing hourly cost (unlike the rest of this stack, which is fully pay-per-request).
7. **Durability: SQS + DLQ between the rule and the consumer.** The EventBridge rule's target is an SQS queue (`ProjectionQueue`), not `EventConsumerFn` directly — the Lambda is triggered by the queue instead. If `EventConsumerFn` fails to process a message (bug, DynamoDB throttled, bad payload) more than `maxReceiveCount` times, SQS moves it to a dead-letter queue (`ProjectionDLQ`) instead of losing it or retrying forever. This is scoped to the SQS→Lambda hop only — EventBridge-to-SQS delivery failures (rare; would need a broken permission/policy) don't get a separate DLQ, to keep one failure-handling layer instead of two.
8. **The microservice owns its own database.** The EC2 microservice writes orders into its own Aurora Serverless v2 Postgres database (`MicroserviceDb`) — a realistic microservice pattern (each service owns its data) — then still publishes to EventBridge exactly as before. This is scoped entirely to the microservice; `PriceProjection` (DynamoDB) and everything downstream of it stay exactly as already deployed. See the dedicated section below.

All CDK API calls below were verified directly against the installed `aws-cdk-lib@2.265.0` `.d.ts` files (not recalled from memory).

## Key API/design notes

- **No special "zero-ingress" flag** — a `new ec2.SecurityGroup(this, id, { vpc, allowAllOutbound: true })` simply has no ingress rules unless `addIngressRule(...)` is called. Don't call it.
- **`ec2.InstanceProps.vpcSubnets` defaults to private subnets** — a default VPC has none (only `PUBLIC`-classified subnets, routed to an IGW). Must explicitly pass `vpcSubnets: { subnetType: ec2.SubnetType.PUBLIC }` or `cdk deploy` fails with "no subnets of type PRIVATE_WITH_EGRESS found." The instance will end up with a public IP as a side effect (default-VPC public subnets auto-assign one) — that's fine, it's still unreachable since the security group has no inbound rules.
- **Scoped IAM grant:** use `eventBus.grantPutEventsTo(instanceRole)` — a native method on `EventBus`/`IEventBus`, scoped to that one bus's ARN. Don't hand-write an inline policy.
- **AMI:** `ec2.MachineImage.latestAmazonLinux2023()` — current, non-deprecated factory method.
- **UserData:** `ec2.UserData.forLinux()`, `.addCommands(...)`, `.addS3DownloadCommand({ bucket, bucketKey })` (returns the local download path as a string, usable in later `addCommands`).
- **App delivery:** `aws-cdk-lib/aws-s3-assets`'s `Asset` construct (bundled in `aws-cdk-lib`, no extra npm install) — uploads the local `microservice/` folder, `.grantRead(instanceRole)`, then UserData downloads + unzips + `npm install --omit=dev` + runs via systemd (more reboot-safe than `nohup`).
- **Node version on AL2023:** install the explicit versioned package `dnf install -y nodejs20` rather than the bare `nodejs` alias (whose default major version isn't reliably predictable) — deterministic, matches the runtime the rest of this repo targets.
- **First deploy triggers a live VPC context lookup** (`ec2.Vpc.fromLookup`), which creates `cdk.context.json` in the repo root. This should be committed for reproducible synths, same as any CDK project using context lookups.
- **SQS dead-letter queue:** `sqs.QueueProps.deadLetterQueue?: { queue: IQueue, maxReceiveCount: number }` — confirmed at `aws-sqs/lib/queue.d.ts:75,177,183`.
- **EventBridge → SQS target:** `targets.SqsQueue(queue)` from `aws-cdk-lib/aws-events-targets` — confirmed its `bind()` automatically calls `queue.grantSendMessages(new iam.ServicePrincipal('events.amazonaws.com', {...}))`, scoped to that rule's ARN. No manual IAM policy needed for the EventBridge→SQS hop.
- **SQS → Lambda trigger:** `SqsEventSource(queue, props?)` from `aws-cdk-lib/aws-lambda-event-sources` (same package `DynamoEventSource` already comes from in `lib/bff-practice-stack.ts`) — confirmed its `bind()` automatically calls `queue.grantConsumeMessages(target)` on the Lambda's role. `SqsEventSourceProps.reportBatchItemFailures` exists — worth enabling so one bad message in a batch doesn't force the whole batch to be retried.
- **Visibility timeout:** AWS recommends the queue's `visibilityTimeout` be at least 6× the consumer Lambda's `timeout`, so a message isn't redelivered to a second concurrent invocation while the first is still processing it. Set `EventConsumerFn`'s `timeout` explicitly (e.g. `cdk.Duration.seconds(10)`) and the queue's `visibilityTimeout` accordingly (e.g. `cdk.Duration.seconds(60)`) rather than relying on defaults.

## Files to create/edit

| File | Purpose |
|---|---|
| `lib/microservice-source-construct.ts` (new) | `MicroserviceEventSource` construct: EventBridge bus + rule, consumer Lambda, VPC lookup, security group, instance role, S3 asset, UserData, EC2 instance |
| `lib/bff-practice-stack.ts` (edit) | Instantiate `MicroserviceEventSource(this, ..., { table })`; add `CfnOutput`s for instance ID + bus name |
| `microservice/server.js` (new) | Plain Node `http` server — full CRUD over `orders` against its own Postgres DB, publishing `OrderCreated`/`OrderUpdated`/`OrderDeleted` to EventBridge on each mutation |
| `microservice/package.json` (new) | Declares `@aws-sdk/client-eventbridge`, `pg`, `@aws-sdk/client-secrets-manager`; installed via real `npm install` on the instance at boot (no Lambda-style bundling workaround needed here) |
| `lambda/eventConsumer.js` (new) | EventBridge-triggered Lambda; `PutItem` into `PriceProjection` via `@aws-sdk/client-dynamodb` (runtime-provided, like `handler.js` already relies on — no `lambda/package.json` dependency needed for this one) |

No `.gitignore` changes needed — the existing bare `node_modules` rule already covers `microservice/node_modules` at any depth, same as it already covers `lambda/node_modules`.

## `lib/microservice-source-construct.ts`

```ts
import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as path from 'path';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as events from 'aws-cdk-lib/aws-events';
import * as targets from 'aws-cdk-lib/aws-events-targets';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import { SqsEventSource } from 'aws-cdk-lib/aws-lambda-event-sources';
import * as sqs from 'aws-cdk-lib/aws-sqs';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as s3assets from 'aws-cdk-lib/aws-s3-assets';

export interface MicroserviceEventSourceProps {
  readonly table: dynamodb.Table;
}

export class MicroserviceEventSource extends Construct {
  public readonly instanceId: string;
  public readonly eventBusName: string;

  constructor(scope: Construct, id: string, props: MicroserviceEventSourceProps) {
    super(scope, id);
    const stack = cdk.Stack.of(this);

    // --- EventBridge bus + BFF event consumer ---
    const bus = new events.EventBus(this, 'MicroserviceEventBus', {
      eventBusName: 'bff-microservice-bus',
    });

    const eventConsumerFn = new lambda.Function(this, 'EventConsumerFn', {
      runtime: lambda.Runtime.NODEJS_20_X,
      handler: 'eventConsumer.handler',
      code: lambda.Code.fromAsset('lambda'),
      environment: { TABLE_NAME: props.table.tableName },
      timeout: cdk.Duration.seconds(10),
    });
    props.table.grantWriteData(eventConsumerFn);

    // --- Durability: SQS buffer + DLQ between the rule and the consumer ---
    const projectionDlq = new sqs.Queue(this, 'ProjectionDLQ', {
      retentionPeriod: cdk.Duration.days(14),
    });
    const projectionQueue = new sqs.Queue(this, 'ProjectionQueue', {
      visibilityTimeout: cdk.Duration.seconds(60),   // >= 6x EventConsumerFn's timeout
      deadLetterQueue: { queue: projectionDlq, maxReceiveCount: 3 },
    });
    eventConsumerFn.addEventSource(new SqsEventSource(projectionQueue, {
      batchSize: 5,
      reportBatchItemFailures: true,
    }));

    new events.Rule(this, 'OrderUpdatedRule', {
      eventBus: bus,
      eventPattern: {
        source: ['bff.microservice'],
        detailType: ['OrderCreated', 'OrderUpdated', 'OrderDeleted'],
      },
      targets: [new targets.SqsQueue(projectionQueue)],
    });

    // --- EC2 publisher ---
    const vpc = ec2.Vpc.fromLookup(this, 'DefaultVpc', { isDefault: true });

    const instanceSg = new ec2.SecurityGroup(this, 'MicroserviceSg', {
      vpc,
      description: 'Microservice EC2 — no inbound rules, reachable only via SSM port forwarding',
      allowAllOutbound: true,
    });
    // No addIngressRule(...) calls => zero inbound rules.

    const instanceRole = new iam.Role(this, 'MicroserviceInstanceRole', {
      assumedBy: new iam.ServicePrincipal('ec2.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonSSMManagedInstanceCore'),
      ],
    });
    bus.grantPutEventsTo(instanceRole);   // scoped to this bus's ARN only, no DynamoDB access

    const asset = new s3assets.Asset(this, 'MicroserviceAsset', {
      path: path.join(__dirname, '..', 'microservice'),
    });
    asset.grantRead(instanceRole);

    const userData = ec2.UserData.forLinux();
    userData.addCommands(
      'dnf update -y',
      'dnf install -y nodejs20 unzip',
      'mkdir -p /opt/microservice',
    );
    const zipPath = userData.addS3DownloadCommand({
      bucket: asset.bucket,
      bucketKey: asset.s3ObjectKey,
    });
    userData.addCommands(
      `unzip -o ${zipPath} -d /opt/microservice`,
      'cd /opt/microservice && npm install --omit=dev',
      `cat > /etc/systemd/system/microservice.service <<'UNIT'
[Unit]
Description=BFF microservice event source
After=network.target

[Service]
Type=simple
WorkingDirectory=/opt/microservice
ExecStart=/usr/bin/node server.js
Restart=on-failure
Environment=PORT=3000
Environment=EVENT_BUS_NAME=${bus.eventBusName}
Environment=AWS_REGION=${stack.region}

[Install]
WantedBy=multi-user.target
UNIT`,
      'systemctl daemon-reload',
      'systemctl enable --now microservice.service',
    );

    const instance = new ec2.Instance(this, 'MicroserviceInstance', {
      vpc,
      vpcSubnets: { subnetType: ec2.SubnetType.PUBLIC },  // default VPC has no private subnets
      instanceType: ec2.InstanceType.of(ec2.InstanceClass.T3, ec2.InstanceSize.MICRO),
      machineImage: ec2.MachineImage.latestAmazonLinux2023(),
      securityGroup: instanceSg,
      role: instanceRole,
      userData,
    });

    this.instanceId = instance.instanceId;
    this.eventBusName = bus.eventBusName;
  }
}
```

Wiring into `lib/bff-practice-stack.ts` (after the existing `table`/stream/AppSync setup):
```ts
import { MicroserviceEventSource } from './microservice-source-construct';
...
const microservice = new MicroserviceEventSource(this, 'MicroserviceEventSource', { table });
new cdk.CfnOutput(this, 'MicroserviceInstanceId', { value: microservice.instanceId });
new cdk.CfnOutput(this, 'MicroserviceEventBusName', { value: microservice.eventBusName });
```

## `microservice/server.js`

Full CRUD over `orders`, plain Node `http` module (no framework, matching `lambda/*.js` style). Every mutating call (create/update/delete) writes to Postgres first, then publishes a matching domain event — RDS is the source of truth, EventBridge is purely a "something changed" notification to the BFF side:

| Method | Path | Behavior |
|---|---|---|
| `POST` | `/orders` | Create — body needs `orderId` (+ optional `status`/`customer`); `409` if `orderId` already exists. Publishes `OrderCreated`. |
| `GET` | `/orders` | List all orders. |
| `GET` | `/orders/:orderId` | Read one; `404` if missing. |
| `PUT` | `/orders/:orderId` | Update — body may include `status` and/or `customer`; only the fields provided change (`404` if the order doesn't exist). Publishes `OrderUpdated` with only the changed fields. |
| `DELETE` | `/orders/:orderId` | Delete; `404` if missing. Publishes `OrderDeleted`. |

```js
const http = require('http');
const { URL } = require('url');
const { EventBridgeClient, PutEventsCommand } = require('@aws-sdk/client-eventbridge');
const { SecretsManagerClient, GetSecretValueCommand } = require('@aws-sdk/client-secrets-manager');
const { Pool } = require('pg');

const PORT = process.env.PORT || 3000;
const EVENT_BUS_NAME = process.env.EVENT_BUS_NAME;
const ebClient = new EventBridgeClient({});
let pool;

async function initDb() {
  const sm = new SecretsManagerClient({});
  const { SecretString } = await sm.send(new GetSecretValueCommand({ SecretId: process.env.DB_SECRET_ARN }));
  const { username, password } = JSON.parse(SecretString);

  pool = new Pool({
    host: process.env.DB_HOST,
    port: Number(process.env.DB_PORT),
    database: process.env.DB_NAME,
    user: username,
    password,
    ssl: { rejectUnauthorized: false },
  });

  await pool.query(`
    CREATE TABLE IF NOT EXISTS orders (
      order_id TEXT PRIMARY KEY,
      status TEXT,
      customer TEXT,
      updated_at TIMESTAMPTZ DEFAULT now()
    )
  `);
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    let data = '';
    req.on('data', (c) => (data += c));
    req.on('end', () => resolve(data));
    req.on('error', reject);
  });
}

async function publish(detailType, detail) {
  await ebClient.send(new PutEventsCommand({
    Entries: [{
      EventBusName: EVENT_BUS_NAME,
      Source: 'bff.microservice',
      DetailType: detailType,
      Detail: JSON.stringify(detail),
    }],
  }));
}

function sendJson(res, statusCode, body) {
  res.writeHead(statusCode, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify(body));
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, `http://${req.headers.host}`);
  const parts = url.pathname.split('/').filter(Boolean);   // e.g. ['orders', 'order-001']
  if (parts[0] !== 'orders') return sendJson(res, 404, { error: 'not found' });
  const orderId = parts[1] ? decodeURIComponent(parts[1]) : undefined;

  try {
    if (req.method === 'GET' && !orderId) {
      const { rows } = await pool.query(
        'SELECT order_id, status, customer, updated_at FROM orders ORDER BY updated_at DESC',
      );
      return sendJson(res, 200, rows);
    }

    if (req.method === 'GET' && orderId) {
      const { rows } = await pool.query(
        'SELECT order_id, status, customer, updated_at FROM orders WHERE order_id = $1',
        [orderId],
      );
      if (rows.length === 0) return sendJson(res, 404, { error: 'not found' });
      return sendJson(res, 200, rows[0]);
    }

    if (req.method === 'POST' && !orderId) {
      const body = JSON.parse((await readBody(req)) || '{}');
      const { orderId: newId, status, customer } = body;
      if (!newId) return sendJson(res, 400, { error: 'orderId is required' });

      const existing = await pool.query('SELECT 1 FROM orders WHERE order_id = $1', [newId]);
      if (existing.rows.length > 0) return sendJson(res, 409, { error: `order ${newId} already exists` });

      await pool.query(
        'INSERT INTO orders (order_id, status, customer, updated_at) VALUES ($1, $2, $3, now())',
        [newId, status ?? null, customer ?? null],
      );
      await publish('OrderCreated', { orderId: newId, status, customer });
      return sendJson(res, 201, { created: true, orderId: newId });
    }

    if (req.method === 'PUT' && orderId) {
      const body = JSON.parse((await readBody(req)) || '{}');
      const { status, customer } = body;
      if (status === undefined && customer === undefined) {
        return sendJson(res, 400, { error: 'body must include status and/or customer' });
      }
      const result = await pool.query(
        `UPDATE orders SET
           status = COALESCE($2, status),
           customer = COALESCE($3, customer),
           updated_at = now()
         WHERE order_id = $1`,
        [orderId, status ?? null, customer ?? null],
      );
      if (result.rowCount === 0) return sendJson(res, 404, { error: 'not found' });
      await publish('OrderUpdated', { orderId, status, customer });
      return sendJson(res, 200, { updated: true, orderId });
    }

    if (req.method === 'DELETE' && orderId) {
      const result = await pool.query('DELETE FROM orders WHERE order_id = $1', [orderId]);
      if (result.rowCount === 0) return sendJson(res, 404, { error: 'not found' });
      await publish('OrderDeleted', { orderId });
      return sendJson(res, 200, { deleted: true, orderId });
    }

    return sendJson(res, 404, { error: 'not found' });
  } catch (err) {
    console.error('request failed', err);
    return sendJson(res, 500, { error: 'internal error' });
  }
});

initDb()
  .then(() => server.listen(PORT, () => console.log(`microservice listening on :${PORT}`)))
  .catch((err) => {
    console.error('failed to initialize database', err);
    process.exit(1);
  });
```

Note on `PUT`: it behaves as a partial update (only the fields you send change) rather than a strict full-replace, since `status`/`customer` are the only two mutable fields and losing one by omitting it from the body would be surprising. `OrderUpdated` events likewise only carry the fields that were actually changed — which is exactly why `lambda/eventConsumer.js` below needed to change from `PutItem` to `UpdateItem`.

`microservice/package.json`:
```json
{
  "name": "bff-practice-microservice",
  "private": true,
  "main": "server.js",
  "dependencies": {
    "@aws-sdk/client-eventbridge": "^3.700.0"
  }
}
```

## `lambda/eventConsumer.js`

**Shape change from the original no-queue design:** since the rule now targets `ProjectionQueue` (SQS) instead of `EventConsumerFn` directly, the Lambda is invoked by SQS, not EventBridge. SQS wraps the *entire* EventBridge event as a JSON string in each record's `body` — the handler no longer receives `{ detail: {...} }` directly, it receives `{ Records: [{ body: "<JSON-stringified EventBridge event>", messageId, ... }] }`. And because `reportBatchItemFailures: true` is set on the event source, the handler must return `{ batchItemFailures: [{ itemIdentifier }, ...] }` naming exactly which messages in the batch failed — SQS uses that to retry only those, not the whole batch.

**Second change, from adding full CRUD:** the microservice's `PUT` is a *partial* update (only sends the fields that actually changed), so `OrderUpdated` events may carry just `status` or just `customer`, not both. The original design's `PutItem` would **replace the whole item**, silently wiping out whichever field wasn't in that particular event — a real data-loss bug once partial updates exist. Switched to `UpdateItem` with a dynamically built expression that only touches the attributes actually present in the event. Also added `OrderDeleted` handling (`DeleteItemCommand`) — full CRUD on the source means the projection needs a delete path too, or deleted orders would live in `PriceProjection` forever:

```js
const { DynamoDBClient, UpdateItemCommand, DeleteItemCommand } = require('@aws-sdk/client-dynamodb');

const client = new DynamoDBClient({});
const TABLE = process.env.TABLE_NAME;

exports.handler = async (event) => {
  const batchItemFailures = [];

  for (const record of event.Records) {
    try {
      const ebEvent = JSON.parse(record.body);   // the EventBridge event, as delivered via SQS
      const detailType = ebEvent['detail-type'];
      const { orderId, status, customer } = ebEvent.detail || {};
      if (!orderId) {
        console.error('Event missing detail.orderId, skipping:', record.body);
        continue;
      }

      if (detailType === 'OrderDeleted') {
        await client.send(new DeleteItemCommand({ TableName: TABLE, Key: { orderId: { S: orderId } } }));
        console.log('Removed projection for:', orderId);
        continue;
      }

      // OrderCreated / OrderUpdated: only touch attributes actually present in
      // the event, so a partial update (e.g. status-only) doesn't clobber the
      // other field already stored in the projection.
      const sets = [];
      const names = {};
      const values = {};
      if (status != null) {
        sets.push('#status = :status');
        names['#status'] = 'status';
        values[':status'] = { S: String(status) };
      }
      if (customer != null) {
        sets.push('#customer = :customer');
        names['#customer'] = 'customer';
        values[':customer'] = { S: String(customer) };
      }
      if (sets.length === 0) {
        console.log('No projected fields in event, skipping write for:', orderId);
        continue;
      }

      await client.send(new UpdateItemCommand({
        TableName: TABLE,
        Key: { orderId: { S: orderId } },
        UpdateExpression: `SET ${sets.join(', ')}`,
        ExpressionAttributeNames: names,
        ExpressionAttributeValues: values,
      }));
      console.log('Projected order update:', orderId);
    } catch (err) {
      console.error('Failed to process record', record.messageId, err);
      batchItemFailures.push({ itemIdentifier: record.messageId });
    }
  }

  return { batchItemFailures };
};
```
`UpdateItem` also happens to upsert by default (creates the item if it doesn't exist), so `OrderCreated` and `OrderUpdated` can share the same code path — no need to branch on those two. A record that throws (or is simply missing `orderId`, handled via `continue` rather than a throw so it's not endlessly retried) gets added to `batchItemFailures` by `messageId`; after `maxReceiveCount` (3) failed attempts SQS moves it to `ProjectionDLQ`.

## RDS: the microservice's own database (Aurora Serverless v2 Postgres)

Confirmed with the user: RDS is the EC2 microservice's *own* transactional store, not a replacement for `PriceProjection`. The microservice writes orders to Postgres, then still publishes to EventBridge — DynamoDB and everything downstream is untouched.

All APIs below verified directly against installed `aws-cdk-lib@2.265.0` `.d.ts`/`.js`:

- **`rds.DatabaseCluster`** (not the older `ServerlessCluster`, which is Aurora Serverless *v1*-only) with `writer: rds.ClusterInstance.serverlessV2('Writer', { publiclyAccessible: false })`. `ClusterInstanceType.serverlessV2()`/`ClusterInstance.serverlessV2()` confirmed at `aws-rds/lib/aurora-cluster-instance.d.ts:65,352`.
- **`serverlessV2MinCapacity`/`serverlessV2MaxCapacity`** default to `0.5`/`2` if omitted (confirmed at `cluster.js`'s `DatabaseClusterNew` constructor). CDK's own validation (`validateServerlessScalingConfig` in `cluster.js`) requires: `maxCapacity` between 1–256, `minCapacity` between **0**–256 (0 is valid), `maxCapacity >= minCapacity`, both in 0.5 increments.
- **`serverlessV2AutoPauseDuration`** (confirmed at `cluster.d.ts:86-94`) — "the duration an Aurora Serverless v2 DB instance must be idle before Aurora attempts to automatically pause it," must be between 300s (5 min) and 86,400s (24h), default 300s. Combined with `serverlessV2MinCapacity: 0`, this lets the cluster scale all the way to zero ACUs when idle — the closest Aurora gets to DynamoDB's pay-per-request idle-cost story, though note storage itself still bills continuously (small, but non-zero, unlike an idle DynamoDB table).
- **`publiclyAccessible?: boolean`** confirmed on the cluster-instance props (`aurora-cluster-instance.d.ts:189`) — set `false` explicitly so the writer gets no public IP even though (same constraint as the EC2 instance) the default VPC only offers `PUBLIC`-classified subnets.
- **`rds.Credentials.fromGeneratedSecret(username, options?)`** (confirmed at `props.d.ts:180`) — generates a random password and stores both username+password in a new Secrets Manager secret (`dbCluster.secret`), rather than a password ever appearing in CDK code or UserData in plaintext.
- **Engine version**: `rds.AuroraPostgresEngineVersion.VER_17_9` is the newest available in this installed CDK version (confirmed via `cluster-engine.d.ts`) — pin explicitly rather than taking an engine default, for reproducibility. Check `node_modules/aws-cdk-lib/aws-rds/lib/cluster-engine.d.ts` for the current newest `VER_*` if this plan is revisited later and a newer `aws-cdk-lib` is installed.

### Additions to `lib/microservice-source-construct.ts`

New import: `import * as rds from 'aws-cdk-lib/aws-rds';`

Added after the EC2 `instanceRole`/`instanceSg` are defined, before the `ec2.Instance` itself:
```ts
// --- RDS: the microservice's own transactional store ---
const dbSecurityGroup = new ec2.SecurityGroup(this, 'MicroserviceDbSg', {
  vpc,
  description: 'Aurora Serverless v2 — inbound only from the microservice EC2 instance',
});
dbSecurityGroup.addIngressRule(instanceSg, ec2.Port.tcp(5432), 'from microservice EC2 instance');

const dbCluster = new rds.DatabaseCluster(this, 'MicroserviceDb', {
  engine: rds.DatabaseClusterEngine.auroraPostgres({
    version: rds.AuroraPostgresEngineVersion.VER_17_9,
  }),
  vpc,
  vpcSubnets: { subnetType: ec2.SubnetType.PUBLIC },   // same constraint as the EC2 instance — default VPC has no private subnets
  securityGroups: [dbSecurityGroup],
  writer: rds.ClusterInstance.serverlessV2('Writer', { publiclyAccessible: false }),
  serverlessV2MinCapacity: 0,      // allows scaling all the way to 0 ACUs when idle
  serverlessV2MaxCapacity: 1,
  serverlessV2AutoPauseDuration: cdk.Duration.minutes(10),
  defaultDatabaseName: 'orders',
  credentials: rds.Credentials.fromGeneratedSecret('microservice_admin'),
  removalPolicy: cdk.RemovalPolicy.DESTROY,   // matches every other resource in this practice stack
});
dbCluster.secret!.grantRead(instanceRole);   // instance fetches DB creds from Secrets Manager at boot, never hardcoded
```

New UserData environment variables for the systemd unit (alongside the existing `EVENT_BUS_NAME`/`AWS_REGION`):
```
Environment=DB_HOST=${dbCluster.clusterEndpoint.hostname}
Environment=DB_PORT=${dbCluster.clusterEndpoint.port}
Environment=DB_NAME=orders
Environment=DB_SECRET_ARN=${dbCluster.secret!.secretArn}
```

### Additions to `microservice/server.js` and `microservice/package.json`

New dependencies: `pg` (Postgres client) and `@aws-sdk/client-secrets-manager` (to fetch the generated credentials at startup — never baked into the image or env vars in plaintext).

On startup, before `server.listen(...)`:
```js
const { SecretsManagerClient, GetSecretValueCommand } = require('@aws-sdk/client-secrets-manager');
const { Pool } = require('pg');

async function initDb() {
  const sm = new SecretsManagerClient({});
  const { SecretString } = await sm.send(new GetSecretValueCommand({ SecretId: process.env.DB_SECRET_ARN }));
  const { username, password } = JSON.parse(SecretString);

  const pool = new Pool({
    host: process.env.DB_HOST,
    port: Number(process.env.DB_PORT),
    database: process.env.DB_NAME,
    user: username,
    password,
    ssl: { rejectUnauthorized: false },   // Aurora requires TLS; using the AWS-issued cert chain properly is a good follow-up, not required for this practice setup
  });

  await pool.query(`
    CREATE TABLE IF NOT EXISTS orders (
      order_id TEXT PRIMARY KEY,
      status TEXT,
      customer TEXT,
      updated_at TIMESTAMPTZ DEFAULT now()
    )
  `);

  return pool;
}
```

`POST /orders/:orderId` handler gains a database write before the existing `PutEvents` call:
```js
await pool.query(
  `INSERT INTO orders (order_id, status, customer, updated_at)
   VALUES ($1, $2, $3, now())
   ON CONFLICT (order_id) DO UPDATE
     SET status = EXCLUDED.status, customer = EXCLUDED.customer, updated_at = now()`,
  [orderId, status, customer],
);
```
followed by the same `PutEventsCommand` call already in the plan — RDS becomes the microservice's source of truth, EventBridge stays purely a "something changed" notification to the BFF side, matching how the rest of this design already separates storage from notification.

## Prerequisite + verification steps

1. **Prerequisite (run manually):** `bff-project-cli`'s existing policies (API Gateway/Cognito/DynamoDB/S3/CloudFormation/Lambda/IAM/CloudWatch/SSM) cover none of EC2, RDS, or Secrets Manager — all three need attaching before any deploy that includes this plan:
   ```
   aws iam attach-user-policy --user-name bff-project-cli \
     --policy-arn arn:aws:iam::aws:policy/AmazonEC2FullAccess
   aws iam attach-user-policy --user-name bff-project-cli \
     --policy-arn arn:aws:iam::aws:policy/AmazonRDSFullAccess
   aws iam attach-user-policy --user-name bff-project-cli \
     --policy-arn arn:aws:iam::aws:policy/SecretsManagerReadWrite
   ```
2. **Deploy:** `npx cdk diff` then `npx cdk deploy` (first run triggers the VPC context lookup, creating `cdk.context.json`).
3. **Get the instance ID + bus name** from the `MicroserviceInstanceId`/`MicroserviceEventBusName` `CfnOutput`s.
4. **SSM port-forward:**
   ```
   aws ssm start-session --target <instance-id> \
     --document-name AWS-StartPortForwardingSession \
     --parameters '{"portNumber":["3000"],"localPortNumber":["8080"]}'
   ```
5. **End-to-end CRUD test:**
   ```
   # create
   curl -X POST localhost:8080/orders \
     -H 'Content-Type: application/json' \
     -d '{"orderId":"order-001","status":"PLACED","customer":"Ada"}'
   # 409 if you repeat the same create
   curl -X POST localhost:8080/orders -d '{"orderId":"order-001"}'

   # read
   curl localhost:8080/orders
   curl localhost:8080/orders/order-001

   # partial update — only status changes, customer should still read "Ada" afterward
   curl -X PUT localhost:8080/orders/order-001 \
     -H 'Content-Type: application/json' \
     -d '{"status":"SHIPPED"}'

   # delete
   curl -X DELETE localhost:8080/orders/order-001
   # 404 afterward
   curl localhost:8080/orders/order-001
   ```
6. **Verify the chain:** CloudWatch Logs for `EventConsumerFn` should show `Projected order update: order-001` after the create/update calls and `Removed projection for: order-001` after the delete. The create/update steps should appear live in the running `client/` app's `LiveOrder.tsx` (watching `order-001`) and `LiveUpdates.tsx` panels.
   **Known gap, not yet fixed by this plan:** the delete will *not* show up live. The already-deployed `lambda/stream.js` explicitly skips DynamoDB Stream `REMOVE` events (`if (record.eventName === 'REMOVE') continue;`), so a `DeleteItem` never reaches AppSync/the subscription — only a manual REST refresh (`GetOrdersFn`'s `Scan`) would reflect it being gone. Propagating deletes live would need a separate, deliberate change to the deployed subscriber pipeline (`stream.js` + likely a schema change, since `Order`/`OrderInput` currently have no "this was deleted" signal) — out of scope for this plan, which only covers the new publisher side.
7. **Node version sanity check on first boot:** SSM into the instance and run `node -v` to confirm `nodejs20` installed as expected; if `dnf install -y nodejs20` isn't available in the account's AL2023 repo mirror, fall back to the plain `nodejs` package name.
8. **Verify the durability layer:** check `ProjectionQueue` in the SQS console — `ApproximateNumberOfMessagesVisible` should return to 0 shortly after a successful test, confirming the message was consumed rather than sitting stuck. To deliberately exercise the DLQ path: temporarily break `EventConsumerFn` (e.g. point `TABLE_NAME` at a table that doesn't exist), send 3 test events, confirm they land in `ProjectionDLQ` instead of retrying forever, then revert the break and redeploy.
9. **Verify RDS:** after a `POST /orders/:orderId`, SSM into the instance and check the systemd unit's logs (`journalctl -u microservice -n 50`) for successful `INSERT`/`UPDATE` activity, or connect directly with `psql` (installable via `dnf install -y postgresql16`) using the same `DB_HOST`/`DB_SECRET_ARN` the service uses, and `SELECT * FROM orders;`.

## Cost note

Two components here bill continuously while they exist, unlike the rest of this stack (pay-per-request/serverless):
- The `t3.micro` EC2 instance bills hourly while running, even idle. Recommend `aws ec2 stop-instances --instance-ids <id>` when not actively testing, or tear down via `cdk destroy`/removing the construct when done experimenting.
- Aurora Serverless v2 bills per ACU-hour while active, but scales to 0 ACUs after `serverlessV2AutoPauseDuration` (10 min) of inactivity when `serverlessV2MinCapacity: 0` — so compute cost approaches zero when idle, similar in spirit to DynamoDB's idle cost. Storage is the exception: Aurora bills a small ongoing storage charge regardless of pause state, which DynamoDB's `PAY_PER_REQUEST` table does not for an empty/idle table in the same way. Not large at this scale, but worth knowing it's not literally free like the rest of the stack.
