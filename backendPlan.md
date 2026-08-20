# Microservice event source: EC2 → EventBridge → Lambda → DynamoDB

## Context

The BFF stack already implements the "subscriber" half of the AWS BFF reference pattern (https://aws.amazon.com/blogs/mobile/backends-for-frontends-pattern/): DynamoDB `PriceProjection` table → Stream → `StreamHandlerFn` → AppSync `publishOrderUpdate` mutation (IAM) → `onOrderUpdate` subscription (Cognito) → `client/` React app. Right now the only way to trigger an update is a manual `aws dynamodb put-item` CLI call. The goal is to build the "publisher" half from the reference diagrams — a real microservice event source running on EC2 — so we can learn the full pattern end to end: a service call → an event bus → a BFF event consumer → the projection table → everything already built downstream.

Architecture decisions already confirmed:
1. **Event delivery:** EC2 → EventBridge (custom bus) → consumer Lambda → DynamoDB `PutItem`. The EC2 role gets *only* `events:PutEvents` scoped to that bus — no DynamoDB access, preserving the publisher/BFF boundary shown in the diagrams.
2. **Trigger:** a small HTTP server on EC2 (`POST /orders/:orderId`) that gets curled to simulate a business event.
3. **IAM prerequisite:** `bff-project-cli` (the deploying IAM user) currently has zero EC2 permissions (confirmed via `aws iam list-attached-user-policies` — has API Gateway/Cognito/DynamoDB/S3/CloudFormation/Lambda/IAM/CloudWatch/SSM full-access policies, nothing EC2). Run the attach-policy command manually before any deploy touches EC2.
4. **Reachability:** SSM Session Manager port-forwarding only — the security group has **zero inbound rules**. No SSH keys, nothing internet-reachable.
5. **VPC:** use the existing default VPC (`vpc-06f6bae0cfd3858de`, confirmed present via `aws ec2 describe-vpcs`) via `ec2.Vpc.fromLookup`. No new VPC/NAT gateway.
6. **Sizing:** `t3.micro` — ongoing hourly cost (unlike the rest of this stack, which is fully pay-per-request).

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

## Files to create/edit

| File | Purpose |
|---|---|
| `lib/microservice-source-construct.ts` (new) | `MicroserviceEventSource` construct: EventBridge bus + rule, consumer Lambda, VPC lookup, security group, instance role, S3 asset, UserData, EC2 instance |
| `lib/bff-practice-stack.ts` (edit) | Instantiate `MicroserviceEventSource(this, ..., { table })`; add `CfnOutput`s for instance ID + bus name |
| `microservice/server.js` (new) | Plain Node `http` server — `POST /orders/:orderId` → EventBridge `PutEvents` |
| `microservice/package.json` (new) | Declares `@aws-sdk/client-eventbridge`; installed via real `npm install` on the instance at boot (no Lambda-style bundling workaround needed here) |
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
    });
    props.table.grantWriteData(eventConsumerFn);

    new events.Rule(this, 'OrderUpdatedRule', {
      eventBus: bus,
      eventPattern: { source: ['bff.microservice'], detailType: ['OrderUpdated'] },
      targets: [new targets.LambdaFunction(eventConsumerFn)],
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

Plain Node `http` module (no framework, matching `lambda/*.js` style):

```js
const http = require('http');
const { EventBridgeClient, PutEventsCommand } = require('@aws-sdk/client-eventbridge');

const PORT = process.env.PORT || 3000;
const EVENT_BUS_NAME = process.env.EVENT_BUS_NAME;
const client = new EventBridgeClient({});

function readBody(req) {
  return new Promise((resolve, reject) => {
    let data = '';
    req.on('data', (c) => (data += c));
    req.on('end', () => resolve(data));
    req.on('error', reject);
  });
}

const server = http.createServer(async (req, res) => {
  const match = req.method === 'POST' && req.url.match(/^\/orders\/([^/]+)$/);
  if (!match) {
    res.writeHead(404, { 'Content-Type': 'application/json' });
    return res.end(JSON.stringify({ error: 'not found' }));
  }

  const orderId = decodeURIComponent(match[1]);
  let body;
  try {
    body = JSON.parse((await readBody(req)) || '{}');
  } catch {
    res.writeHead(400, { 'Content-Type': 'application/json' });
    return res.end(JSON.stringify({ error: 'invalid JSON body' }));
  }

  const { status, customer } = body;
  if (!status && !customer) {
    res.writeHead(400, { 'Content-Type': 'application/json' });
    return res.end(JSON.stringify({ error: 'body must include status and/or customer' }));
  }

  try {
    await client.send(new PutEventsCommand({
      Entries: [{
        EventBusName: EVENT_BUS_NAME,
        Source: 'bff.microservice',
        DetailType: 'OrderUpdated',
        Detail: JSON.stringify({ orderId, status, customer }),
      }],
    }));
    res.writeHead(202, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ published: true, orderId }));
  } catch (err) {
    console.error('PutEvents failed:', err);
    res.writeHead(502, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'failed to publish event' }));
  }
});

server.listen(PORT, () => console.log(`microservice listening on :${PORT}`));
```

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

```js
const { DynamoDBClient, PutItemCommand } = require('@aws-sdk/client-dynamodb');

const client = new DynamoDBClient({});
const TABLE = process.env.TABLE_NAME;

exports.handler = async (event) => {
  const { orderId, status, customer } = event.detail || {};
  if (!orderId) {
    console.error('Event missing detail.orderId, skipping:', JSON.stringify(event));
    return { statusCode: 400 };
  }

  const item = { orderId: { S: orderId } };
  if (status != null) item.status = { S: String(status) };
  if (customer != null) item.customer = { S: String(customer) };

  await client.send(new PutItemCommand({ TableName: TABLE, Item: item }));
  console.log('Projected order update:', orderId);
  return { statusCode: 200 };
};
```
Uses `PutItem` (mirrors the manual `aws dynamodb put-item` calls this replaces) rather than `UpdateItem` — simpler, and the DynamoDB Stream fires on both `INSERT`/`MODIFY` either way, so downstream behavior is identical.

## Prerequisite + verification steps

1. **Prerequisite (run manually):**
   ```
   aws iam attach-user-policy --user-name bff-project-cli \
     --policy-arn arn:aws:iam::aws:policy/AmazonEC2FullAccess
   ```
2. **Deploy:** `npx cdk diff` then `npx cdk deploy` (first run triggers the VPC context lookup, creating `cdk.context.json`).
3. **Get the instance ID + bus name** from the `MicroserviceInstanceId`/`MicroserviceEventBusName` `CfnOutput`s.
4. **SSM port-forward:**
   ```
   aws ssm start-session --target <instance-id> \
     --document-name AWS-StartPortForwardingSession \
     --parameters '{"portNumber":["3000"],"localPortNumber":["8080"]}'
   ```
5. **End-to-end test:**
   ```
   curl -X POST localhost:8080/orders/order-001 \
     -H 'Content-Type: application/json' \
     -d '{"status":"SHIPPED","customer":"Ada"}'
   ```
6. **Verify the chain:** CloudWatch Logs for `EventConsumerFn` should show `Projected order update: order-001`; the update should appear live in the running `client/` app's `LiveOrder.tsx` (watching `order-001`) and `LiveUpdates.tsx` panels — same as the earlier CLI-triggered test, but now via a real service call through EventBridge instead of a direct DynamoDB write.
7. **Node version sanity check on first boot:** SSM into the instance and run `node -v` to confirm `nodejs20` installed as expected; if `dnf install -y nodejs20` isn't available in the account's AL2023 repo mirror, fall back to the plain `nodejs` package name.

## Cost note

Unlike the rest of this stack (pay-per-request/serverless), the `t3.micro` instance bills hourly while running, even idle. Recommend `aws ec2 stop-instances --instance-ids <id>` when not actively testing, or tear down via `cdk destroy`/removing the construct when done experimenting.
