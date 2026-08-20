import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as events from 'aws-cdk-lib/aws-events';
import * as targets from 'aws-cdk-lib/aws-events-targets';
import * as sqs from 'aws-cdk-lib/aws-sqs';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import { SqsEventSource } from 'aws-cdk-lib/aws-lambda-event-sources';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';

export interface MicroserviceEventSourceProps {
  readonly table: dynamodb.Table;
}

export class MicroserviceEventSource extends Construct {
  public readonly eventBusName: string;
  public readonly rule: events.Rule;
  public readonly projectionQueue: sqs.Queue;
  public readonly projectionDlq: sqs.Queue;
  public readonly eventConsumerFn: lambda.Function;

  constructor(scope: Construct, id: string, props: MicroserviceEventSourceProps) {
    super(scope, id);

    const bus = new events.EventBus(this, 'MicroserviceEventBus', {
      eventBusName: 'bff-microservice-bus',
    });

    // Step 6: dead-letter queue — messages that fail processing
    // maxReceiveCount times land here instead of retrying forever or
    // being lost.
    const projectionDlq = new sqs.Queue(this, 'ProjectionDLQ', {
      retentionPeriod: cdk.Duration.days(14),
    });

    // Step 3: durable buffer between the rule and the consumer Lambda.
    // visibilityTimeout is set to 6x EventConsumerFn's timeout below, per
    // AWS guidance, so a message isn't redelivered to a second concurrent
    // invocation while the first is still processing it.
    const projectionQueue = new sqs.Queue(this, 'ProjectionQueue', {
      visibilityTimeout: cdk.Duration.seconds(60),
      deadLetterQueue: { queue: projectionDlq, maxReceiveCount: 3 },
    });

    const rule = new events.Rule(this, 'OrderUpdatedRule', {
      eventBus: bus,
      eventPattern: { source: ['bff.microservice'] },
      targets: [new targets.SqsQueue(projectionQueue)],
    });

    // Step 5: consumer Lambda now writes into the existing PriceProjection
    // table — this is where the new publisher path connects into the
    // already-deployed subscriber pipeline (Stream -> AppSync -> client).
    const eventConsumerFn = new lambda.Function(this, 'EventConsumerFn', {
      runtime: lambda.Runtime.NODEJS_20_X,
      handler: 'eventConsumer.handler',
      code: lambda.Code.fromAsset('lambda'),
      environment: { TABLE_NAME: props.table.tableName },
      timeout: cdk.Duration.seconds(10),
    });
    props.table.grantWriteData(eventConsumerFn);
    eventConsumerFn.addEventSource(new SqsEventSource(projectionQueue, {
      batchSize: 5,
      reportBatchItemFailures: true,
    }));

    this.eventBusName = bus.eventBusName;
    this.rule = rule;
    this.projectionQueue = projectionQueue;
    this.projectionDlq = projectionDlq;
    this.eventConsumerFn = eventConsumerFn;
  }
}
