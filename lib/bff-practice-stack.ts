import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as path from 'path';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as apigateway from 'aws-cdk-lib/aws-apigateway';
import * as appsync from 'aws-cdk-lib/aws-appsync';
import * as cognito from 'aws-cdk-lib/aws-cognito';
import * as cloudwatch from 'aws-cdk-lib/aws-cloudwatch';
import { DynamoEventSource } from 'aws-cdk-lib/aws-lambda-event-sources';
import { MicroserviceEventSource } from './microservice-source-construct';

export class BffPracticeStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const table = new dynamodb.Table(this, 'PriceProjection', {
      partitionKey: { name: 'orderId', type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      stream: dynamodb.StreamViewType.NEW_IMAGE,
    });

    const getOrdersFn = new lambda.Function(this, 'GetOrdersFn', {
      runtime: lambda.Runtime.NODEJS_20_X,
      handler: 'handler.handler',
      code: lambda.Code.fromAsset('lambda'),
      environment: { TABLE_NAME: table.tableName },
      tracing: lambda.Tracing.ACTIVE,
    });
    table.grantReadData(getOrdersFn);

    // User pool declared BEFORE the API so the authorizer can reference it
    const userPool = new cognito.UserPool(this, 'BffUserPool', {
      selfSignUpEnabled: true,
      signInAliases: { email: true },
      autoVerify: { email: true },
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    const userPoolClient = new cognito.UserPoolClient(this, 'BffUserPoolClient', {
      userPool,
      authFlows: { userSrp: true, userPassword: true },
    });

    // NEW: Cognito authorizer for the REST API
    const apiAuthorizer = new apigateway.CognitoUserPoolsAuthorizer(this, 'BffApiAuthorizer', {
      cognitoUserPools: [userPool],
    });

    const api = new apigateway.LambdaRestApi(this, 'BffApi', {
      handler: getOrdersFn,
      proxy: true,
      defaultCorsPreflightOptions: { allowOrigins: apigateway.Cors.ALL_ORIGINS },
      defaultMethodOptions: {
        authorizer: apiAuthorizer,
        authorizationType: apigateway.AuthorizationType.COGNITO,   // every route now requires a valid token
      },
      deployOptions: { tracingEnabled: true },
    });

    const graph = new appsync.GraphqlApi(this, 'BffGraphApi', {
      name: 'bff-graph-api',
      definition: appsync.Definition.fromFile(
        path.join(__dirname, '..', 'graphql', 'schema.graphql')
      ),
      authorizationConfig: {
        defaultAuthorization: {
          authorizationType: appsync.AuthorizationType.USER_POOL,
          userPoolConfig: { userPool },
        },
        additionalAuthorizationModes: [
          { authorizationType: appsync.AuthorizationType.IAM },
        ],
      },
      // xrayEnabled intentionally omitted: it forces CloudFormation to replace
      // the GraphQL API (new URL, cascading replacement of schema/data
      // source/resolver) rather than an in-place update. Revisit deliberately
      // if AppSync-side tracing is needed later.
    });

    const noneDs = graph.addNoneDataSource('NoneDS');
    noneDs.createResolver('PublishResolver', {
      typeName: 'Mutation',
      fieldName: 'publishOrderUpdate',
      requestMappingTemplate: appsync.MappingTemplate.fromString(`{
  "version": "2017-02-28",
  "payload": $util.toJson($context.arguments.order)
}`),
      responseMappingTemplate: appsync.MappingTemplate.fromString(`$util.toJson($context.result)`),
    });

    const streamFn = new lambda.Function(this, 'StreamHandlerFn', {
      runtime: lambda.Runtime.NODEJS_20_X,
      handler: 'stream.handler',
      code: lambda.Code.fromAsset('lambda'),
      environment: { APPSYNC_URL: graph.graphqlUrl },
      tracing: lambda.Tracing.ACTIVE,
    });
    streamFn.addEventSource(new DynamoEventSource(table, {
      startingPosition: lambda.StartingPosition.LATEST,
      batchSize: 5,
      retryAttempts: 2,
    }));
    graph.grantMutation(streamFn, 'publishOrderUpdate');

    // ── Monitoring dashboard: one screen for system health ──────────
    const dashboard = new cloudwatch.Dashboard(this, 'BffDashboard', {
      dashboardName: 'BFF-Live-Health',
    });

    dashboard.addWidgets(
      new cloudwatch.GraphWidget({
        title: 'Lambda Invocations',
        left: [
          getOrdersFn.metricInvocations({ period: cdk.Duration.minutes(5) }),
          streamFn.metricInvocations({ period: cdk.Duration.minutes(5) }),
        ],
        width: 12,
      }),
      new cloudwatch.GraphWidget({
        title: 'Lambda Errors',
        left: [
          getOrdersFn.metricErrors({ period: cdk.Duration.minutes(5) }),
          streamFn.metricErrors({ period: cdk.Duration.minutes(5) }),
        ],
        width: 12,
      }),
    );

    dashboard.addWidgets(
      new cloudwatch.GraphWidget({
        title: 'Lambda Duration (ms)',
        left: [
          getOrdersFn.metricDuration({ period: cdk.Duration.minutes(5) }),
          streamFn.metricDuration({ period: cdk.Duration.minutes(5) }),
        ],
        width: 12,
      }),
      new cloudwatch.GraphWidget({
        title: 'API Requests & Errors',
        left: [
          api.metricCount({ period: cdk.Duration.minutes(5) }),
          api.metricServerError({ period: cdk.Duration.minutes(5) }),
          api.metricClientError({ period: cdk.Duration.minutes(5) }),
        ],
        width: 12,
      }),
    );

    dashboard.addWidgets(
      new cloudwatch.GraphWidget({
        title: 'DynamoDB Capacity (Read/Write)',
        left: [
          table.metricConsumedReadCapacityUnits({ period: cdk.Duration.minutes(5) }),
          table.metricConsumedWriteCapacityUnits({ period: cdk.Duration.minutes(5) }),
        ],
        width: 12,
      }),
      new cloudwatch.SingleValueWidget({
        title: 'Stream Errors (last 5 min)',
        metrics: [streamFn.metricErrors({ period: cdk.Duration.minutes(5) })],
        width: 12,
      }),
    );

    // ── Producer pipeline (backend_implementation_steps.md Steps 1-6) ──
    const microservice = new MicroserviceEventSource(this, 'MicroserviceEventSource', { table });

    // Same dashboard, new rows — one screen for both the subscriber
    // pipeline above and the producer pipeline below.
    dashboard.addWidgets(
      new cloudwatch.GraphWidget({
        title: 'OrderUpdatedRule Invocations',
        left: [
          new cloudwatch.Metric({
            namespace: 'AWS/Events',
            metricName: 'Invocations',
            dimensionsMap: { RuleName: microservice.rule.ruleName },
            period: cdk.Duration.minutes(5),
          }),
          new cloudwatch.Metric({
            namespace: 'AWS/Events',
            metricName: 'FailedInvocations',
            dimensionsMap: { RuleName: microservice.rule.ruleName },
            period: cdk.Duration.minutes(5),
          }),
        ],
        width: 12,
      }),
      new cloudwatch.GraphWidget({
        title: 'EventConsumerFn Invocations & Errors',
        left: [
          microservice.eventConsumerFn.metricInvocations({ period: cdk.Duration.minutes(5) }),
          microservice.eventConsumerFn.metricErrors({ period: cdk.Duration.minutes(5) }),
        ],
        width: 12,
      }),
    );

    dashboard.addWidgets(
      new cloudwatch.GraphWidget({
        title: 'ProjectionQueue / ProjectionDLQ Depth',
        left: [
          microservice.projectionQueue.metricApproximateNumberOfMessagesVisible({ period: cdk.Duration.minutes(5) }),
          microservice.projectionDlq.metricApproximateNumberOfMessagesVisible({ period: cdk.Duration.minutes(5) }),
        ],
        width: 12,
      }),
      new cloudwatch.SingleValueWidget({
        title: 'DLQ Backlog (current)',
        metrics: [microservice.projectionDlq.metricApproximateNumberOfMessagesVisible()],
        width: 12,
      }),
    );

    new cdk.CfnOutput(this, 'ApiUrl', { value: api.url });
    new cdk.CfnOutput(this, 'GraphqlUrl', { value: graph.graphqlUrl });
    new cdk.CfnOutput(this, 'UserPoolId', { value: userPool.userPoolId });
    new cdk.CfnOutput(this, 'UserPoolClientId', { value: userPoolClient.userPoolClientId });
    new cdk.CfnOutput(this, 'DashboardUrl', {
      value: `https://${this.region}.console.aws.amazon.com/cloudwatch/home?region=${this.region}#dashboards:name=${dashboard.dashboardName}`,
    });
    new cdk.CfnOutput(this, 'MicroserviceEventBusName', { value: microservice.eventBusName });
  }
}