import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as path from 'path';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as apigateway from 'aws-cdk-lib/aws-apigateway';
import * as appsync from 'aws-cdk-lib/aws-appsync';
import * as cognito from 'aws-cdk-lib/aws-cognito';
import { DynamoEventSource } from 'aws-cdk-lib/aws-lambda-event-sources';

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
    });
    streamFn.addEventSource(new DynamoEventSource(table, {
      startingPosition: lambda.StartingPosition.LATEST,
      batchSize: 5,
      retryAttempts: 2,
    }));
    graph.grantMutation(streamFn, 'publishOrderUpdate');

    new cdk.CfnOutput(this, 'ApiUrl', { value: api.url });
    new cdk.CfnOutput(this, 'GraphqlUrl', { value: graph.graphqlUrl });
    new cdk.CfnOutput(this, 'UserPoolId', { value: userPool.userPoolId });
    new cdk.CfnOutput(this, 'UserPoolClientId', { value: userPoolClient.userPoolClientId });
  }
}