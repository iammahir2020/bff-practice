# BFF practice client

A minimal React + Vite app to manually exercise the deployed REST API and AppSync GraphQL API from `BffPracticeStack`. This is a test harness, not production UI — it only reads: it lists orders via the REST API and subscribes to live order updates via GraphQL. There is no write endpoint; use the AWS Console or CLI to write to DynamoDB directly and watch the update arrive here.

## Setup

1. Deploy the stack from the repo root:
   ```
   cd ..
   npx cdk deploy
   ```
   Note the four `CfnOutput` values printed at the end: `ApiUrl`, `GraphqlUrl`, `UserPoolId`, `UserPoolClientId`.

2. Configure the client:
   ```
   cp .env.example .env
   ```
   Edit `.env` and fill in the four values above, plus the AWS region you deployed to (`VITE_AWS_REGION`).

3. Install and run:
   ```
   npm install
   npm run dev
   ```

4. Open the printed local URL. Sign up a new user (self-signup + email verification is enabled on the Cognito user pool), confirm with the emailed code, then sign in.

5. The "Orders (REST)" panel should load (likely empty on a fresh table) — use "Refresh" to re-fetch.

6. To see the "Live updates" panel fire, write directly to the `PriceProjection` DynamoDB table in a second terminal while this page stays open, e.g.:
   ```
   aws dynamodb put-item \
     --table-name <PriceProjection table name> \
     --item '{"orderId": {"S": "order-1"}, "status": {"S": "shipped"}, "customer": {"S": "Ada"}}'
   ```
   This flows: DynamoDB Stream → `StreamHandlerFn` → AppSync `publishOrderUpdate` mutation (IAM) → `onOrderUpdate` subscription (Cognito user pool) → this page.

## Notes

- The REST call authenticates with the signed-in user's Cognito **ID token**, sent unprefixed in the `Authorization` header (matches API Gateway's built-in Cognito authorizer, not a `Bearer` scheme).
- `publishOrderUpdate` is intentionally not called from this client — it's `@aws_iam`-only in the schema and reserved for the backend stream Lambda.
