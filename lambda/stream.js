// Triggered by DynamoDB Streams. Signs a GraphQL mutation to AppSync with
// SigV4 (IAM auth) using the Lambda's execution-role credentials — no API key.
//
// aws4 is a real dependency of this function (see lambda/package.json) and
// must be present in lambda/node_modules so it ships in the deployment zip —
// the Node.js Lambda runtime does NOT expose @smithy/@aws-sdk signing
// internals as top-level requirable modules, only what each bundled
// @aws-sdk/client-* package nests inside its own node_modules.
const https = require('https');
const aws4 = require('aws4');
const { URL } = require('url');

const APPSYNC_URL = process.env.APPSYNC_URL;
const REGION = process.env.AWS_REGION;   // set automatically by the Lambda runtime

const MUTATION = `mutation Publish($order: OrderInput!) {
  publishOrderUpdate(order: $order) { orderId status customer }
}`;

function post(opts, body) {
  return new Promise((resolve, reject) => {
    const req = https.request(opts, (res) => {
      let data = '';
      res.on('data', (c) => (data += c));
      res.on('end', () => resolve(data));
    });
    req.on('error', reject);
    req.write(body);
    req.end();
  });
}

exports.handler = async (event) => {
  const url = new URL(APPSYNC_URL);

  for (const record of event.Records) {
    if (record.eventName === 'REMOVE') continue;
    const img = record.dynamodb.NewImage;
    if (!img) continue;

    const order = {
      orderId: img.orderId?.S,
      status: img.status?.S ?? null,
      customer: img.customer?.S ?? null,
    };
    const body = JSON.stringify({ query: MUTATION, variables: { order } });

    // Build the request, then SigV4-sign it. aws4 pulls the role's temporary
    // credentials from the Lambda's AWS_* env vars — nothing to hand it.
    const opts = {
      host: url.host,
      path: url.pathname,     // "/graphql"
      service: 'appsync',     // AppSync's SigV4 service name
      region: REGION,
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body,
    };
    aws4.sign(opts);          // injects Authorization + X-Amz-Date + X-Amz-Security-Token

    const result = await post(opts, body);
    console.log('Published:', result);
  }
  return { statusCode: 200 };
};