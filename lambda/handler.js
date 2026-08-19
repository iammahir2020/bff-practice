const { DynamoDBClient, ScanCommand } = require('@aws-sdk/client-dynamodb');

const client = new DynamoDBClient({});
const TABLE = process.env.TABLE_NAME;

// Minimal unmarshal: turn DynamoDB's {S:"x"} / {N:"5"} shape into plain values
const val = (av) => av.S ?? (av.N !== undefined ? Number(av.N) : av.BOOL ?? null);
const toItem = (raw) =>
  Object.fromEntries(Object.entries(raw).map(([k, v]) => [k, val(v)]));

exports.handler = async () => {
  const res = await client.send(new ScanCommand({ TableName: TABLE }));
  const items = (res.Items || []).map(toItem);
  return {
    statusCode: 200,
    headers: {
      'Content-Type': 'application/json',
      'Access-Control-Allow-Origin': '*', // lets a browser call it from anywhere, for now
    },
    body: JSON.stringify(items),
  };
};