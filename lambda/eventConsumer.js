const { DynamoDBClient, UpdateItemCommand } = require('@aws-sdk/client-dynamodb');

const client = new DynamoDBClient({});
const TABLE = process.env.TABLE_NAME;

exports.handler = async (event) => {
  const batchItemFailures = [];

  for (const record of event.Records) {
    try {
      const ebEvent = JSON.parse(record.body);   // the EventBridge event, as delivered via SQS
      const { orderId, status, customer } = ebEvent.detail || {};
      if (!orderId) {
        console.error('Event missing detail.orderId, skipping:', record.body);
        continue;
      }

      // Only touch attributes actually present in the event, so a partial
      // update (e.g. status-only) doesn't clobber the other field already
      // stored in the projection.
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
