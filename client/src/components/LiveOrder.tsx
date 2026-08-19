import { useEffect, useState } from 'react';
import { subscribeToOrderUpdates } from '../api/graphql';
import type { Order } from '../types';

export default function LiveOrder() {
  const [watchedId, setWatchedId] = useState('order-001');
  const [order, setOrder] = useState<Order | null>(null);
  const [updatedAt, setUpdatedAt] = useState<string | null>(null);

  useEffect(() => {
    setOrder(null);
    setUpdatedAt(null);
    return subscribeToOrderUpdates((incoming) => {
      if (incoming.orderId === watchedId) {
        setOrder(incoming);
        setUpdatedAt(new Date().toLocaleTimeString());
      }
    });
  }, [watchedId]);

  return (
    <section>
      <h2>Watch one order live</h2>
      <label>
        Order ID:{' '}
        <input value={watchedId} onChange={(e) => setWatchedId(e.target.value)} />
      </label>

      {order ? (
        <div>
          <p>orderId: {order.orderId}</p>
          <p>status: {order.status}</p>
          <p>customer: {order.customer}</p>
          <p>last updated: {updatedAt}</p>
        </div>
      ) : (
        <p>
          No update received yet for "{watchedId}" — write to DynamoDB with this orderId
          (Console or CLI) and it'll appear here instantly, no refresh needed.
        </p>
      )}
    </section>
  );
}
