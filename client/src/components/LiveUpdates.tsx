import { useEffect, useState } from 'react';
import { subscribeToOrderUpdates } from '../api/graphql';
import type { Order } from '../types';

interface LogEntry {
  receivedAt: string;
  order: Order;
}

export default function LiveUpdates() {
  const [log, setLog] = useState<LogEntry[]>([]);

  useEffect(() => {
    return subscribeToOrderUpdates((order) => {
      setLog((prev) => [{ receivedAt: new Date().toLocaleTimeString(), order }, ...prev]);
    });
  }, []);

  return (
    <section>
      <h2>Live updates (GraphQL subscription)</h2>
      <p>
        Trigger this by writing directly to the <code>PriceProjection</code> DynamoDB table
        (Console or CLI) while this page is open.
      </p>
      <ul>
        {log.map((entry, i) => (
          <li key={i}>
            [{entry.receivedAt}] {entry.order.orderId} — {entry.order.status} —{' '}
            {entry.order.customer}
          </li>
        ))}
      </ul>
    </section>
  );
}
