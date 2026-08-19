import { useCallback, useEffect, useState } from 'react';
import { getOrders } from '../api/rest';
import type { Order } from '../types';

export default function OrdersList() {
  const [orders, setOrders] = useState<Order[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(() => {
    setLoading(true);
    setError(null);
    getOrders()
      .then(setOrders)
      .catch((err) => setError(err.message))
      .finally(() => setLoading(false));
  }, []);

  useEffect(() => {
    load();
  }, [load]);

  return (
    <section>
      <h2>Orders (REST)</h2>
      <button onClick={load} disabled={loading}>
        {loading ? 'Loading…' : 'Refresh'}
      </button>
      {error && <p style={{ color: 'red' }}>{error}</p>}
      <ul>
        {orders.map((o) => (
          <li key={o.orderId}>
            {o.orderId} — {o.status} — {o.customer}
          </li>
        ))}
      </ul>
    </section>
  );
}
