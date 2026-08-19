import { fetchAuthSession } from 'aws-amplify/auth';
import type { Order } from '../types';

export async function getOrders(): Promise<Order[]> {
  const session = await fetchAuthSession();
  const idToken = session.tokens?.idToken?.toString();
  if (!idToken) throw new Error('Not signed in');

  const res = await fetch(import.meta.env.VITE_API_URL, {
    headers: { Authorization: idToken },
  });
  if (!res.ok) throw new Error(`GET orders failed: ${res.status}`);
  return res.json();
}
