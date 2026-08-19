import { generateClient, type GraphQLQuery, type GraphQLSubscription } from 'aws-amplify/api';
import type { Order } from '../types';

const client = generateClient();

const ON_ORDER_UPDATE = /* GraphQL */ `
  subscription OnOrderUpdate {
    onOrderUpdate {
      orderId
      status
      customer
    }
  }
`;

const HEALTH = /* GraphQL */ `
  query Health {
    _health
  }
`;

interface OnOrderUpdateSubscription {
  onOrderUpdate?: Order | null;
}

interface HealthQuery {
  _health: string | null;
}

export function subscribeToOrderUpdates(onNext: (order: Order) => void) {
  const sub = client
    .graphql<GraphQLSubscription<OnOrderUpdateSubscription>>({
      query: ON_ORDER_UPDATE,
      authMode: 'userPool',
    })
    .subscribe({
      next: ({ data }) => {
        if (data.onOrderUpdate) onNext(data.onOrderUpdate);
      },
      error: (err) => console.error('subscription error', err),
    });
  return () => sub.unsubscribe();
}

export async function checkHealth(): Promise<string | null> {
  const result = await client.graphql<GraphQLQuery<HealthQuery>>({
    query: HEALTH,
    authMode: 'userPool',
  });
  return result.data._health;
}
