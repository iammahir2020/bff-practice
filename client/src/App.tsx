import { Authenticator } from '@aws-amplify/ui-react';
import OrdersList from './components/OrdersList';
import LiveUpdates from './components/LiveUpdates';
import LiveOrder from './components/LiveOrder';

export default function App() {
  return (
    <Authenticator>
      {({ signOut, user }) => (
        <div style={{ padding: '1rem' }}>
          <p>Signed in as {user?.signInDetails?.loginId}</p>
          <button onClick={signOut}>Sign out</button>
          <OrdersList />
          <LiveOrder />
          <LiveUpdates />
        </div>
      )}
    </Authenticator>
  );
}
