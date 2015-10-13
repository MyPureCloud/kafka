package org.apache.kafka.clients;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ClusterConnectionsStatesTest {

    /**
     * Make sure a node is not immediately ready after it is disconnected due to a long timeout, for example.
     */
    @Test
    public void testBlockoutAfterTimeout() {
        ClusterConnectionStates states = new ClusterConnectionStates(10L);

        long now = 0;
        int node = 0;

        states.connecting(node, now);

        // while the node is connecting it can't connect again, but isn't blacked out either:
        assertFalse(states.canConnect(node, now));
        assertFalse(states.isBlackedOut(node, now));

        // simulate a default 120 second SO_TIMEOUT elapsing and the node disconnecting:
        now = 120000L;
        states.disconnected(node, now);

        // since the node just disconnected it should be blacked out:
        assertFalse(states.canConnect(node, now));
        assertTrue(states.isBlackedOut(node, now));

        // simulate some time passing:
        now = 130000;

        // the node should no longer be blacked out:
        assertTrue(states.canConnect(node, now));
        assertFalse(states.isBlackedOut(node, now));
    }
}
