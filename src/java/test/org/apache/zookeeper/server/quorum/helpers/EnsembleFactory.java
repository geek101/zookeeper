package org.apache.zookeeper.server.quorum.helpers;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.server.quorum.ElectionException;
import org.apache.zookeeper.server.quorum.QuorumServer;

public class EnsembleFactory {
    public static Ensemble createEnsemble(
            final String type, final long id, final int quorumSize,
            final int stableTimeout, final TimeUnit stableTimeUnit,
            final List<QuorumServer> servers,
            final Long readTimeoutMsec,
            final Long connectTimeoutMsec,
            final Long keepAliveTimeoutMsec,
            final Integer keepAliveCount,
            final String keyStoreLocation,
            final String keyStorePassword,
            final String trustStoreLocation,
            final String trustStorePassword,
            final String trustStoreCAAlias)
            throws ElectionException {
        if (type.compareToIgnoreCase("mock") == 0) {
            return new MockEnsemble(id, quorumSize, stableTimeout,
                    stableTimeUnit);
        } else if (type.compareToIgnoreCase("mockbcast") == 0) {
            return new EnsembleMockBcast(id, quorumSize, stableTimeout,
                    stableTimeUnit);
        } else if (type.compareToIgnoreCase("flemockbcast") == 0) {
            return new EnsembleFLEMockBcast(id, quorumSize, stableTimeout,
                    stableTimeUnit);
        } else if (type.compareToIgnoreCase("quorumbcast") == 0 ||
                type.compareToIgnoreCase("quorumbcast-ssl") == 0) {
            Boolean sslEnabled = false;
            if (type.contains("ssl"))
                sslEnabled = true;

            return new EnsembleVoteView(id, quorumSize, stableTimeout,
                    stableTimeUnit, servers, readTimeoutMsec,
                    connectTimeoutMsec, keepAliveTimeoutMsec, keepAliveCount,
                    sslEnabled, keyStoreLocation, keyStorePassword,
                    trustStoreLocation, trustStorePassword, trustStoreCAAlias);

        }   else if (type.compareToIgnoreCase("flequorumbcast") == 0 ||
                type.compareToIgnoreCase("flequorumbcast-ssl") == 0) {
            Boolean sslEnabled = false;
            if (type.contains("ssl"))
                sslEnabled = true;

            return new EnsembleFLEVoteView(id, quorumSize, stableTimeout,
                    stableTimeUnit, servers, readTimeoutMsec,
                    connectTimeoutMsec, keepAliveTimeoutMsec, keepAliveCount,
                    sslEnabled, keyStoreLocation, keyStorePassword,
                    trustStoreLocation, trustStorePassword, trustStoreCAAlias);
        }
        throw new IllegalArgumentException("invalid type: " + type);
    }
}
