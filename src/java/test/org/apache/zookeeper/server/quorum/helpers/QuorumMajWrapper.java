package org.apache.zookeeper.server.quorum.helpers;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.zookeeper.server.quorum.QuorumServer;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Imported from ZK.
 */
public class QuorumMajWrapper implements QuorumVerifier {
    private static final Logger LOG
            = LoggerFactory.getLogger(QuorumMajWrapper.class);

    int half;

    /**
     * Defines a majority to avoid computing it every time.
     *
     * @param n number of servers
     */
    public QuorumMajWrapper(int n){
        this.half = n/2;
    }

    /**
     * Returns weight of 1 by default.
     *
     * @param id
     */
    public long getWeight(long id){
        return (long) 1;
    }

    @Override
    public boolean containsQuorum(Set<Long> set) {
        return false;
    }

    /**
     * Verifies if a set is a majority.
     */
    public boolean containsQuorum(final HashSet<Long> set){
        return set.size() > half;
    }

    @Override
    public boolean containsQuorumFromCount(final long quorumCount) {
        return quorumCount > half;
    }

    @Override
    public long getVersion() {
        return 0;
    }

    @Override
    public void setVersion(long ver) {

    }

    @Override
    public Map<Long, QuorumServer> getAllMembers() {
        return null;
    }

    @Override
    public Map<Long, QuorumServer> getVotingMembers() {
        return null;
    }

    @Override
    public Map<Long, QuorumServer> getObservingMembers() {
        return null;
    }
}