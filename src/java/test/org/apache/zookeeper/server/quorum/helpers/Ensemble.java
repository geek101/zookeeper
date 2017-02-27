package org.apache.zookeeper.server.quorum.helpers;

import java.util.Collection;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.zookeeper.server.quorum.ElectionException;
import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.server.quorum.Vote;
import org.apache.commons.lang3.tuple.ImmutablePair;

public interface Ensemble {
    Ensemble runLooking() throws InterruptedException,
            ExecutionException, ElectionException;
    Ensemble configure(final String quorumStr)
            throws ElectionException, ExecutionException, InterruptedException;
    Ensemble configure(final Collection<
            ImmutablePair<Long, QuorumPeer.ServerState>> quorumWithState)
            throws ElectionException, ExecutionException, InterruptedException;
    Collection<Ensemble> moveToLookingForAll()
            throws ElectionException, InterruptedException, ExecutionException;
    Ensemble moveToLooking(final long sid)
            throws ElectionException, InterruptedException, ExecutionException;

    int getQuorumSize();
    FLEV2Wrapper getFleToRun();
    FLEV2Wrapper getFleThatRan();
    HashMap<Long, Vote> getLeaderLoopResult();
    QuorumCnxMesh getQuorumCnxMesh();

    void verifyLeaderAfterShutDown() throws InterruptedException,
            ExecutionException;
    Future<?> shutdown();

    boolean isConnected(final long serverSid);

    Collection<Collection<Collection<
            ImmutablePair<Long, QuorumPeer.ServerState>>>>
    quorumMajorityWithLeaderServerStateCombinations();
}
