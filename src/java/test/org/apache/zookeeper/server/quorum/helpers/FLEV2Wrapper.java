package org.apache.zookeeper.server.quorum.helpers;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.zookeeper.server.quorum.ElectionException;
import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.server.quorum.Vote;

public interface FLEV2Wrapper {
    long getId();
    QuorumPeer.ServerState getState();

    Vote getSelfVote();
    Future<Void> updateSelfVote(final Vote vote);

    Future<Vote> runLeaderElection(
            final Collection<Vote> votes)
            throws ElectionException, InterruptedException, ExecutionException;

    void waitForVotesRun(final Map<Long, Vote> voteMap)
            throws InterruptedException, ExecutionException;
    void verifyNonTermination();

    void shutdown();
}
