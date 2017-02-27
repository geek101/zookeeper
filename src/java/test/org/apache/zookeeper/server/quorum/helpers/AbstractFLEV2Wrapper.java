package org.apache.zookeeper.server.quorum.helpers;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.server.quorum.FastLeaderElectionV2;
import org.apache.zookeeper.server.quorum.Vote;
import org.apache.zookeeper.server.quorum.VoteViewChange;
import org.apache.zookeeper.server.quorum.VoteViewConsumerCtrl;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;

import static org.apache.zookeeper.server.quorum.QuorumPeer.LearnerType;
import static org.apache.zookeeper.server.quorum.QuorumPeer.ServerState;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public abstract class AbstractFLEV2Wrapper extends FastLeaderElectionV2
implements FLEV2Wrapper {
    private final VoteViewChange voteViewChange;

    public AbstractFLEV2Wrapper(final long mySid,
                                final LearnerType learnerType,
                                final QuorumVerifier quorumVerifier,
                                final VoteViewChange voteViewChange,
                                final VoteViewConsumerCtrl voteViewConsumerCtrl,
                                final int stableTimeout,
                                final TimeUnit stableTimeoutUnit) {
        super(mySid, learnerType, quorumVerifier, voteViewChange,
                voteViewConsumerCtrl, stableTimeout, stableTimeoutUnit);
        this.voteViewChange = voteViewChange;
    }

    @Override
    public Vote getSelfVote() {
        return voteViewChange.getSelfVote();
    }

    @Override
    public ServerState getState() {
        return voteViewChange.getSelfVote().getState();
    }

    @Override
    public Future<Void> updateSelfVote(final Vote vote) {
        try {
            return voteViewChange.updateSelfVote(vote);
        } catch (InterruptedException | ExecutionException exp) {
            throw new RuntimeException(exp);
        }
    }

    @Override
    public void waitForVotesRun(final Map<Long, Vote> voteMap)
            throws InterruptedException, ExecutionException {
        super.waitForVotesRun(voteMap);
    }

    @Override
    public void verifyNonTermination() {
        assertNotEquals("non terminating did run sid: " + getId(),
                null, getLastLookForLeader());
        assertEquals("non terminating never tried stability run sid: "
                + getId(), null, couldTerminate());
    }

    @Override
    public void shutdown() {
        super.shutdown();
    }

    @Override
    public int hashCode() {
        return (int) this.getId();
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof AbstractFLEV2Wrapper &&
                this.getId() == ((AbstractFLEV2Wrapper) other).getId();
    }

    public QuorumVerifier getQuorumVerifier() {
        return super.getQuorumVerifier();
    }

    public LearnerType getLearnerType() {
        return super.getLearnerType();
    }

    public VoteViewChange getVoteViewChange() {
        return super.getVoteViewChange();
    }

    public VoteViewConsumerCtrl getVoteViewConsumerCtrl() {
        return super.getVoteViewConsumerCtrl();
    }

    public int getStableTimeout() {
        return super.getStableTimeout();
    }

    public TimeUnit getStableTimeUnit() {
        return super.getStableTimeUnit();
    }
}

