package org.apache.zookeeper.server.quorum.helpers;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.server.quorum.ElectionException;
import org.apache.zookeeper.server.quorum.FastLeaderElectionV2Round;
import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.server.quorum.Vote;
import org.apache.zookeeper.server.quorum.VoteViewChange;
import org.apache.zookeeper.server.quorum.VoteViewConsumerCtrl;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;
import org.apache.commons.lang3.tuple.ImmutablePair;

public class MockFLEV2Wrapper extends AbstractFLEV2Wrapper {
    final VoteViewChange voteViewChange;
    public MockFLEV2Wrapper(final long mySid,
                               final QuorumPeer.LearnerType learnerType,
                               final QuorumVerifier quorumVerifier,
                               final VoteViewChange voteViewChange,
                               final VoteViewConsumerCtrl voteViewConsumerCtrl,
                               final int stableTimeout,
                               final TimeUnit stableTimeoutUnit) {
        super(mySid, learnerType, quorumVerifier, voteViewChange,
                voteViewConsumerCtrl, stableTimeout, stableTimeoutUnit);
        this.voteViewChange = voteViewChange;
    }

    /**
     * Run one loop of election epoch update and leader election and return
     * the elected vote along with the collection of votes which will contain
     * our self vote updated to election epoch.
     * Update self vote to follow the leader.
     * @param votes
     * @return Leader elected vote and set of votes with our vote with
     * election epoch updated.
     * @throws ElectionException
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public ImmutablePair<Vote, Collection<Vote>> lookForLeaderLoopUpdateHelper(
            final Collection<Vote> votes)
            throws ElectionException, InterruptedException, ExecutionException {
        final FastLeaderElectionV2Round flev2Round =
                new FastLeaderElectionV2Round(getId(),
                        getQuorumVerifier(), votes, LOG);
        flev2Round.lookForLeader();

        lastLookForLeader = flev2Round;

        // Update our vote if leader is non null
        if (flev2Round.getLeaderVote() != null) {
            suggestedForTermination = flev2Round;
            final Vote finalSelfVote
                    = catchUpToLeaderBeforeExitAndUpdate(flev2Round);
            assert finalSelfVote != null;
            updateSelfVote(finalSelfVote).get();
        }

        setVotesInRun(votes);

        return ImmutablePair.of(flev2Round.getLeaderVote(), flev2Round
                .getVoteMap().values());
    }

    @Override
    public Future<Vote> runLeaderElection(
            final Collection<Vote> votes)
            throws ElectionException, InterruptedException, ExecutionException {
        return CompletableFuture.completedFuture(
                lookForLeaderLoopUpdateHelper(votes).getLeft());
    }
}
