/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.zookeeper.server.quorum;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;
import org.apache.zookeeper.server.quorum.util.LogPrefix;
import org.apache.zookeeper.server.quorum.util.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FastLeaderElectionV2 implements Election {
    private static final Logger LOGS
            = LoggerFactory.getLogger(FastLeaderElectionV2.class);
    private static final int CONSUME_WAIT_MSEC = 3000;  // wait for top loop.
    private final Long mySid;
    private final QuorumPeer.LearnerType learnerType;
    private final QuorumVerifier quorumVerifier;
    private final VoteViewChange voteViewChange;
    private final VoteViewConsumerCtrl voteViewConsumerCtrl;
    private final int stableTimeout;
    private final TimeUnit stableTimeoutUnit;
    protected LogPrefix LOG = null;
    protected AtomicReference<CompletableFuture<Collection<Vote>>>
            waitForLookRunFuture
            = new AtomicReference<>();
    protected FastLeaderElectionV2Round lastLookForLeader = null;
    protected FastLeaderElectionV2Round suggestedForTermination = null;
    private final Random random = new Random();

    public FastLeaderElectionV2(
            final long mySid, final QuorumPeer.LearnerType learnerType,
            final QuorumVerifier quorumVerifier,
            final VoteViewChange voteViewChange,
            final VoteViewConsumerCtrl voteViewConsumerCtrl,
            final int stableTimeout,
            final TimeUnit stableTimeoutUnit) {
        this.mySid = mySid;
        this.learnerType = learnerType;
        this.quorumVerifier = quorumVerifier;
        this.voteViewChange = voteViewChange;
        this.voteViewConsumerCtrl = voteViewConsumerCtrl;
        this.stableTimeout = stableTimeout;
        this.stableTimeoutUnit = stableTimeoutUnit;
        this.LOG = new LogPrefix(LOGS, "mySid:" + this.mySid +
                "-electionEpoch:0");
        this.random.setSeed(System.nanoTime() ^ this.mySid);
        waitForLookRunFuture.set(
                CompletableFuture.completedFuture((Collection<Vote>)null));
    }

    /**
     * Predicate used to get changes to Vote set we last processed.
     */
    private class DefaultPredicate implements Predicate<Collection<Vote>> {
        private final Map<Long, Vote> lastVotesMap;

        public DefaultPredicate(final Collection<Vote> lastVotes) {
            lastVotesMap = new HashMap<>();
            updateVotes(lastVotes);
        }

        /**
         * Check if given votes are same as what we have.
         *
         * @param votes incoming vote set.
         * @return false if same, true if different.
         */
        @Override
        public Boolean call(final Collection<Vote> votes) {
            return VoteView.canAnyChangeView(lastVotesMap, votes);
        }

        protected void updateVotes(final Collection<Vote> votes) {
            lastVotesMap.clear();
            for (final Vote v : votes) {
                lastVotesMap.put(v.getSid(), v);
            }
        }
    }

    /**
     * Predicate to ensure that given a new set of votes and leader is still
     * the same then stay stable. Break if a different leader is elected.
     * This will not update self vote.
     */
    private class LeaderStabilityPredicate implements Predicate<Collection<Vote>> {
        FastLeaderElectionV2Round fleV2Round;
        public LeaderStabilityPredicate(
                final FastLeaderElectionV2Round fleV2Round) {
            this.fleV2Round = fleV2Round;
        }

        /**
         * Return the last updated vote map when ended.
         *
         * @return
         */
        public FastLeaderElectionV2Round getFleV2Round() {
            return fleV2Round;
        }

        /**
         * Check each given vote and verify the stored elected leader is still
         * capable of being a leader.
         *
         * @param votes
         * @return
         */
        @Override
        public Boolean call(final Collection<Vote> votes) {
            if (!VoteView.canAnyChangeView(fleV2Round.getVoteMap(), votes)) {
                return false;
            }

            LOG.debug("something changed, running leader election again");
            final FastLeaderElectionV2Round nextFleV2Round =
                    new FastLeaderElectionV2Round(fleV2Round, votes);
            nextFleV2Round.lookForLeader();

            if (nextFleV2Round.getLeaderVote() == null ||
                    // Do not break for election epoch change
                    !nextFleV2Round.getLeaderVote().matchForLeaderStability(
                            this.fleV2Round.getLeaderVote()) ||
                    // Do not break for LOOKING becoming LEADING
                    (fleV2Round.getLeaderVote().getState()
                            == QuorumPeer.ServerState.LEADING &&
                    nextFleV2Round.getLeaderVote().getState()
                            != QuorumPeer.ServerState.LEADING)) {
                if (nextFleV2Round.getLeaderVote() != null) {
                    LOG.info("broke stability for: "
                            + nextFleV2Round.getLeaderVote());
                } else {
                    LOG.info("broke stability for null vote");
                }

                fleV2Round = nextFleV2Round;
                return true;
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("was stable for leader: "
                        + this.fleV2Round.getLeaderVote());
            }

            fleV2Round = nextFleV2Round;
            return false;
        }
    }

    public long getId() {
        return mySid;
    }

    public void shutdown() {
        LOG.info("shutdown");
    }

    public Vote lookForLeader() throws InterruptedException {
        throw new IllegalAccessError("not implemented for FLEV2");
    }

    /**
     * API to start leader election round.
     *
     * @return Vote if leader election is done, VoteView will be updated.
     * @throws ElectionException    fatal exception, something went wrong must
     *                              exit.
     * @throws InterruptedException fatal exception must exit
     * @throws ExecutionException   fatal exception must exit.
     */
    public Vote lookForLeader(final long peerEpoch,
                              final long zxid) throws ElectionException,
            InterruptedException, ExecutionException {
        // increase the logical clock and start with last good values
        final Vote selfVote = getSelfVote().leaderElectionVote(peerEpoch, zxid);

        // let us broadcast this and  wait till that is done.
        updateSelfVote(selfVote).get();

        LOG.resetPrefix("mySid:" + getId() + "-electionEpoch:"
                + selfVote.getElectionEpoch());

        LOG.info("Entering FLEV2 with vote: " + selfVote);

        // Create and get the change consumer.
        final VoteViewChangeConsumer consumer
                = voteViewConsumerCtrl.createChangeConsumer();

        // Start with just our vote.
        Collection<Vote> votes = Collections.singletonList(selfVote);
        while (true) {
            votes = consumer.consume(CONSUME_WAIT_MSEC, TimeUnit.MILLISECONDS,
                    new DefaultPredicate(votes));

            if (votes == null) {
                votes = Collections.singletonList(getSelfVote());
                continue;
            }

            final FastLeaderElectionV2Round flev2Round =
                    new FastLeaderElectionV2Round(getId(),
                            getQuorumVerifier(), votes, LOG);
            flev2Round.lookForLeader();

            lastLookForLeader = flev2Round;

            // Lets update our election epoch if it did get borrowed from
            // someone else, speeds up things since we are going into
            // stability check phase.
            updateSelfVote(flev2Round.getSelfVote());

            if (flev2Round.foundLeaderWithQuorum()) {
                // If there is quorum for a leader then try stability check
                suggestedForTermination = flev2Round;
                final LeaderStabilityPredicate leaderStabilityPredicate
                        = getLeaderStabilityPredicate(flev2Round);

                votes = suggestedLeaderStabilityCheck(
                        consumer, flev2Round.getLeaderVote(),
                        flev2Round.getLeaderQuorum(),
                        leaderStabilityPredicate);

                if (votes != null) {
                    // stability failed, reset votes and go to next round.
                    updateSelfFromFleV2Round(
                            leaderStabilityPredicate.getFleV2Round());
                    votes = leaderStabilityPredicate.getFleV2Round()
                            .getVoteMap().values();
                    // Used for testing, helps with waiting till all Votes were
                    // considered.
                    // Terminate the unterminated on.
                    setVotesInRun(votes);
                    continue;
                }

                // stability passed, exit this instance of run.
                setVotesInRun(leaderStabilityPredicate.getFleV2Round()
                        .getVoteMap().values());
                final Vote selfFinalVote
                        = catchUpToLeaderBeforeExitAndUpdate(
                        leaderStabilityPredicate.getFleV2Round());

                if (selfFinalVote == null) {
                    updateSelfFromFleV2Round(
                            leaderStabilityPredicate.getFleV2Round());
                    votes = leaderStabilityPredicate.getFleV2Round()
                            .getVoteMap().values();
                    continue;
                }
                leaveInstance(selfFinalVote);
                voteViewConsumerCtrl.removeConsumer(consumer);
                return selfFinalVote;
            }

            final Vote v = updateSelfFromFleV2Round(flev2Round);

            // Used for testing, helps with waiting till all Votes were
            // considered.
            // Terminate the unterminated on.
            setVotesInRun(votes);

            votes = flev2Round.getVoteMap().values();
            LOG.resetPrefix("mySid:" + getId() + "-electionEpoch:"
                    + v.getElectionEpoch());
            LOG.debug("No elected leader, trying again");
        }
    }

    /**
     * Helper API to update the self vote
     * @param fleV2Round
     * @return
     * @throws InterruptedException
     * @throws ExecutionException
     */
    private Vote updateSelfFromFleV2Round(
            final FastLeaderElectionV2Round fleV2Round)
            throws InterruptedException, ExecutionException {
        Vote selfVote = fleV2Round.getSelfVote();
        if (fleV2Round.getLeaderVote() != null) {
            selfVote = selfVote.catchUpToVote(fleV2Round.getLeaderVote());
            updateSelfVote(selfVote);
            return selfVote;
        }

        // If here then there is no suggested leader, we have to reset
        // to electing ourselves as leader, we cannot point to invalid
        // peer as leader.
        updateSelfVote(selfVote.setSelfAsLeader());
        return selfVote;
    }

    /**
     * If a quorum exists then run stability predicate and return an elected
     * leader if all goes well.
     * @param suggestedLeader Vote could belong to ours.
     * @param suggestedLeaderQuorum The quorum for this Vote
     * @param leaderStabilityPredicate
     * @return Will return the elected leader vote else will return null.
     */
    protected Collection<Vote> suggestedLeaderStabilityCheck(
            final VoteViewChangeConsumer consumer,
            final Vote suggestedLeader,
            final HashSet<Long> suggestedLeaderQuorum,
            final LeaderStabilityPredicate leaderStabilityPredicate) throws
            ElectionException, InterruptedException, ExecutionException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Quorum for Leader elected, Vote: " + suggestedLeader
                    + " with count:" + suggestedLeaderQuorum.size());
        }

        // For peace of mind!, if we picked someone else ensure
        // then that vote thinks its a leader.
        assert suggestedLeader.electedSelfAsLeader();

        final int fuzzyStableTimeout = stableTimeout
                + random.nextInt(stableTimeout / 2);

        // Run with consumer predicate.
        return consumer.consume(
                fuzzyStableTimeout, stableTimeoutUnit,
                leaderStabilityPredicate);
    }

    protected Vote catchUpToLeaderBeforeExitAndUpdate(
            final FastLeaderElectionV2Round fleV2Round)
            throws InterruptedException, ExecutionException {
        final Vote leaderVote = fleV2Round.getLeaderVote();
        final Vote selfVote = fleV2Round.getSelfVote();
        // We are done, catch up to leader vote and break the loop.
        QuorumPeer.ServerState targetState
                = (leaderVote.getLeader() == getId()) ?
                QuorumPeer.ServerState.LEADING : learningState();

        if (targetState == QuorumPeer.ServerState.LEADING) {
            // cool, we want to be leader, but only if enough follow us.
            if (!quorumVerifier.containsQuorum(fleV2Round.getLeaderQuorum())) {
                return null;
            }
        } else {
            // if we are follower then leader should be leader by now!.
            if (!leaderVote.isLeader()) {
                return null;
            }
        }

        final Vote finalVote = selfVote.
                catchUpToLeaderVote(leaderVote, targetState);
        updateSelfVote(finalVote);
        return finalVote;
    }

    /**
     * Used for leader stability check, protected for testing.
     *
     * @param fleV2Round
     * @return
     */
    protected LeaderStabilityPredicate getLeaderStabilityPredicate(
            final FastLeaderElectionV2Round fleV2Round) {
        return new LeaderStabilityPredicate(fleV2Round);
    }

    /**
     * Update VoteView with the vote which we think is best.
     *
     * @param vote
     * @throws InterruptedException
     * @throws ExecutionException
     */
    private Future<Void> updateSelfVote(final Vote vote) throws
            InterruptedException,
            ExecutionException {
        return voteViewChange.updateSelfVote(vote);
    }

    private Vote getSelfVote() {
        return voteViewChange.getSelfVote();
    }

    /**
     * A learning state can be either FOLLOWING or OBSERVING.
     * This method simply decides which one depending on the
     * role of the server.
     *
     * @return ServerState
     */
    private QuorumPeer.ServerState learningState() {
        if (learnerType == QuorumPeer.LearnerType.PARTICIPANT) {
            LOG.debug("I'm a participant: " + getId());
            return QuorumPeer.ServerState.FOLLOWING;
        } else {
            LOG.debug("I'm an observer: " + getId());
            return QuorumPeer.ServerState.OBSERVING;
        }
    }

    private void leaveInstance(final Vote v) {
        LOG.info("Leaving FLEV2 instance with vote: " + v);
    }

    /**
     * Used for testing
     */
    protected QuorumVerifier getQuorumVerifier() {
        return this.quorumVerifier;
    }

    protected QuorumPeer.LearnerType getLearnerType() {
        return this.learnerType;
    }

    protected VoteViewChange getVoteViewChange() {
        return this.voteViewChange;
    }

    protected VoteViewConsumerCtrl getVoteViewConsumerCtrl() {
        return this.voteViewConsumerCtrl;
    }

    protected int getStableTimeout() {
        return this.stableTimeout;
    }

    protected TimeUnit getStableTimeUnit() {
        return this.stableTimeoutUnit;
    }

    protected Object couldTerminate() {
        return suggestedForTermination;
    }

    protected Object getLastLookForLeader() {
        return lastLookForLeader;
    }

    protected void setVotesInRun(final Collection<Vote> votes) {
        final Collection<Vote> copyVotes = new ArrayList<>();
        for (final Vote v : votes) {
            copyVotes.add(v);
        }
        // Used for testing, helps with waiting till all Votes were
        // considered.
        // Terminate the unterminated on.
        final CompletableFuture<Collection<Vote>> lastVotesFuture =
                new CompletableFuture<>();
        lastVotesFuture.complete(Collections.unmodifiableCollection(copyVotes));
        waitForLookRunFuture.set(lastVotesFuture);
    }

    /**
     * Used for testing, we need to only exit after given votes were
     * considered for a run.
     * @param voteMap given votes much match what were processed in the run.
     * @throws InterruptedException
     * @throws ExecutionException
     */
    protected void waitForVotesRun(final Map<Long, Vote> voteMap)
            throws InterruptedException, ExecutionException {
        LOG.warn("entering wait for votes run count: " + voteMap.size());
        while(true) {
            final Collection<Vote> votesInRun
                    = waitForLookRunFuture.get().get();
            if (votesInRun == null || voteMap.size() != votesInRun.size()) {
                if (votesInRun != null) {
                    final String errStr = "wait for votes non success count: "
                            + votesInRun.size() + ", expected: "
                            + voteMap.size();
                    if (votesInRun.size() > voteMap.size()) {
                        throw new RuntimeException(errStr);
                    }
                }
                continue;
            }

            boolean missedAVote = false;
            for (final Vote v : votesInRun) {
                // TODO: can we match the entire Vote?, but
                // since vote is update it is hard to do
                // from outside?. May be not?.
                if (!voteMap.containsKey(v.getSid())) {
                    missedAVote = true;
                    break;
                }
            }

            if (!missedAVote) {
                return;
            }
        }
    }
}
