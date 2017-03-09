/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>http://www.apache.org/licenses/LICENSE-2.0</p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.zookeeper.server.quorum.helpers;

import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.server.quorum.ElectionException;
import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.server.quorum.Vote;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EnsembleMockBcast extends AbstractEnsemble {
    private static final Logger LOG
            = LoggerFactory.getLogger(EnsembleMockBcast.class.getClass());

    private MockQuorumBcast mockQuorumBcast;

    private FLEV2Wrapper fleThatRan;
    public EnsembleMockBcast(final long id, final int quorumSize,
                             final int stableTimeout,
                             final TimeUnit stableTimeoutUnit)
            throws ElectionException {
        super(id, quorumSize, stableTimeout, stableTimeoutUnit);
        mockQuorumBcast = new MockQuorumBcast(getId(), quorumSize,
                getQuorumCnxMesh());
    }

    public EnsembleMockBcast(
            final Ensemble parentEnsemble, final QuorumCnxMesh quorumCnxMeshArg,
            final Collection<ImmutablePair<Long, QuorumPeer.ServerState>>
                                     flatQuorumWithState,
            final Collection<Collection<ImmutablePair<Long,
                                     QuorumPeer.ServerState>>>
                    partitionedQuorumArg,
            final int stableTimeout, final TimeUnit stableTimeoutUnit)
            throws ElectionException {
        super(parentEnsemble, quorumCnxMeshArg, flatQuorumWithState,
                partitionedQuorumArg, stableTimeout, stableTimeoutUnit);
        mockQuorumBcast = new MockQuorumBcast(getId(),
                parentEnsemble.getQuorumSize(),
                parentEnsemble.getQuorumCnxMesh());
    }

    @Override
    public Ensemble createEnsemble(
            final Ensemble parentEnsemble,
            final QuorumCnxMesh quorumCnxMeshArg,
            final Collection<ImmutablePair<Long, QuorumPeer.ServerState>>
                    flatQuorumWithState,
            final Collection<Collection<ImmutablePair<Long,
                    QuorumPeer.ServerState>>>
                    partitionedQuorumArg) throws ElectionException {
        return new EnsembleMockBcast(parentEnsemble, quorumCnxMeshArg,
                flatQuorumWithState, partitionedQuorumArg,
                stableTimeout, stableTimeoutUnit);
    }

    @Override
    protected void runLookingForSid(final long sid) throws InterruptedException,
            ExecutionException, ElectionException {
        super.runLookingForSid(sid);
        fleThatRan = fles.get(sid);

        for (final FLEV2Wrapper f : fles.values()) {
            if (f.getId() != sid &&
                    f.getState() == QuorumPeer.ServerState.LOOKING
                    && isConnected(f.getId())) {
                futuresForLookingPeers.put(f.getId(),
                        f.runLeaderElection(getVotes()));
            }
        }

        futuresForLookingPeers.put(fleThatRan.getId(),
                        fleThatRan.runLeaderElection(getVotes()));
    }

    @Override
    public FLEV2Wrapper getFleThatRan() {
        return fleThatRan;
    }

    public boolean isConnected(final long serverSid) {
        return mockQuorumBcast.isConnectedToAny(serverSid);
    }

    @Override
    protected FLEV2Wrapper createFLEV2(
            final long sid, final QuorumVerifier quorumVerifier) {
        if (mockQuorumBcast == null) {
            mockQuorumBcast = new MockQuorumBcast(getId(), getQuorumSize(),
                    getQuorumCnxMesh());
        }
        return createFLEV2BcastWrapper(sid, quorumVerifier);
    }

    @Override
    protected FLEV2Wrapper copyFLEV2(final FLEV2Wrapper fle, final Vote vote)
            throws InterruptedException, ExecutionException {
        if (!(fle instanceof FLEV2BcastWrapper)) {
            throw new IllegalArgumentException("fle is not of MockBcast " +
                    "type");
        }
        assert vote != null;
        mockQuorumBcast.resetQuorumCnxMesh(getQuorumCnxMesh());
        FLEV2BcastWrapper flev2BcastWrapper = createFLEV2BcastWrapper(fle
                .getId(), ((FLEV2BcastWrapper) fle).getQuorumVerifier());
        flev2BcastWrapper.updateSelfVote(vote).get();
        return flev2BcastWrapper;
    }

    protected FLEV2BcastWrapper createFLEV2BcastWrapper(
            final long sid, final QuorumVerifier quorumVerifier) {
        final VoteViewMockBcast voteViewMockBcast
                = new VoteViewMockBcast(sid, mockQuorumBcast);
        return new FLEV2BcastWrapper(sid, QuorumPeer.LearnerType.PARTICIPANT,
                quorumVerifier, voteViewMockBcast,
                voteViewMockBcast, stableTimeout, stableTimeoutUnit);
    }

    protected MockQuorumBcast getMockQuorumBcast() {
        return mockQuorumBcast;
    }
}
