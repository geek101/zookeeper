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

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.zookeeper.common.X509Exception;
import org.apache.zookeeper.server.quorum.ElectionException;
import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.server.quorum.QuorumServer;
import org.apache.zookeeper.server.quorum.Vote;
import org.apache.zookeeper.server.quorum.VoteViewChange;
import org.apache.zookeeper.server.quorum.VoteViewConsumerCtrl;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;
import org.apache.zookeeper.server.quorum.netty.BaseTest;
import org.apache.zookeeper.server.quorum.util.ChannelException;
import org.apache.zookeeper.server.quorum.util.LogPrefix;
import org.apache.zookeeper.server.quorum.util.QuorumSSLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EnsembleVoteView extends AbstractEnsemble {
    private static final Logger LOGS
            = LoggerFactory.getLogger(EnsembleVoteView.class.getClass());
    private final List<QuorumServer> servers;
    private final long readTimeoutMsec;
    private final long connectTimeoutMsec;
    private final long keepAliveTimeoutMsec;
    private final int keepAliveCount;
    private final boolean sslEnabled;
    final String keyStoreLocation;
    final String keyStorePassword;
    final String trustStoreLocation;
    final String trustStorePassword;
    final String trustStoreCAAlias;

    private final Map<Long, QuorumServer> serverMap;

    private MockQuorumBcast mockQuorumBcast;

    private Map<Long,
                ImmutablePair<QuorumBcastWithCnxMesh, VoteViewWrapper>>
            quorumBcastAndVoteViewMap = new HashMap<>();

    private FLEV2Wrapper fleThatRan;

    public EnsembleVoteView(final long id, final int quorumSize,
                            final QuorumVerifier quorumVerifier,
                            final EnsembleState pastState,
                            final Ensemble parent,
                            final int stableTimeout,
                            final TimeUnit stableTimeoutUnit,
                            final List<QuorumServer> servers,
                            final long readTimeoutMsec,
                            final long connectTimeoutMsec,
                            final long keepAliveTimeoutMsec,
                            final int keepAliveCount,
                            boolean sslEnabled,
                            final String keyStoreLocation,
                            final String keyStorePassword,
                            final String trustStoreLocation,
                            final String trustStorePassword,
                            final String trustStoreCAAlias) {
        super(id, quorumSize, quorumVerifier, pastState, parent, stableTimeout,
                stableTimeoutUnit);
        this.servers = servers;
        this.readTimeoutMsec = readTimeoutMsec;
        this.connectTimeoutMsec = connectTimeoutMsec;
        this.keepAliveTimeoutMsec = keepAliveTimeoutMsec;
        this.keepAliveCount = keepAliveCount;
        this.sslEnabled = sslEnabled;
        this.keyStoreLocation = keyStoreLocation;
        this.keyStorePassword = keyStorePassword;
        this.trustStoreLocation = trustStoreLocation;
        this.trustStorePassword = trustStorePassword;
        this.trustStoreCAAlias = trustStoreCAAlias;

        this.serverMap = new HashMap<>();
        for (final QuorumServer quorumServer : servers) {
            if (serverMap.containsKey(quorumServer.id())) {
                throw new IllegalArgumentException("sid collision for " +
                        "server:" + quorumServer.id());
            }
            serverMap.put(quorumServer.id(), quorumServer);
        }
    }

    public EnsembleVoteView(final long id, final int quorumSize,
                            final int stableTimeout,
                            final TimeUnit stableTimeoutUnit,
                            final List<QuorumServer> servers,
                            final long readTimeoutMsec,
                            final long connectTimeoutMsec,
                            final long keepAliveTimeoutMsec,
                            final int keepAliveCount,
                            boolean sslEnabled,
                            final String keyStoreLocation,
                            final String keyStorePassword,
                            final String trustStoreLocation,
                            final String trustStorePassword,
                            final String trustStoreCAAlias)
            throws ElectionException {
        this(id, quorumSize, new QuorumMajWrapper(quorumSize),
                EnsembleState.INVALID, null, stableTimeout, stableTimeoutUnit,
                servers, readTimeoutMsec, connectTimeoutMsec,
                keepAliveTimeoutMsec, keepAliveCount, sslEnabled,
                keyStoreLocation, keyStorePassword, trustStoreLocation,
                trustStorePassword, trustStoreCAAlias);
        this.fles = new HashMap<>();

        quorumCnxMesh = new QuorumCnxMeshBase(this.getQuorumSize());
        quorumCnxMesh.connectAll();
        mockQuorumBcast = new MockQuorumBcast(getId(), getQuorumSize(),
                quorumCnxMesh);
        final Collection<ImmutablePair<Long, QuorumPeer
                .ServerState>> noPartition = new ArrayList<>();
        for (final FLEV2Wrapper fle : getQuorumWithInitVoteSet(
                quorumSize, new QuorumMajWrapper(quorumSize))) {
            this.fles.put(fle.getId(), fle);
            noPartition.add(ImmutablePair.of(fle.getId(), fle.getState()));
        }
        this.partitionedQuorum.add(noPartition);
        this.LOG = new LogPrefix(LOGS, toString());
    }

    public EnsembleVoteView(final Ensemble parentEnsemble,
                            final QuorumCnxMesh quorumCnxMeshArg,
                            final Collection<ImmutablePair<Long,
                                    QuorumPeer.ServerState>>
                                    flatQuorumWithState,
                            final Collection<Collection<ImmutablePair<Long,
                                    QuorumPeer.ServerState>>>
                                    partitionedQuorumArg,
                            final int stableTimeout,
                            final TimeUnit stableTimeoutUnit,
                            final List<QuorumServer> servers)
            throws ElectionException {
        this(((EnsembleVoteView)parentEnsemble).getId() + 1,
                (parentEnsemble).getQuorumSize(),
                ((EnsembleVoteView)parentEnsemble).getQuorumVerifier(),
                ((EnsembleVoteView)parentEnsemble).getState(),
                parentEnsemble, stableTimeout, stableTimeoutUnit,
                servers,
                ((EnsembleVoteView)parentEnsemble).readTimeoutMsec,
                ((EnsembleVoteView)parentEnsemble).connectTimeoutMsec,
                ((EnsembleVoteView)parentEnsemble).keepAliveTimeoutMsec,
                ((EnsembleVoteView)parentEnsemble).keepAliveCount,
                ((EnsembleVoteView)parentEnsemble).sslEnabled,
                ((EnsembleVoteView)parentEnsemble).keyStoreLocation,
                ((EnsembleVoteView)parentEnsemble).keyStorePassword,
                ((EnsembleVoteView)parentEnsemble).trustStoreLocation,
                ((EnsembleVoteView)parentEnsemble).trustStorePassword,
                ((EnsembleVoteView)parentEnsemble).trustStoreCAAlias);
        this.fles = ((AbstractEnsemble)parentEnsemble).fles;
        this.quorumCnxMesh = quorumCnxMeshArg;
        mockQuorumBcast = new MockQuorumBcast(getId(), getQuorumSize(),
                quorumCnxMesh);
        this.flatQuorumWithState = flatQuorumWithState;
        this.partitionedQuorum = partitionedQuorumArg;
        this.LOG = new LogPrefix(LOGS, toString());
    }

    protected List<QuorumServer> getServersForEnsemble() {
        List<QuorumServer> serversForEnsemble = servers;
        if (flatQuorumWithState != null) {
            serversForEnsemble = new ArrayList<>();
            for (ImmutablePair<Long, QuorumPeer.ServerState> pair
                    : flatQuorumWithState) {
                serversForEnsemble.add(serverMap.get(pair.getLeft()));
            }
        }

        return serversForEnsemble;
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


        return new EnsembleVoteView(parentEnsemble, quorumCnxMeshArg,
                flatQuorumWithState, partitionedQuorumArg,
                stableTimeout, stableTimeoutUnit, getServersForEnsemble());
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

    @Override
    public boolean isConnected(final long serverSid) {
        return quorumCnxMesh.isConnectedToAny(serverSid);
    }

    @Override
    public Future<?> shutdown() {
        super.shutdown();
        for (final ImmutablePair<QuorumBcastWithCnxMesh, VoteViewWrapper> p
                : quorumBcastAndVoteViewMap.values()) {
            try {
                p.getRight().shutdown().get();
            } catch (InterruptedException | ExecutionException exp) {
                final String errStr = "Cannot fail here, exp: " + exp;
                LOG.error(errStr);
                throw new RuntimeException(errStr);
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    protected FLEV2Wrapper createFLEV2(
            final long sid, final QuorumVerifier quorumVerifier) {
        return createFLEV2BcastWrapper(sid, quorumVerifier);
    }

    @Override
    protected FLEV2Wrapper copyFLEV2(final FLEV2Wrapper fle,
                                     final Vote vote)
            throws InterruptedException, ExecutionException {
        if (!(fle instanceof FLEV2BcastWrapper) &&
                !(fle instanceof FLEV2BcastNoThreadWrapper)) {
            throw new IllegalArgumentException("fle is not of proper type: "
                    + fle.getClass().getName());
        }
        assert vote != null;

        FLEV2Wrapper flev2Wrapper = createFLEV2BcastWrapper(fle
                .getId(), ((AbstractFLEV2Wrapper) fle).getQuorumVerifier());
        flev2Wrapper.updateSelfVote(vote).get();
        return flev2Wrapper;
    }

    /**
     * Only FINAL state ensemble is run hence minimize threads and other
     * artifcat creation, helps with resources for test cases.
     * @param sid
     * @param quorumVerifier
     * @return
     */
    private FLEV2Wrapper createFLEV2BcastWrapper(
            final long sid, final QuorumVerifier quorumVerifier) {
        if (this.getState() != EnsembleState.FINAL) {
            final MockVoteView mockVoteView
                    = new MockVoteView(sid, mockQuorumBcast);
            return new FLEV2BcastNoThreadWrapper(sid,
                    QuorumPeer.LearnerType.PARTICIPANT, quorumVerifier,
                    mockVoteView, mockVoteView, stableTimeout,
                    stableTimeoutUnit);
        }

        // In final state allocate everything required.
        final EventLoopGroup eventLoopGroupForQBCast
                = new NioEventLoopGroup(VoteViewWrapper.MAX_THREAD_COUNT,
                Executors.newSingleThreadExecutor(new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable target) {
                        final Thread thread = new Thread(target);
                        LOG.debug("Creating new worker thread");
                        thread.setUncaughtExceptionHandler(
                                new Thread.UncaughtExceptionHandler() {
                                    @Override
                                    public void uncaughtException(Thread t,
                                                                  Throwable e) {
                                        LOG.error("Uncaught Exception: " + e);
                                        System.exit(1);
                                    }
                                });
                        return thread;
                    }
                }));

        org.apache.zookeeper.server.quorum.netty.QuorumBroadcast quorumBroadcast;
        try {
            // Create a quorumBcast per VoteView.
            quorumBroadcast = new QuorumBcastWithCnxMesh(sid,
                    servers, serverMap.get(sid).getElectionAddr(),
                    eventLoopGroupForQBCast, readTimeoutMsec, connectTimeoutMsec,
                    keepAliveTimeoutMsec, keepAliveCount,
                    sslEnabled, quorumCnxMesh);
        } catch (ChannelException | IOException exp) {
            final String errStr = "Cannot fail here, exp: " + exp;
            LOG.error(errStr);
            throw new RuntimeException(errStr);
        }

        // In final state allocate everything required.
        final EventLoopGroup eventLoopGroupForVoteView
                = new NioEventLoopGroup(VoteViewWrapper.MAX_THREAD_COUNT,
                Executors.newSingleThreadExecutor(new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable target) {
                        final Thread thread = new Thread(target);
                        LOG.debug("Creating new worker thread");
                        thread.setUncaughtExceptionHandler(
                                new Thread.UncaughtExceptionHandler() {
                                    @Override
                                    public void uncaughtException(Thread t,
                                                                  Throwable e) {
                                        LOG.error("Uncaught Exception: " + e);
                                        System.exit(1);
                                    }
                                });
                        return thread;
                    }
                }));

        final VoteViewWrapper voteViewWrapper = new VoteViewWrapper(sid,
                serverMap.get(sid).getElectionAddr(), eventLoopGroupForVoteView,
                quorumBroadcast);

        quorumBcastAndVoteViewMap.put(sid,
                ImmutablePair.of((QuorumBcastWithCnxMesh)quorumBroadcast,
                        voteViewWrapper));

        final QuorumPeerNonDynCheck quorumPeerNonDynCheck =
                new QuorumPeerNonDynCheck(this.keyStoreLocation,
                        this.keyStorePassword);

        try {
            voteViewWrapper.start(new QuorumSSLContext(quorumPeerNonDynCheck,
                    BaseTest.createQuorumPeerConfig(getClass().getClassLoader(),
                    keyStoreLocation, keyStorePassword,
                    trustStoreLocation, trustStorePassword)));
        } catch (IOException | ChannelException | CertificateException |
                NoSuchAlgorithmException | X509Exception.KeyManagerException |
                X509Exception.TrustManagerException exp) {
            final String errStr = "Cannot fail here, exp: " + exp;
            LOG.error(errStr);
            throw new RuntimeException(errStr);
        }

        return createFLEWrapper(sid,
                quorumVerifier, voteViewWrapper, voteViewWrapper, stableTimeout,
                stableTimeoutUnit);
    }

    protected FLEV2Wrapper createFLEWrapper(
            final long sid,
            final QuorumVerifier quorumVerifier,
            final VoteViewChange voteViewChange,
            final VoteViewConsumerCtrl voteViewConsumerCtrl,
            final int stableTimeout,
            final TimeUnit stableTimeoutUnit) {
        return new FLEV2BcastWrapper(sid, QuorumPeer.LearnerType.PARTICIPANT,
                quorumVerifier, voteViewChange, voteViewConsumerCtrl,
                stableTimeout, stableTimeoutUnit);
    }
}
