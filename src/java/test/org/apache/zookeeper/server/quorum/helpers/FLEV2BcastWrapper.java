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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.server.quorum.ElectionException;
import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.server.quorum.Vote;
import org.apache.zookeeper.server.quorum.VoteViewChange;
import org.apache.zookeeper.server.quorum.VoteViewConsumerCtrl;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FLEV2BcastWrapper extends AbstractFLEV2Wrapper {
    private static final Logger LOG
            = LoggerFactory.getLogger(FLEV2BcastWrapper.class.getName());
    final QuorumVerifier quorumVerifier;
    final ExecutorService executorService;
    final FLEV2BcastWrapper self;
    public FLEV2BcastWrapper(final long mySid,
                            final QuorumPeer.LearnerType learnerType,
                            final QuorumVerifier quorumVerifier,
                            final VoteViewChange voteViewChange,
                            final VoteViewConsumerCtrl voteViewConsumerCtrl,
                            final int stableTimeout,
                            final TimeUnit stableTimeoutUnit) {
        super(mySid, learnerType, quorumVerifier, voteViewChange,
                voteViewConsumerCtrl, stableTimeout, stableTimeoutUnit);
        this.quorumVerifier = quorumVerifier;
        this.executorService = Executors.newSingleThreadExecutor(
                new ThreadFactory() {
            @Override
            public Thread newThread(Runnable target) {
                final Thread thread = new Thread(target);
                LOG.debug("Creating new worker thread");
                thread.setUncaughtExceptionHandler(
                        new Thread.UncaughtExceptionHandler() {
                            @Override
                            public void uncaughtException(Thread t,
                                                          Throwable e) {
                                LOG.error("Uncaught Exception", e);
                                System.exit(1);
                            }
                        });
                return thread;
            }
        });
        this.self = this;
    }

    /**
     * Run the actual lookForLeader API in a different thread.
     * @param votes
     * @return
     * @throws ElectionException
     * @throws InterruptedException
     * @throws ExecutionException
     */
    @Override
    public Future<Vote> runLeaderElection(
            final Collection<Vote> votes) throws ElectionException,
            InterruptedException, ExecutionException {
        FutureTask<Vote> futureTask = new FutureTask<>(
                new Callable<Vote>()  {
                    @Override
                    public Vote call() throws ElectionException,
                            InterruptedException,
                            ExecutionException {
                        return self.lookForLeader(
                                getSelfVote().getPeerEpoch(),
                                getSelfVote().getZxid());
                    }
                });
        submitTask(futureTask);
        return futureTask;
    }

    @Override
    public QuorumVerifier getQuorumVerifier() {
        return quorumVerifier;
    }

    @Override
    public void shutdown() {
        executorService.shutdown();
    }

    protected void submitTask(FutureTask task) {
        executorService.submit(task);
    }
}
