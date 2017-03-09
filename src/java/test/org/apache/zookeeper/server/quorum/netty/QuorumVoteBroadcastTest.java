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

package org.apache.zookeeper.server.quorum.netty;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.apache.zookeeper.server.quorum.QuorumBroadcast;
import org.apache.zookeeper.server.quorum.QuorumBroadcastFactory;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.QuorumServer;
import org.apache.zookeeper.server.quorum.Vote;
import org.apache.zookeeper.server.quorum.helpers.QuorumPeerDynCheckWrapper;
import org.apache.zookeeper.server.quorum.util.Callback;
import org.apache.zookeeper.server.quorum.util.ChannelException;
import org.apache.zookeeper.server.quorum.util.QuorumSSLContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class QuorumVoteBroadcastTest extends BaseTest {
    private static final Logger LOG
            = LoggerFactory.getLogger(QuorumVoteBroadcastTest.class);

    private final String type;
    private final long readTimeoutMsec;
    private final long connectTimeoutMsec;
    private final long keepAliveTimeoutMsec;
    private final int keepAliveCount;
    private HashMap<Long, QuorumServer> serverMap = new HashMap<>();
    private final ExecutorService executor
            = Executors.newSingleThreadExecutor();
    private EventLoopGroup eventLoopGroup;
    private HashMap<Long, List<QuorumServer>> qbastConfMap
            = new HashMap<>();
    private HashMap<Long, QuorumBroadcast> qbcastMap
            = new HashMap<>();
    private long idCount = 1L;
    final long rgenseed = System.currentTimeMillis();
    Random random = new Random(rgenseed);

    // TODO: make this a per broadcast class queue.
    private final ConcurrentLinkedQueue<Vote> inboundVoteQueue
            = new ConcurrentLinkedQueue<>();

    private class MsgRxCb implements Callback<Vote> {
        @Override
        public void call(final Vote o) throws ChannelException, IOException {
            inboundVoteQueue.add(o);
        }
    }

    private final MsgRxCb msgRxCb = new MsgRxCb();

    @Override
    @Before
    public void setup() throws Exception {
        super.setup();
        eventLoopGroup = new NioEventLoopGroup(1, executor);
        LOG.info("Setup type: " + type);
        ClassLoader cl = getClass().getClassLoader();
        int count = 0;
        for (Map.Entry<Long, List<QuorumServer>> entry :
                qbastConfMap.entrySet()) {
            QuorumBroadcast qbcast
                    = QuorumBroadcastFactory.createQuorumBroadcast(type,
                    entry.getKey(), entry.getValue(),
                    serverMap.get(entry.getKey()).getElectionAddr(),
                    eventLoopGroup, readTimeoutMsec, connectTimeoutMsec,
                    keepAliveTimeoutMsec, keepAliveCount);
            if (qbcast == null) {
                throw new NullPointerException("qbcast is invalid!, bailing");
            }

            final QuorumPeerDynCheckWrapper quorumPeerDynCheckWrapper =
                    new QuorumPeerDynCheckWrapper();
            quorumPeerDynCheckWrapper.setQuorumServerMap(entry.getValue());

            qbcast.start(msgRxCb, new QuorumSSLContext
                            (quorumPeerDynCheckWrapper,
                                    createQuorumPeerConfig(count, keyStore,
                                            keyPassword, 0, trustStore,
                                            trustPassword)));
            qbcastMap.put(entry.getKey(), qbcast);
            count++;
        }
    }

    @After
    public void tearDown() throws Exception {
        for (org.apache.zookeeper.server.quorum.QuorumBroadcast qbcast : qbcastMap.values()) {
            qbcast.shutdown();
        }
        qbcastMap.clear();
        eventLoopGroup.shutdownGracefully().sync();
    }

    @SuppressWarnings("unchecked")
    public QuorumVoteBroadcastTest(final String type,
                                   final List<String> serverList,
                                   final long readTimeoutMsec,
                                   final long connectTimeoutMsec,
                                   final long keepAliveTimeoutMsec,
                                   final int keepAliveCount)
            throws ChannelException, QuorumPeerConfig.ConfigException {
        this.type = type;
        try {
            for (String server : serverList) {
                serverMap.put(idCount,
                        new QuorumServer(idCount++, server));
            }
        } catch(QuorumPeerConfig.ConfigException exp) {
            LOG.error("Config error: " + exp);
            throw exp;
        }

        for (Map.Entry<Long, QuorumServer> entry :
                serverMap.entrySet()) {
            Long id = entry.getKey();
            HashMap<Long, QuorumServer> mapCpy = (HashMap)serverMap.clone();
            mapCpy.remove(id);
            List<QuorumServer> list = new ArrayList<>();
            list.addAll(mapCpy.values());
            qbastConfMap.put(id, list);
        }

        this.readTimeoutMsec = readTimeoutMsec;
        this.connectTimeoutMsec = connectTimeoutMsec;
        this.keepAliveTimeoutMsec = keepAliveTimeoutMsec;
        this.keepAliveCount = keepAliveCount;
    }

    @Parameterized.Parameters
    public static Collection quorumServerConfigs() {
        return Arrays.asList( new Object[][] {
                //{ "nio", Arrays.asList("localhost:2888:15555",
                // "localhost:2888:16666") },
                /*
                { "netty-ssl", Arrays.asList("localhost:2888:25555",
                        "localhost:2888:26666"), 0, 0, 0L, 0 },
                { "netty-ssl", Arrays.asList("localhost:2888:25556",
                        "localhost:2888:26667"), 0, 10, 0L, 0 },
                */
                { "netty", Arrays.asList("localhost:2888:27777",
                        "localhost:2888:28888",
                        "localhost:2888:29999"), 0, 10, 0L, 0 },
                { "netty-ssl", Arrays.asList("localhost:2888:25557",
                        "localhost:2888:26668"), 250, 100, 100, 3 },
                /*
                { "nio", Arrays.asList("localhost:2888:17777",
                "localhost:2888:18888",
                        "localhost:19999") },
                { "nio", Arrays.asList("localhost:2888:20001",
                        "localhost:2888:21111",
                        "localhost:2888:22222", "localhost:2888:23333",
                        "localhost:2888:24444") },
                 */
        });
    }

    /**
     * Test a random sender and message being received by rest of them.
     */
    @SuppressWarnings("unchecked")
    @Test(timeout = 3500)
    public void testBroadcast() throws Exception {
        Vote v = new Vote (random.nextLong(), random.nextLong(),
                random.nextLong());

        LOG.info("Sending value: " + v.toString() + " Size: " + v.toString
                ().length());
        org.apache.zookeeper.server.quorum.QuorumBroadcast sender = getSingleSender();
        HashMap<Long, org.apache.zookeeper.server.quorum.QuorumBroadcast> rcvMap;
        rcvMap = (HashMap)qbcastMap.clone();
        rcvMap.remove(sender.sid());

        sender.broadcast(v);

        List<Vote> rxMsgs = new ArrayList<>();

        HashSet<Long> gotSet = new HashSet<>();
        while(rxMsgs.size() < rcvMap.size()) {
            sender.runNow();
            for (org.apache.zookeeper.server.quorum.QuorumBroadcast rxQb : rcvMap.values()) {
                if (gotSet.contains(rxQb.sid())) {
                    continue;
                }

                // Only ones we have heard from yet.
                rxQb.runNow();
                Vote m = inboundVoteQueue.poll();
                if (m != null && m.equals(v)) {
                    rxMsgs.add(m);
                    gotSet.add(m.getSid());
                    break;
                }
            }
        }

        for (Vote rxMsg  : rxMsgs) {
            assertTrue(rxMsg.equals(v));
        }
    }

    @Test(timeout = 15000)
    public void runBroadcast10Times() throws Exception {
        for (int i = 0; i < 10; i++) {
            testBroadcast();
        }
    }

    @Test
    public void runBroadcastAndWait() throws Exception {
        testBroadcast();
        Thread.sleep(10000);
    }

    private QuorumBroadcast getSingleSender() {
        Iterator<Map.Entry<Long, QuorumBroadcast>> iter
                = qbcastMap.entrySet().iterator();
        int rnd = random.nextInt(qbcastMap.values().size());
        for (int i = 0; i < rnd && iter.hasNext(); i++, iter.next());
        Map.Entry<Long, QuorumBroadcast> sendEntry = iter.next();
        return sendEntry.getValue();
    }
}
