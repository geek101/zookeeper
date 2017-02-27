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
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.zookeeper.common.X509Exception;
import org.apache.zookeeper.server.quorum.QuorumServer;
import org.apache.zookeeper.server.quorum.Vote;
import org.apache.zookeeper.server.quorum.helpers.PortAssignment;
import org.apache.zookeeper.server.quorum.util.Callback;
import org.apache.zookeeper.server.quorum.util.ChannelException;
import io.netty.buffer.ByteBuf;
import io.netty.channel.nio.NioEventLoopGroup;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.zookeeper.server.quorum.util.QuorumSSLContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class VotingChannelMgrTest extends BaseTest {
    private static final Logger LOG = LoggerFactory.getLogger
            (VotingChannelMgrTest.class);
    private final String type;
    private final Long readTimeoutMsec;
    private final Long connectTimeoutMsec;
    private final Long keepAliveTimeoutMsec;
    private final Integer keepAliveCount;
    private final Long sidStart;
    private final Long sidEnd;
    private final QuorumServer listenServer;
    private final QuorumServer client1;
    private Long sidRunning;
    private VotingChannelMgr votingChannelMgr;
    final long rgenseed = System.currentTimeMillis();
    Random random = new Random(rgenseed);

    private final ConcurrentLinkedQueue<Vote> inboundVoteQueue
            = new ConcurrentLinkedQueue<>();

    private final class MsgRxCb implements Callback<Vote> {
        @Override
        public void call(final Vote o) throws ChannelException, IOException {
            inboundVoteQueue.add(o);
        }
    }

    private final MsgRxCb msgRxCb = new MsgRxCb();

    @Parameterized.Parameters
    public static Collection quorumServerConfigs() {
        return Arrays.asList( new Object[][] {
                // SSL, ListenAddr, ReadTimeout, ConnectTimeout,
                // KeepAliveTimeout, KeepAliveCount, StartSid,
                // EndSid
                //{ "plain", 15555, 0L, 0L, 0L, 0, 1L, 99L},
                //{ "ssl", 25555, 0L, 0L, 0L, 0, 100L, 199L},
                { "plain", PortAssignment.unique(), 100, 10, 100L, 3, 200L,
                        299L},
                { "ssl", PortAssignment.unique(), 300L, 200L, 100L, 3, 300L,
                        399L},
    });}

    public VotingChannelMgrTest(final String type, final int listenAddr,
                                final long readTimeoutMsec,
                                final long connectTimeoutMsec,
                                final long keepAliveTimeoutMsec,
                                final int keepAliveCount,
                                final long sidStart, final long sidEnd) {
        this.type = type;
        this.readTimeoutMsec = readTimeoutMsec;
        this.connectTimeoutMsec = connectTimeoutMsec;
        this.keepAliveTimeoutMsec = keepAliveTimeoutMsec;
        this.keepAliveCount = keepAliveCount;
        this.sidStart = sidStart;
        this.sidRunning = this.sidStart;
        this.sidEnd = sidEnd;

        listenServer = new QuorumServer(this.sidRunning++,
                new InetSocketAddress("localhost",
                        PortAssignment.unique()));
        client1 = new QuorumServer(this.sidRunning++,
                new InetSocketAddress(
                        "localhost", PortAssignment.unique()));
    }

    @Before
    public void setup() throws Exception {
        eventLoopGroup = new NioEventLoopGroup(1, executor);
        LOG.info("Setup type: " + type);
        ClassLoader cl = getClass().getClassLoader();
        if (type.equals("plain")) {
            sslEnabled = false;
        } else if (type.equals("ssl")) {
            sslEnabled = true;
        } else {
            throw new IllegalArgumentException("type is invalid:" + type);
        }

        votingChannelMgr = new VotingChannelMgr(listenServer.id(),
                listenServer.getElectionAddr(), readTimeoutMsec,
                connectTimeoutMsec, keepAliveTimeoutMsec, keepAliveCount,
                eventLoopGroup, sslEnabled, msgRxCb,
                new QuorumSSLContext(quorumPeerDynCheckWrapper,
                        createQuorumPeerConfig(0, keyStore, keyPassword,
                                0, trustStore, trustPassword)));
    }

    @After
    public void tearDown() throws Exception {
        if (votingChannelMgr != null) {
            LOG.info("Shutting voting channel mgr");
            votingChannelMgr.shutdown();
        }
        LOG.info("Shutting down eventLoopGroup");
        eventLoopGroup.shutdownGracefully().sync();
    }

    @Test(timeout = 2000)
    public void testListener() throws IOException, NoSuchAlgorithmException,
            X509Exception, CertificateException, ChannelException,
            InterruptedException, KeyStoreException {
        mgrInit();
        close(connectAndAssert(client1, listenServer));
    }

    @Test(timeout = 1000, expected = SocketException.class)
    public void testIncomingInvalidServer() throws IOException,
            NoSuchAlgorithmException, X509Exception, CertificateException,
            ChannelException, InterruptedException, KeyStoreException {
        mgrInit();

        Socket clientSocket = connectAndAssert(client1, listenServer);
        writeBufToSocket(buildHdr(client1.id(), client1.getElectionAddr()),
                clientSocket);

        socketIsClosed(clientSocket, null);
    }

    @Test(timeout = 1000)
    public void oneServerDuplex() throws IOException,
            NoSuchAlgorithmException, X509Exception, CertificateException,
            ChannelException, InterruptedException, KeyStoreException {
        socketPairClose(oneServerDuplexHelper(client1, 1));
    }

    @Test(timeout = 2000, expected = SocketException.class)
    public void oneServerDeInit() throws IOException,
            NoSuchAlgorithmException, X509Exception, CertificateException,
            ChannelException, InterruptedException, KeyStoreException {
        ImmutablePair<ServerSocket, Socket> socketPair =
                oneServerDuplexHelper(client1, 1);

        // Remove the server
        votingChannelMgr.removeServer(client1);

        // Verify that the above triggers a disconnect
        socketIsClosed(socketPair.getRight(), null);

        socketPairClose(socketPair);
    }

    @Test(timeout = 1000)
    public void testOneServerSidResolveAndVote() throws IOException,
            NoSuchAlgorithmException, X509Exception, CertificateException,
            ChannelException, InterruptedException, KeyStoreException {
        ImmutablePair<ServerSocket, Socket> socketPair =
                oneServerDuplexHelper(client1, 1);

        // Connect and send the hdr
        Socket clientSocket = connectAndAssert(client1, listenServer);

        // Send the hdr
        writeBufToSocket(buildHdr(client1.id(), client1.getElectionAddr()),
                clientSocket);

        boolean closedCheck = false;
        // Ensure the previous incoming channel is closed.
        try {
            socketIsClosed(socketPair.getRight(), null);
        } catch (SocketException exp) {
            closedCheck = true;
        }

        assertTrue("closed check", closedCheck);

        socketPairClose(socketPair);

        // Send a Vote via this channel and verify it received.
        assertEquals(inboundVoteQueue.size(), 0);
        sendVerifyVote(123L, 456L, client1.id(), clientSocket,
                inboundVoteQueue);

        close(clientSocket);
    }

    @Test(timeout = 1000)
    public void testVoteRx() throws IOException,
            NoSuchAlgorithmException, X509Exception, CertificateException,
            ChannelException, InterruptedException, KeyStoreException {
        socketPairClose(oneVoteRxHelper(client1, 1,
                new Vote(123L, 456L, listenServer.id())));
    }

    @Test(timeout = 1000)
    public void testSameVoteRxFail() throws IOException,
            NoSuchAlgorithmException, X509Exception, CertificateException,
            ChannelException, InterruptedException, KeyStoreException {
        Vote vote = new Vote(123L, 456L, listenServer.id());
        ImmutablePair<ServerSocket, Socket> socketPair
                = oneVoteRxHelper(client1, 1, vote);

        // Send it again.
        votingChannelMgr.sendVote(vote);

        // Send loop runs ever millisec, 50msec is plenty of time.
        Socket socket = socketPair.getRight();
        socket.setSoTimeout(50);

        final String expectedStr = "Read timed out";
        String expStr = "";
        try {
            readHelper(socketPair.getRight(),
                    vote.buildMsg().readableBytes());
        } catch (SocketTimeoutException exp) {
            expStr = exp.getMessage();
        }

        assertEquals(expectedStr, expStr);

        socketPairClose(socketPair);
    }

    @Test(timeout = 1000)
    public void testVoteRxOnChannelReconnect() throws IOException,
            NoSuchAlgorithmException, X509Exception, CertificateException,
            ChannelException, InterruptedException, KeyStoreException {
        Vote vote = new Vote(123L, 456L, listenServer.id());
        ImmutablePair<ServerSocket, Socket> socketPair
                = oneVoteRxHelper(client1, 1, vote);

        // Close the socket on our end.
        socketPair.getRight().close();

        // Verify re-connect
        Socket socket = socketPair.getLeft().accept();
        setSockOptions(socket);

        assertTrue("re-connected", socket.isConnected());

        // Go past header Rx
        hdrRxVerify(listenServer, socket);

        // Verify vote Rx
        voteRxVerify(listenServer, vote, socket);

        socket.close();
        socketPair.getLeft().close();
    }

    private ImmutablePair<ServerSocket, Socket> oneVoteRxHelper(
            final QuorumServer server, int index,
            final Vote vote) throws IOException,
            NoSuchAlgorithmException, X509Exception, CertificateException,
            ChannelException, InterruptedException, KeyStoreException {
        ImmutablePair<ServerSocket, Socket> socketPair =
                oneServerDuplexHelper(server, index);

        // Send Vote via Mgr and Rx it.
        votingChannelMgr.sendVote(vote);

        voteRxVerify(listenServer, vote, socketPair.getRight());
        return socketPair;
    }

    private ImmutablePair<ServerSocket, Socket> oneServerDuplexHelper(
            final QuorumServer server, int index) throws IOException,
            NoSuchAlgorithmException, X509Exception, CertificateException,
            ChannelException, InterruptedException, KeyStoreException {
        mgrInit();

        // Add the server to mgr
        votingChannelMgr.addServer(server);

        return startListenReadHdr(server, index);
    }

    private ImmutablePair<ServerSocket, Socket> startListenReadHdr(
            final QuorumServer server, int index) throws X509Exception,
            IOException, NoSuchAlgorithmException, CertificateException,
            KeyStoreException {
        // Lets start the server
        ServerSocket serverSocket = newServerAndBindTest(server, index);

        // We should get a connect
        Socket socket = serverSocket.accept();
        setSockOptions(socket);

        hdrRxVerify(listenServer, socket);

        return ImmutablePair.of(serverSocket, socket);
    }

    private void voteRxVerify(final QuorumServer from, final Vote vote,
                              final Socket socket) throws IOException {
        ByteBuf voteBufExpected = vote.buildMsg();
        ByteBuf voteBufRead = readHelper(socket,
                voteBufExpected.readableBytes());
        Vote voteRx = Vote.buildVote(voteBufRead, vote.getSid());
        assertTrue("vote match", vote.match(voteRx));
    }

    private void hdrRxVerify(final QuorumServer from, Socket socket)
            throws IOException {
        // Go past header Rx
        ByteBuf hdrTx = buildHdr(from.id(), from.getElectionAddr());
        ByteBuf hdrRx = readHelper(socket, hdrTx.readableBytes());

        assertTrue("hdr check", hdrEquals(from.id(), from.getElectionAddr(), hdrRx));
    }

    private void mgrInit() throws IOException, NoSuchAlgorithmException,
            X509Exception, CertificateException, ChannelException,
            InterruptedException {
        votingChannelMgr.start();
        votingChannelMgr.waitForListener();
    }

    private void socketPairClose(ImmutablePair<ServerSocket, Socket>
                                 socketPair) {
        close(socketPair.getRight());
        close(socketPair.getLeft());
    }
}
