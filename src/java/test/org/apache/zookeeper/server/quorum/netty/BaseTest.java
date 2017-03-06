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
package org.apache.zookeeper.server.quorum.netty;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.concurrent.Future;
import org.apache.zookeeper.SSLCertCfg;
import org.apache.zookeeper.client.ClientX509Util;
import org.apache.zookeeper.common.X509Exception;
import org.apache.zookeeper.common.X509Util;
import org.apache.zookeeper.common.ZKConfig;
import org.apache.zookeeper.server.quorum.AbstractServer;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.QuorumServer;
import org.apache.zookeeper.server.quorum.Vote;
import org.apache.zookeeper.server.quorum.helpers.AsyncClientSocket;
import org.apache.zookeeper.server.quorum.helpers.AsyncServerSocket;
import org.apache.zookeeper.server.quorum.helpers.QuorumPeerDynCheckWrapper;
import org.apache.zookeeper.server.quorum.util.ChannelException;
import org.apache.zookeeper.server.quorum.util.QuorumSocketFactory;
import org.apache.zookeeper.server.quorum.util.QuorumX509Util;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.netty.buffer.Unpooled.buffer;
import static org.apache.zookeeper.common.ZKConfig.SSL_KEYSTORE_LOCATION;
import static org.apache.zookeeper.common.ZKConfig.SSL_KEYSTORE_PASSWD;
import static org.apache.zookeeper.common.ZKConfig.SSL_TRUSTSTORE_LOCATION;
import static org.apache.zookeeper.server.quorum.util.InitMessageCtx.PROTOCOL_VERSION;
import static org.apache.zookeeper.server.quorum.util.InitMessageCtx.getAddrString;
import static org.junit.Assert.assertTrue;

public class BaseTest {
    protected static final Logger LOG = LoggerFactory.getLogger(
            BaseTest.class.getName());
    protected X509ClusterBase x509Cluster;

    protected List<String> keyStore;
    protected List<String> keyPassword;
    protected List<String> trustStore ;
    protected List<String> trustPassword;
    protected static String trustStoreCAAlias = "ca";

    protected List<String> badKeyStore;
    protected List<String> badKeyPassword;
    protected List<String> badTrustStore;
    protected List<String> badTrustPassword;
    protected static String badTrustStoreCAAlias = "ca";

    protected final ExecutorService executor = Executors
            .newSingleThreadExecutor();
    protected EventLoopGroup eventLoopGroup = null;

    protected boolean sslEnabled = true;

    protected ServerSocket serverSocket = null;
    protected Socket clientSocket1 = null;
    protected Socket clientSocket2 = null;

    protected QuorumPeerDynCheckWrapper quorumPeerDynCheckWrapper =
            new QuorumPeerDynCheckWrapper();

    protected List<QuorumPeerConfig> quorumPeerConfigList =
            new ArrayList<>();

    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

    @Before
    public void setup() throws Exception {
        x509Cluster = new X509ClusterCASigned("foo",
                testFolder.newFolder("ssl").toPath(), 3);
        keyStore = new ArrayList<>();
        x509Cluster.getKeyStoreList().forEach(x -> keyStore.add(x.toString()));
        keyPassword = x509Cluster.getKeyStorePasswordList();
        trustStore = Collections.singletonList(
                x509Cluster.getTrustStore().toString());
        trustPassword = Collections.singletonList(
                x509Cluster.getTrustStorePassword());
        badKeyStore = new ArrayList<>();
        x509Cluster.getBadKeyStoreList().forEach(x -> badKeyStore.add(x.toString()));
        badKeyPassword = x509Cluster.getBadKeyStorePasswordList();
        badTrustStore = Collections.singletonList(
                x509Cluster.getBadTrustStore().toString());
        badTrustPassword = Collections.singletonList(
                x509Cluster.getBadTrustStorePassword());

    }

    protected ChannelFuture startListener(final InetSocketAddress listenerAddr,
                                          final EventLoopGroup group,
                                          final NettyChannel handler)
            throws ChannelException {

        ServerBootstrap serverBootstrap = new ServerBootstrap();
        serverBootstrap.group(group)
                .channel(NioServerSocketChannel.class)
                .handler(new LoggingHandler(LogLevel.INFO))
                .option(ChannelOption.SO_REUSEADDR, true)
                .childHandler(new AcceptInitializer(handler))
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.SO_LINGER, 0);

        return (serverBootstrap.bind(listenerAddr));
    }

    protected class AcceptInitializer
            extends ChannelInitializer<SocketChannel> {
        private final NettyChannel handler;
        public AcceptInitializer(NettyChannel handler) {
            this.handler = handler;
        }
        @Override
        public void initChannel(SocketChannel ch) throws Exception {
            ChannelPipeline p = ch.pipeline();
            handler.setChannel(ch);
            p.addLast("serverhandler", handler);
        }
    }

    /**
     * Synchronous connector.
     * @param server
     * @param group
     * @param handler
     * @throws InterruptedException
     */
    protected void startConnectionSync(
            final AbstractServer server, final EventLoopGroup group,
            final ChannelHandler handler, long timeoutMsec)
            throws InterruptedException {
        // Create an new Channel.
        startConnectionHelper(group, handler)
                .connect(server.getElectionAddr()).sync().await(timeoutMsec);
    }

    protected Future startConnectionAsync(final AbstractServer server,
                                          final EventLoopGroup group,
                                          final ChannelHandler handler)
            throws InterruptedException {
        // Create an new Channel and return the future.
        return startConnectionHelper(group, handler)
                .connect(server.getElectionAddr());
    }

    protected Bootstrap startConnectionHelper(
            final EventLoopGroup group, final ChannelHandler handler) {
        Bootstrap clientBootstrap = new Bootstrap();
        clientBootstrap.group(group)
                .channel(NioSocketChannel.class)
                .handler(handler)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_LINGER, 0);
        return clientBootstrap;
    }

    protected Collection<Socket> connectOneClientToServerTest(
            final QuorumServer from, final QuorumServer to,
            int clientIndex, int serverIndex)
            throws X509Exception, IOException, InterruptedException,
            ExecutionException, NoSuchAlgorithmException, CertificateException,
            KeyStoreException {

        return connectOneClientToServerTest(
                newClient(from, clientIndex,to, serverIndex),
                to, serverIndex);
    }

    protected Collection<Socket> connectOneBadClientToServerTest(
            final QuorumServer from, final QuorumServer to,
            int clientIndex, int serverIndex)
            throws X509Exception, IOException, InterruptedException,
            ExecutionException, NoSuchAlgorithmException, CertificateException,
            KeyStoreException {

        return connectOneClientToServerTest(newBadClient(from, clientIndex,
                to, serverIndex),
                to, serverIndex);
    }

    protected Collection<Socket> connectOneClientToServerTest(
            final Socket client, final QuorumServer to, int serverIndex)
            throws X509Exception, IOException, InterruptedException,
            ExecutionException, NoSuchAlgorithmException, CertificateException,
            KeyStoreException {
        serverSocket = newServerAndBindTest(to, serverIndex);

        clientSocket1 = client;
        FutureTask<Socket> clientSocketFuture
                = new AsyncClientSocket(clientSocket1).connect(to);
        FutureTask<Socket> serverSocketFuture
                = new AsyncServerSocket(serverSocket).accept();

        while (!clientSocketFuture.isDone()
                || !serverSocketFuture.isDone()) {
            Thread.sleep(2);
        }

        assertTrue("connected", clientSocketFuture.get().isConnected());
        assertTrue("accepted", serverSocketFuture.get().isConnected());
        return Collections.unmodifiableList(
                Arrays.asList(clientSocketFuture.get(),
                        serverSocketFuture.get()));
    }

    protected Socket newClient(final QuorumServer from, final int fromIndex,
                               final QuorumServer to, final int toIndex)
            throws X509Exception, IOException, NoSuchAlgorithmException,
            CertificateException, KeyStoreException {
        return startClient(createQuorumPeerConfig(fromIndex, keyStore,
                keyPassword, 0, trustStore, trustPassword), to,
                toIndex);
    }

    protected Socket newBadClient(final QuorumServer from, final int fromIndex,
                                  final QuorumServer to, final int toIndex)
            throws X509Exception, IOException, NoSuchAlgorithmException,
            CertificateException, KeyStoreException {
        return startClient(createQuorumPeerConfig(fromIndex, badKeyStore,
                badKeyPassword, toIndex, badTrustStore,
                badTrustPassword), to, toIndex);
    }

    protected Socket startClient(final QuorumPeerConfig quorumPeerConfig,
                                 final QuorumServer to,
                                 final int toIndex)
            throws X509Exception, IOException, NoSuchAlgorithmException,
            CertificateException, KeyStoreException {
        ClassLoader cl = getClass().getClassLoader();
        if (sslEnabled) {
            return ClientX509Util.createSSLContext(quorumPeerConfig,
                    to.getElectionAddr(),
                    getSSlCertStr(toIndex)).getSocketFactory()
                    .createSocket();
        } else {
            return QuorumSocketFactory.createWithoutSSL()
                    .buildForClient(to.getElectionAddr(),
                    new SSLCertCfg(getSSlCertStr(toIndex)));
        }
    }

    protected ServerSocket newServerAndBindTest(final QuorumServer server,
                                                int index)
            throws X509Exception, IOException, NoSuchAlgorithmException,
            CertificateException, KeyStoreException {
        ServerSocket s = startListener(server, index);
        s.setReuseAddress(true);
        assertTrue("bind worked", s.isBound());
        return s;
    }

    protected ServerSocket startListener(final QuorumServer server, int index)
            throws X509Exception, IOException, NoSuchAlgorithmException,
            CertificateException, KeyStoreException {
        ClassLoader cl = getClass().getClassLoader();
        if (sslEnabled) {
            final QuorumPeerConfig quorumPeerConfig =
                    createQuorumPeerConfig(index, keyStore, keyPassword,
                            0, trustStore, trustPassword);

            server.setSslCertCfg(new SSLCertCfg(getSSlCertStr(index)));

            quorumPeerDynCheckWrapper.addQuorumServer(server);

            // TODO: Refactor to use QuorumSocketFactory
            return QuorumX509Util.createSSLContext
                    (quorumPeerDynCheckWrapper, quorumPeerConfig)
                    .getServerSocketFactory()
                    .createServerSocket(server.getElectionAddr().getPort(),
                            QuorumSocketFactory.LISTEN_BACKLOG,
                            server.getElectionAddr().getAddress());
        } else {
            return QuorumSocketFactory.createWithoutSSL()
                    .buildForServer(quorumPeerDynCheckWrapper,
                    server.getElectionAddr().getPort(),
                            server.getElectionAddr().getAddress());
        }
    }

    protected Socket connectAndAssert(final QuorumServer from,
                                      final QuorumServer to)
            throws IOException, X509Exception, NoSuchAlgorithmException,
            CertificateException, KeyStoreException {
        // Connect and write a hdr, check the return value
        Socket socket = newClient(from, 1, to, 0);
        socket.setTcpNoDelay(true);
        socket.setSoLinger(true, 0);
        socket.setKeepAlive(true);
        socket.connect(to.getElectionAddr());

        assertTrue("connected", socket.isConnected());
        return socket;
    }

    protected void sendVerifyVote(long leader, long zxid,
                                  long sid, Socket socket,
                                  ConcurrentLinkedQueue<Vote> inboundVoteQueue)
            throws IOException, InterruptedException {
        Vote v = new Vote(leader, zxid, sid);

        ByteBuf voteBuf = v.buildMsg();
        writeBufToSocket(voteBuf, socket);

        Vote vRx = null;
        while ((vRx = inboundVoteQueue.poll()) == null) {
            Thread.sleep(1);
        }
        LOG.debug("Got vote: " + vRx);
        assertTrue(vRx.match(v));
    }

    protected void writeBufToSocket(ByteBuf byteBuf, Socket socket)
            throws IOException {
        int size = byteBuf.readableBytes();
        byte b[] = new byte[size];
        byteBuf.readBytes(b);
        socket.getOutputStream().write(b);
    }

    protected ByteBuf buildHdr(long sid, InetSocketAddress myElectionAddr) {
        ByteBuf buf = buffer(2046);
        final String electionAddrStr
                = getAddrString(myElectionAddr);
        buf.writeLong(PROTOCOL_VERSION)
                .writeLong(sid)
                .writeInt(electionAddrStr.getBytes().length)
                .writeBytes(electionAddrStr.getBytes());
        return buf;
    }

    protected boolean hdrEquals(long sid, InetSocketAddress myElectionAddr,
                                ByteBuf hdrRx) {
        final String electionAddrStr
                = getAddrString(myElectionAddr);
        boolean val1 = hdrRx.readLong() == PROTOCOL_VERSION &&
                hdrRx.readLong() == sid &&
                hdrRx.readInt() == electionAddrStr.getBytes().length;

        byte[] b = new byte[electionAddrStr.getBytes().length];
        hdrRx.readBytes(b);
        String readAddr = new String(b);
        return val1 && readAddr.compareTo(electionAddrStr) == 0;
    }

    protected ByteBuf readHelper(final Socket socket, int len)
            throws IOException {
        byte[] b = new byte[len];
        BufferedInputStream bis =
                new BufferedInputStream(socket.getInputStream());
        int readLen = 0;
        while(readLen < len) {
            readLen += bis.read(b, readLen, len-readLen);
        }

        ByteBuf buf = Unpooled.buffer(len);
        buf.writeBytes(b);
        return buf;
    }

    protected void socketIsClosed(final Socket socket, final ByteBuf buf)
            throws InterruptedException, IOException {
        while(!socket.isClosed()) {
            Thread.sleep(100);
            try {
                socket.getInputStream().read();
                if (buf != null) {
                    writeBufToSocket(buf, socket);
                } else {
                    socket.getOutputStream().write(new byte[1]);
                }
            } catch (IOException exp) {
                LOG.info("client socket read exp: {}", exp);
                throw exp;
            }
        }
    }

    protected void setSockOptions(final Socket socket) throws SocketException {
        socket.setSoLinger(true, 0);
        socket.setReuseAddress(true);
        socket.setTcpNoDelay(true);
        socket.setKeepAlive(true);
    }

    protected void close(ServerSocket s) {
        try {
            s.close();
        } catch (IOException e) {
        }
    }

    protected void close(Socket s) {
        try {
            s.close();
        } catch (IOException e) {}
    }

    public static QuorumPeerConfig createQuorumPeerConfig(
            final ClassLoader cl, final String keyStore,
            final String keyStorePassword, final String trustStore,
            final String trustPassword) {
        final QuorumPeerConfig quorumPeerConfig = new QuorumPeerConfig();
        quorumPeerConfig.setProperty(SSL_KEYSTORE_LOCATION, keyStore);
        quorumPeerConfig.setProperty(SSL_KEYSTORE_PASSWD,
                keyStorePassword);
        quorumPeerConfig.setProperty(SSL_TRUSTSTORE_LOCATION,
                trustStore);
        quorumPeerConfig.setProperty(SSL_KEYSTORE_PASSWD,
                trustPassword);
        return quorumPeerConfig;
    }

    protected QuorumPeerConfig createQuorumPeerConfig(
            final int keyIndex, final List<String> keyStore,
            final List<String> keyPassword, final int trustIndex,
            final List<String> trustStore, final List<String> trustPassword) {
        return createQuorumPeerConfig(getClass().getClassLoader(),
                keyStore.get(keyIndex), keyPassword.get(keyIndex),
                trustStore.get(trustIndex), trustPassword.get(trustIndex));
    }

    public static String getSSlCertStr(final ClassLoader cl,
                                       final String keyStore,
                                       final String keyPassword)
            throws CertificateException, NoSuchAlgorithmException, KeyStoreException, IOException {
        final KeyStore keyStoreObj = X509Util.loadKeyStore(
                cl.getResource(keyStore).getFile(), keyPassword);
        final Enumeration<String> aliases = keyStoreObj.aliases();
        if (!aliases.hasMoreElements()) {
            throw new IllegalAccessError("Keystore: "
                    + cl.getResource(keyStore).getFile()
                    + " has no alias");
        }
        final X509Certificate cert = (X509Certificate)keyStoreObj
                .getCertificate(aliases.nextElement());
;
        final MessageDigest md = X509Util.getMessageDigestFromCert(cert,
                ZKConfig.SSL_DIGEST_DEFAULT_ALGO);
        return SSLCertCfg.getDigestToCertFp(md);
    }

    private String getSSlCertStr(final int index, final List<String> keyStore,
                                 final List<String> keyPassword) throws
            CertificateException, NoSuchAlgorithmException, KeyStoreException,
            IOException {
        return getSSlCertStr(getClass().getClassLoader(), keyStore.get(0),
                keyPassword.get(0));
    }

    private String getSSlCertStr(final int index) throws CertificateException,
            NoSuchAlgorithmException, KeyStoreException, IOException {
        return getSSlCertStr(index, keyStore, keyPassword);
    }
}
