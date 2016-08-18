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

import org.apache.zookeeper.common.X509Exception;
import org.apache.zookeeper.server.quorum.util.QuorumSocketFactory;
import org.apache.zookeeper.server.quorum.utils.AsyncClientSocket;
import org.apache.zookeeper.server.quorum.utils.AsyncServerSocket;
import org.apache.zookeeper.server.quorum.utils.QuorumServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLHandshakeException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URL;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * TODO: Remove static PATH, my class path is broken for idea.
 * Created by powell on 12/10/15.
 */
public class QuorumSocketFactoryTest extends QuorumSocketFactoryTestBase {
    private static final Logger LOG
            = LoggerFactory.getLogger(QuorumSocketFactory.class);
    private static final String PATH =
            "<zookeeper repo path>/resources/";
    private final QuorumServer listenServer
            = new QuorumServer(1, new InetSocketAddress("localhost", 38888));
    private final QuorumServer client1
            = new QuorumServer(2, new InetSocketAddress("localhost", 45555));
    private final QuorumServer client2
            = new QuorumServer(3, new InetSocketAddress("localhost", 46666));

    private boolean sslEnabled;
    private ServerSocket serverSocket = null;
    private Socket clientSocket1 = null;
    private Socket clientSocket2 = null;

    @Before
    public void setup() {
        this.sslEnabled = true;
    }

    @After
    public void cleanUp() {
        if (clientSocket1 != null) {
            close(clientSocket1);
        }

        if (clientSocket2 != null) {
            close(clientSocket2);
        }

        if (serverSocket != null) {
            close(serverSocket);
        }
    }

    @Test
    public void testListener() throws Exception {
        serverSocket = newServerAndBindTest(listenServer, 0);
        serverSocket.close();
        serverSocket = null;
    }

    @Test
    public void testAccept() throws Exception {
        final int serverIndex = 0;
        final int clientIndex = 1;

        Collection<Socket> sockets = connectOneClientToServerTest(
                client1, listenServer, clientIndex, serverIndex);
        Iterator<Socket> it = sockets.iterator();
        it.next();
        it.next().close();
        clientSocket1.close();
        clientSocket1 = null;
        serverSocket.close();
        serverSocket = null;
    }

    @Test(expected=SSLHandshakeException.class)
    public void testBadClient() throws X509Exception, InterruptedException,
            ExecutionException, IOException, NoSuchAlgorithmException {
        readWriteTestHelper(connectOneBadClientToServerTest(
                client1, listenServer, 0, 1), "HelloWorld!");
    }

    @Test
    public void testReadWrite() throws X509Exception, InterruptedException,
            ExecutionException, IOException, NoSuchAlgorithmException {
        readWriteTestHelper(connectOneClientToServerTest(
                client1, listenServer, 0, 1), "HelloWorld!");
    }

    private void readWriteTestHelper(Collection<Socket> sockets,
                                     final String testStr)
            throws InterruptedException, ExecutionException, IOException {
        Iterator<Socket> it = sockets.iterator();
        // Write from client
        FutureTask<Void> writeFuture
                = new AsyncClientSocket(it.next()).write(testStr);
        FutureTask<String> readFuture
                = new AsyncClientSocket(it.next()).read();

        while(!writeFuture.isDone()
                || !readFuture.isDone()) {
            Thread.sleep(2);
        }

        String str = null;
        try {
            str = readFuture.get();
        } catch (ExecutionException exp) {
            if (exp.getCause() != null &&
                    exp.getCause() instanceof IOException) {
                if (exp.getCause() instanceof SSLHandshakeException) {
                    throw (SSLHandshakeException) exp.getCause();
                } else {
                    throw (IOException) exp.getCause();
                }
            } else {
                throw exp;
            }
        }

        assertEquals("data txrx", testStr, str);
        it = sockets.iterator();
        it.next();
        it.next().close();
        clientSocket1.close();
        clientSocket1 = null;
        serverSocket.close();
        serverSocket = null;
    }

    private Collection<Socket> connectOneClientToServerTest(
            final QuorumServer from, final QuorumServer to,
            int clientIndex, int serverIndex)
            throws X509Exception, IOException, InterruptedException,
            ExecutionException, NoSuchAlgorithmException {

        return connectOneClientToServerTest(newClient(from, clientIndex),
                to, serverIndex);
    }

    private Collection<Socket> connectOneBadClientToServerTest(
            final QuorumServer from, final QuorumServer to,
            int clientIndex, int serverIndex)
            throws X509Exception, IOException, InterruptedException,
            ExecutionException, NoSuchAlgorithmException {

        return connectOneClientToServerTest(newBadClient(from, clientIndex),
                to, serverIndex);
    }

    private Collection<Socket> connectOneClientToServerTest(final Socket client,
                                                            final QuorumServer to, int serverIndex)
            throws X509Exception, IOException, InterruptedException,
            ExecutionException, NoSuchAlgorithmException {
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

    private Socket newClient(final QuorumServer from, int index)
            throws X509Exception, IOException, NoSuchAlgorithmException {
        return startClient(from, keyStore.get(index), keyPassword.get(index),
                trustStore.get(0), trustPassword.get(0), trustStoreCAAlias);
    }

    private Socket newBadClient(final QuorumServer from, int index)
            throws X509Exception, IOException, NoSuchAlgorithmException {
        return startClient(from, badKeyStore.get(index),
                badKeyPassword.get(index),
                badTrustStore.get(0), badTrustPassword.get(0),
                badTrustStoreCAAlias);
    }

    private Socket startClient(final QuorumServer from,
                               final String keyStoreLocation,
                               final String keyStorePassword,
                               final String trustStoreLocation,
                               final String trustStorePassword,
                               final String trustStoreCAAlias)
            throws X509Exception, IOException, NoSuchAlgorithmException {
        ClassLoader cl = getClass().getClassLoader();
        URL resource = cl.getResource("");
        LOG.info("root path: " + resource.getPath());
        if (this.sslEnabled) {
            return QuorumSocketFactory.createForSSL(PATH + keyStoreLocation,
                    keyStorePassword,
                    PATH + trustStoreLocation,
                    trustStorePassword,
                    trustStoreCAAlias).buildForClient();
        } else {
            return QuorumSocketFactory.createWithoutSSL().buildForClient();
        }
    }

    private ServerSocket newServerAndBindTest(final QuorumServer server,
                                              int index)
            throws X509Exception, IOException, NoSuchAlgorithmException {
        ServerSocket s = startListener(server, index);
        assertTrue("bind worked", s.isBound());
        return s;
    }

    private ServerSocket startListener(final QuorumServer server, int index)
            throws X509Exception, IOException, NoSuchAlgorithmException {
        ClassLoader cl = getClass().getClassLoader();
        QuorumSocketFactory quorumSocketFactory = null;
        if (this.sslEnabled) {
            quorumSocketFactory = QuorumSocketFactory.createForSSL(PATH +
                    keyStore.get(index),
                    keyPassword.get(index),
                    PATH + trustStore.get(0),
                    trustPassword.get(0),
                    trustStoreCAAlias);
        } else {
            quorumSocketFactory = QuorumSocketFactory.createWithoutSSL();
        }

        return quorumSocketFactory.buildForServer(server.addr()
                .getPort(), server.addr().getAddress());
    }

    private void close(ServerSocket s) {
        try {
            s.close();
        } catch (IOException e) {
        }
    }

    private void close(Socket s) {
        try {
            s.close();
        } catch (IOException e) {}
    }
}
