/**
 *  Licensed to the Apache Software Foundation (ASF) under one
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

package org.apache.zookeeper.server.quorum;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;

import org.apache.zookeeper.common.SSLContextCreator;
import org.apache.zookeeper.common.X509Exception;
import org.apache.zookeeper.server.quorum.util.Callback;
import org.apache.zookeeper.server.quorum.util.ChannelException;

/**
 * Interface for QuorumBroadcast, has the external facing API.
 */
public interface QuorumBroadcast {
    /**
     * What is my id.
     * @return my sid given at creation time.
     */
    long sid();

    /**
     * Add a new unique server to broadcast pool. Will throw exception if
     * same server is added again.
     * @param server
     * @throws ChannelException
     */
    void addServer(final QuorumServer server) throws ChannelException;

    /**
     * Remove an existing server from broadcast pool, Will throw exception
     * if an unknown server is removed.
     * @param server
     * @throws ChannelException
     */
    void removeServer(final QuorumServer server) throws ChannelException;

    /**
     * Might block based on underlying implementation.
     * @throws Exception
     */
    void start(final Callback<Vote> msgRxCb,
               final SSLContextCreator sslContextCreator)
            throws IOException, ChannelException,
            CertificateException, NoSuchAlgorithmException,
            X509Exception.KeyManagerException,
            X509Exception.TrustManagerException;

    /**
     * API to broadcast the given vote.
     * This is not a queue service. If a previous outstanding
     * vote (i.e vote could not be sent etc) exists it will
     * not be sent any more and will be replaced by the new message.
     * @param vote
     */
    void broadcast(final Vote vote);

    /**
     * API to shutdown the listener and incoming messages.
     */
    void shutdown() throws InterruptedException;

    /**
     * API for NIO case, not implemented for Netty case.
     */
    void runNow();
}
