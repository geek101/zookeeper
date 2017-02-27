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

import org.apache.zookeeper.common.SSLContextCreator;
import org.apache.zookeeper.common.X509Exception;
import org.apache.zookeeper.server.quorum.QuorumServer;
import org.apache.zookeeper.server.quorum.Vote;
import org.apache.zookeeper.server.quorum.util.Callback;
import org.apache.zookeeper.server.quorum.util.ChannelException;


public class MockQuorumBcastTestBase extends AbstractQuorumBcastTestWrapper {
    public MockQuorumBcastTestBase(final long id, final int quorumSize) {
        super(id, quorumSize);
    }

    @Override
    public void addServer(final QuorumServer server) throws ChannelException {}

    @Override
    public void removeServer(final QuorumServer server) throws
            ChannelException {}

    /**
     * Might block based on underlying implementation.
     *
     * @param msgRxCb
     * @param sslContextCreator
     * @throws Exception
     */
    @Override
    public void start(final Callback<Vote> msgRxCb,
                      final SSLContextCreator sslContextCreator)
            throws IOException, ChannelException,
            CertificateException, NoSuchAlgorithmException, X509Exception
                    .KeyManagerException, X509Exception.TrustManagerException {}

    @Override
    public void broadcast(final Vote vote) {}

    @Override
    public void shutdown() {}

    @Override
    public void runNow() {}
}
