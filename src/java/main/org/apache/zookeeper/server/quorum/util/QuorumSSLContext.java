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
package org.apache.zookeeper.server.quorum.util;

import java.net.InetSocketAddress;

import javax.net.ssl.SSLContext;

import org.apache.zookeeper.common.SSLContextCreator;
import org.apache.zookeeper.common.X509Exception;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeerDynamicCertCheck;


public class QuorumSSLContext implements SSLContextCreator {
    private final QuorumPeerDynamicCertCheck quorumPeerDynamicCertCheck;
    private QuorumPeerConfig quorumPeerConfig;
    public QuorumSSLContext(
            final QuorumPeerDynamicCertCheck quorumPeerDynamicCertCheck,
            final QuorumPeerConfig quorumPeerConfig) {
        this.quorumPeerDynamicCertCheck = quorumPeerDynamicCertCheck;
        this.quorumPeerConfig = quorumPeerConfig;
    }

    public void resetQuorumPeerConfig(final QuorumPeerConfig quorumPeerConfig) {
        this.quorumPeerConfig = quorumPeerConfig;
    }

    @Override
    public SSLContext forClient(final InetSocketAddress peerAddr,
                                final String peerCertFingerPrintStr)
            throws X509Exception.SSLContextException {
        return QuorumX509Util.createSSLContext(quorumPeerConfig, peerAddr,
                peerCertFingerPrintStr);
    }

    @Override
    public SSLContext forServer() throws X509Exception.SSLContextException {
        return QuorumX509Util.createSSLContext(quorumPeerDynamicCertCheck,
                quorumPeerConfig);
    }
}
