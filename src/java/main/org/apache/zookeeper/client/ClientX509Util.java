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
package org.apache.zookeeper.client;

import java.net.InetSocketAddress;

import javax.net.ssl.SSLContext;

import org.apache.zookeeper.common.X509ChainedTrustManager;
import org.apache.zookeeper.common.X509Exception;
import org.apache.zookeeper.common.X509Util;
import org.apache.zookeeper.common.ZKConfig;
import org.apache.zookeeper.server.quorum.util.ZKPeerX509TrustManager;
import org.apache.zookeeper.server.quorum.util.ZKX509TrustManager;

public class ClientX509Util extends X509Util {
    /**
     * SSL context which depends given peer's digest along with regular
     * verification via Truststore done first.
     * @param zkConfig Given client config.
     * @param peerAddr The Zookeeper server address to connect to
     * @param peerCertFingerPrintStr The digest for the above server.
     * @return SSLContext which can perform authentication.
     * @throws X509Exception.SSLContextException
     */
    public static SSLContext createSSLContext(
            final ZKConfig zkConfig, final InetSocketAddress peerAddr,
            final String peerCertFingerPrintStr)
            throws X509Exception.SSLContextException {
        if (peerCertFingerPrintStr == null) {
            final String errStr = "Peer's: " + peerAddr +
                    " certificate fingerprint cannot be null";
            LOG.error(errStr);
            throw new IllegalAccessError(errStr);
        }
        try {
            return createSSLContext(zkConfig, new X509ChainedTrustManager(
                    new ZKX509TrustManager(zkConfig.getProperty(
                            ZKConfig.SSL_TRUSTSTORE_LOCATION),
                            zkConfig.getProperty(
                                    ZKConfig.SSL_TRUSTSTORE_PASSWD)),
                    new ZKPeerX509TrustManager(zkConfig,
                            peerAddr, peerCertFingerPrintStr)));
        } catch (X509Exception.TrustManagerException exp) {
            throw new X509Exception.SSLContextException(exp);
        }
    }
}