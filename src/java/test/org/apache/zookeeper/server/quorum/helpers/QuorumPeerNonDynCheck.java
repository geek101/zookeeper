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
package org.apache.zookeeper.server.quorum.helpers;

import java.io.IOException;
import java.net.InetAddress;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import org.apache.zookeeper.SSLCertCfg;
import org.apache.zookeeper.common.X509Util;
import org.apache.zookeeper.common.ZKConfig;
import org.apache.zookeeper.server.quorum.QuorumPeerDynamicCertCheck;
import org.apache.zookeeper.server.quorum.netty.BaseTest;


public class QuorumPeerNonDynCheck implements QuorumPeerDynamicCertCheck {
    private final String certFpStr;
    public QuorumPeerNonDynCheck(final String keyStore,
                                 final String keyStorePassword) {
        try {
            certFpStr = BaseTest.getSSlCertStr(getClass().getClassLoader(),
                    keyStore, keyStorePassword);
        } catch (CertificateException | NoSuchAlgorithmException |
                KeyStoreException | IOException exp) {
            throw new IllegalAccessError(exp.getMessage());
        }
    }

    @Override
    public String getQuorumServerFingerPrintByElectionAddress(
            final InetAddress serverElectionAddr) {
        return certFpStr;
    }

    @Override
    public String getQuorumServerFingerPrintByCert(X509Certificate cert)
            throws CertificateEncodingException, NoSuchAlgorithmException {
        return certFpStr.equals(SSLCertCfg.getDigestToCertFp(
                X509Util.getMessageDigestFromCert(cert, ZKConfig
                        .SSL_DIGEST_DEFAULT_ALGO))) ? certFpStr : null;
    }
}
