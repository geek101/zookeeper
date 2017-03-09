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

import java.net.InetAddress;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.List;

import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.server.quorum.QuorumPeerDynamicCertCheck;
import org.apache.zookeeper.server.quorum.QuorumServer;


public class QuorumPeerDynCheckWrapper implements QuorumPeerDynamicCertCheck {
    final HashMap<Long, QuorumServer> quorumServerMap;

    public QuorumPeerDynCheckWrapper() {
        this.quorumServerMap = new HashMap<>();
    }

    public void addQuorumServer(final QuorumServer quorumServer) {
        this.quorumServerMap.put(quorumServer.id, quorumServer);
    }

    public void setQuorumServerMap(final List<QuorumServer> quorumServerList) {
        quorumServerMap.clear();
        quorumServerList.forEach(x -> quorumServerMap.put(x.id(), x));
    }

    @Override
    public String getQuorumServerFingerPrintByElectionAddress(
            final InetAddress serverElectionAddr) {
        return QuorumPeer.getQuorumServerFingerPrintByElectionAddress
                (quorumServerMap, serverElectionAddr);
    }

    @Override
    public String getQuorumServerFingerPrintByCert(final X509Certificate cert)
            throws CertificateEncodingException, NoSuchAlgorithmException {
        return QuorumPeer.getQuorumServerFingerPrintByCert(
                quorumServerMap, cert);
    }
}
