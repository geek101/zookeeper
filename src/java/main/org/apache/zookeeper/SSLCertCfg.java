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

package org.apache.zookeeper;

import java.security.MessageDigest;
import java.util.HashMap;
import java.util.Map;

import org.apache.zookeeper.common.X509Util;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static javax.xml.bind.DatatypeConverter.printHexBinary;

public class SSLCertCfg {
    private static final Logger LOG = LoggerFactory.getLogger(SSLCertCfg.class);
    private final String certFingerPrintStr;

    public SSLCertCfg() {
        certFingerPrintStr = null;
    }

    public SSLCertCfg(final String certFingerPrintStr) {
        this.certFingerPrintStr = certFingerPrintStr;
    }

    public String getCertFingerPrintStr() {
        return certFingerPrintStr;
    }

    public static SSLCertCfg parseCertCfgStr(final String certCfgStr)
            throws QuorumPeerConfig.ConfigException {
        final String[] parts = certCfgStr.split(":");
        final Map<String, Integer> propKvMap =
                getKeyAndIndexMap(certCfgStr);
        if (propKvMap.containsKey("cert")) {
            int fpIndex = propKvMap.get("cert") + 1;
            if (parts.length <= fpIndex) {
                final String errStr = "No fingerprint provided for self " +
                        "signed, server cfg string: " + certCfgStr;
                throw new QuorumPeerConfig.ConfigException(errStr);
            }
            LOG.debug("certCfgStr: " + certCfgStr +
                    ", fp:" + parts[fpIndex]);
            if (getMessageDigest(parts[fpIndex]) != null) {
                return new SSLCertCfg(parts[fpIndex]);
            }
        }

        return new SSLCertCfg();
    }

    private static Map<String, Integer> getKeyAndIndexMap(final String cfgStr) {
        final Map<String, Integer> propKvMap = new HashMap<>();
        final String[] parts = cfgStr.split(":");
        for (int i = 0; i < parts.length; i++) {
            propKvMap.put(parts[i].trim().toLowerCase(), i);
        }

        return propKvMap;
    }

    /**
     * Given a fingerprint get the supported message digest object for that.
     * @param fp fingerprint.
     * @return MessageDigest, null on error
     */
    private static MessageDigest getMessageDigest(final String fp)
            throws QuorumPeerConfig.ConfigException {
        // Check for supported algos for the given fingerprint if cannot
        // validate throw exception.
        MessageDigest md = X509Util.getSupportedMessageDigestForFpStr(
                new QuorumPeerConfig(), fp);
        if (md == null) {
            final String errStr = "Algo in fingerprint: " + fp +
                    " not supported, bailing out";
            throw new QuorumPeerConfig.ConfigException(errStr);
        }

        return md;
    }

    public static String getDigestToCertFp(final MessageDigest md) {
        return md.getAlgorithm().toLowerCase() + "-" +
                printHexBinary(md.digest()).toLowerCase();
    }
}
