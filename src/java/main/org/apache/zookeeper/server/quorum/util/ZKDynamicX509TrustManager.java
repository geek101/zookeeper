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

import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedTrustManager;

import org.apache.commons.validator.routines.InetAddressValidator;
import org.apache.zookeeper.common.X509Util;
import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZKDynamicX509TrustManager extends X509ExtendedTrustManager {
    private static final Logger LOG
            = LoggerFactory.getLogger(ZKPeerX509TrustManager.class);

    private final QuorumPeer quorumPeer;

    public ZKDynamicX509TrustManager(final QuorumPeer quorumPeer) {
        this.quorumPeer = quorumPeer;
    }

    @Override
    public void checkClientTrusted(X509Certificate[] x509Certificates, String s)
            throws CertificateException {
        validateSingleAndSelfSigned(x509Certificates);
    }

    @Override
    public void checkServerTrusted(X509Certificate[] x509Certificates, String s)
            throws CertificateException {
        validateSingleAndSelfSigned(x509Certificates);
    }

    public X509Certificate[] getAcceptedIssuers() {
        return new X509Certificate[]{};
    }

    public void checkClientTrusted(
            final X509Certificate[] certs, final String authType,
            final Socket socket)
            throws CertificateException {
        validateSingleAndSelfSigned(certs);
    }

    public void checkServerTrusted(
            final X509Certificate[] certs, final String authType,
            final Socket socket)
            throws CertificateException {
        validateSingleAndSelfSigned(certs);
    }

    @Override
    public void checkClientTrusted(X509Certificate[] x509Certificates, String s,
                                   SSLEngine sslEngine)
            throws CertificateException {
        validateSingleAndSelfSigned(x509Certificates);
    }

    @Override
    public void checkServerTrusted(X509Certificate[] x509Certificates, String s,
                                   SSLEngine sslEngine)
            throws CertificateException {
        validateSingleAndSelfSigned(x509Certificates);
    }

    private void validateSingleAndSelfSigned(final X509Certificate[] certs)
            throws CertificateException {
        if (certs.length == 0) {
            final String errStr = "Invalid server, did not send any cert";
            LOG.error(errStr);
            throw new CertificateException(errStr);
        }

        try {
            validatePeerCert(certs[0]);
        } catch (NoSuchAlgorithmException exp) {
            final String errStr = "Unable to validate peer cert";
            LOG.error("{}", errStr, exp);
            throw new CertificateException(errStr, exp);
        }
    }

    private void validatePeerCert(
            final X509Certificate cert, final String peerHost)
            throws CertificateException {
        if(peerHost == null || peerHost.trim().isEmpty()) {
            final String errStr = "Invalid peerHost, cannot be null or empty";
            LOG.error(errStr);
            throw new CertificateException(errStr);
        }

        if (!InetAddressValidator.getInstance().isValid(peerHost)) {
            final String errStr = "Invalid peerHost, should be a valid IP";
            LOG.error(errStr);
            throw new CertificateException(errStr);
        }

        // We have a valid ip address
        try {
            validatePeerCert(cert, InetAddress.getByName(peerHost));
        } catch (UnknownHostException exp) {
            final String errStr = "Invalid peerHost, should be a valid IP";
            LOG.error("{}", errStr, exp);
            throw new CertificateException(errStr, exp);
        }
    }

    private void validatePeerCert(final X509Certificate cert,
                                  final InetAddress peerAddr)
            throws CertificateException {

        LOG.info("validating cert: " + cert);
        // Verify that server presented a self-signed cert.
        X509Util.verifySelfSigned(cert);

        final String peerCertFingerPrint =
                quorumPeer.getQuorumServerFingerPrintByElectionAddress(
                        peerAddr);

        // If we could not get the fp then bail!.
        if (peerCertFingerPrint == null) {
            final String errStr = "Invalid cert and peerAddr: " +
                    peerAddr + " could not find fingerprint for this address";
            LOG.error(errStr);
            throw new CertificateException(errStr);
        }

        validatePeerCert(cert, peerCertFingerPrint);
        LOG.info("validation done");
    }

    private void validatePeerCert(final X509Certificate cert)
            throws CertificateException, NoSuchAlgorithmException {
        if (quorumPeer == null) {
            throw new IllegalAccessError("Cannot be used this way, quorumPeer" +
                    " is null");
        }

        LOG.info("validating cert: " + cert);
        final String peerCertFingerPrint =
                quorumPeer.getQuorumServerFingerPrintByCert(cert);

        // If we could not get the fp then bail!.
        if (peerCertFingerPrint == null) {
            final String errStr = "Invalid cert";
            LOG.error(errStr);
            throw new CertificateException(errStr);
        }

        LOG.info("validation done");
    }
}
