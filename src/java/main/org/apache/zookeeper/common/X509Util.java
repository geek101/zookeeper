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
package org.apache.zookeeper.common;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.InvalidKeyException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PublicKey;
import java.security.SignatureException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509KeyManager;
import javax.net.ssl.X509TrustManager;
import javax.xml.bind.DatatypeConverter;

import org.apache.zookeeper.SSLCertCfg;
import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.server.quorum.util.ZKDynamicX509TrustManager;
import org.apache.zookeeper.server.quorum.util.ZKPeerX509TrustManager;
import org.apache.zookeeper.server.quorum.util.ZKX509TrustManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static javax.xml.bind.DatatypeConverter.printHexBinary;
import static org.apache.zookeeper.common.X509Exception.KeyManagerException;
import static org.apache.zookeeper.common.X509Exception.SSLContextException;
import static org.apache.zookeeper.common.X509Exception.TrustManagerException;

/**
 * Utility code for X509 handling
 */
public class X509Util {
    private static final Logger LOG = LoggerFactory.getLogger(X509Util.class);

    /**
     * @deprecated Use {@link ZKConfig#SSL_VERSION_DEFAULT}
     *             instead.
     */
    @Deprecated
    public static final String SSL_VERSION_DEFAULT = "TLSv1";
    /**
     * @deprecated Use {@link ZKConfig#SSL_VERSION}
     *             instead.
     */
    @Deprecated
    public static final String SSL_VERSION = "zookeeper.ssl.version";
    /**
     * @deprecated Use {@link ZKConfig#SSL_KEYSTORE_LOCATION}
     *             instead.
     */
    @Deprecated
    public static final String SSL_KEYSTORE_LOCATION = "zookeeper.ssl.keyStore.location";
    /**
     * @deprecated Use {@link ZKConfig#SSL_KEYSTORE_PASSWD}
     *             instead.
     */
    @Deprecated
    public static final String SSL_KEYSTORE_PASSWD = "zookeeper.ssl.keyStore.password";
    /**
     * @deprecated Use {@link ZKConfig#SSL_TRUSTSTORE_LOCATION}
     *             instead.
     */
    @Deprecated
    public static final String SSL_TRUSTSTORE_LOCATION = "zookeeper.ssl.trustStore.location";
    /**
     * @deprecated Use {@link ZKConfig#SSL_TRUSTSTORE_PASSWD}
     *             instead.
     */
    @Deprecated
    public static final String SSL_TRUSTSTORE_PASSWD = "zookeeper.ssl.trustStore.password";
    /**
     * @deprecated Use {@link ZKConfig#SSL_AUTHPROVIDER}
     *             instead.
     */
    @Deprecated
    public static final String SSL_AUTHPROVIDER = "zookeeper.ssl.authProvider";

    /**
     * @deprecated Use {@link ZKConfig#SSL_TRUSTSTORE_CA_ALIAS}
     *             instead.
     */
    @Deprecated
    public static final String SSL_TRUSTSTORE_CA_ALIAS =
            "zookeeper.ssl.trustStore.rootCA.alias";
    /**
     * @deprecated Use {@link ZKConfig#SSL_DIGEST_DEFAULT_ALGO}
     *             instead.
     */
    @Deprecated
    public static final String SSL_DIGEST_DEFAULT_ALGO ="SHA-256";
    /**
     * @deprecated Use {@link ZKConfig#SSL_DIGEST_ALGOS}
     *             instead.
     */
    @Deprecated
    public static final String SSL_DIGEST_ALGOS = "quorum.ssl.digest.algos";

    /**
     * API to create SSL context for clients. Supports both self-signed and
     * CA signed.
     * @param peerAddr host that client is trying to connect to
     * @param peerCertCfg host's self-signed cert of CA signed cert.
     * @return SSLContext with right verification based on self-signed or CA
     * signed.
     * @throws SSLContextException
     */
    public static SSLContext createSSLContext(
            final ZKConfig config,
            final InetSocketAddress peerAddr,
            final SSLCertCfg peerCertCfg)
            throws SSLContextException {
        final KeyManager[] keyManagers = createKeyManagers(config);
        TrustManager[] trustManagers;
        if (peerCertCfg.isSelfSigned()) {
            trustManagers = createTrustManagers(config, peerAddr,
                    peerCertCfg.getCertFingerPrintStr());
        } else if (peerCertCfg.isCASigned()) {
            // Lets load the CA for truststore.
            trustManagers = createTrustManagers(config, null);
        } else {
            throw new IllegalArgumentException("Invalid argument, no SSL cfg " +
                    "provided");
        }

        return createSSLContext(config, keyManagers, trustManagers);
    }

    /**
     * SSL context which can be used by both client and server side which
     * depend on dynamic config for authentication. Hence we need quorumPeer.
     * @param quorumPeer Used for getting QuorumVerifier and certs from
     *                   QuorumPeerConfig. Both commited and last verified.
     * @return SSLContext which can perform authentication based on dynamic cfg.
     * @throws SSLContextException
     */
    public static SSLContext createSSLContext(final ZKConfig config,
                                              final QuorumPeer quorumPeer)
            throws SSLContextException {
        final KeyManager[] keyManagers = createKeyManagers(config);
        final TrustManager[] trustManagers =
                createTrustManagers(config, quorumPeer);

        return createSSLContext(config, keyManagers, trustManagers);
    }

    private static KeyManager[] createKeyManagers(final ZKConfig config) throws
            SSLContextException {
        final String keyStoreLocationProp =
                config.getProperty(ZKConfig.SSL_KEYSTORE_LOCATION);
        final String keyStorePasswordProp =
                config.getProperty(ZKConfig.SSL_KEYSTORE_PASSWD);

        if (keyStoreLocationProp == null && keyStorePasswordProp == null) {
            LOG.warn("keystore not specified for client connection");
            return null;
        } else {
            if (keyStoreLocationProp == null) {
                throw new SSLContextException("keystore location not " +
                        "specified for client connection");
            }
            if (keyStorePasswordProp == null) {
                throw new SSLContextException("keystore password not " +
                        "specified for client connection");
            }
            try {
                return new KeyManager[]{
                        createKeyManager(keyStoreLocationProp,
                                keyStorePasswordProp)};
            } catch (KeyManagerException e) {
                throw new SSLContextException("Failed to create KeyManager", e);
            }
        }
    }

    /**
     * If QuorumPeer is not provided and this is called it means we are CA
     * mode and need both truststore location and password.
     * @param quorumPeer
     * @return
     * @throws SSLContextException
     */
    private static TrustManager[] createTrustManagers(
            final ZKConfig config,
            final QuorumPeer quorumPeer) throws SSLContextException {
        String trustStoreLocationProp =
                config.getProperty(ZKConfig.SSL_TRUSTSTORE_LOCATION);
        String trustStorePasswordProp =
                config.getProperty(ZKConfig.SSL_TRUSTSTORE_PASSWD);

        if (trustStoreLocationProp == null && trustStorePasswordProp == null) {
            if (quorumPeer == null) {
                final String errStr = "truststore not specified";
                LOG.error(errStr);
                throw new SSLContextException(errStr);
            }

            // Create self-signed verification using QuorumPeer.
            return new TrustManager[] {createTrustManager(quorumPeer)};
        } else {
            if (trustStoreLocationProp == null) {
                throw new SSLContextException("truststore location not " +
                        "specified for client connection");
            }
            if (trustStorePasswordProp == null) {
                throw new SSLContextException("truststore password not " +
                        "specified for client connection");
            }
            try {
                return new TrustManager[] {
                        createTrustManager(config, trustStoreLocationProp,
                                trustStorePasswordProp)};
            } catch (TrustManagerException e) {
                throw new SSLContextException(
                        "Failed to create TrustManager", e);
            }
        }
    }

    private static TrustManager[] createTrustManagers(
            final ZKConfig config,
            final InetSocketAddress peerAddr,
            final String peerCertFingerPrintStr) {
        return new TrustManager[]{
                    createTrustManager(config, peerAddr,
                            peerCertFingerPrintStr)};
    }

    private static SSLContext createSSLContext(
            final ZKConfig config,
            final KeyManager[] keyManagers,
            final TrustManager[] trustManagers)
            throws SSLContextException {
        String sslVersion = config.getProperty(ZKConfig.SSL_VERSION);
        if (sslVersion == null) {
            sslVersion = ZKConfig.SSL_VERSION_DEFAULT;
        }
        SSLContext sslContext;
        try {
            sslContext = SSLContext.getInstance(sslVersion);
            sslContext.init(keyManagers, trustManagers, null);
        } catch (Exception e) {
            throw new SSLContextException(e);
        }
        return sslContext;
    }

    public static X509KeyManager createKeyManager(
            final String keyStoreLocation, final String keyStorePassword)
            throws KeyManagerException {
        try {

            KeyStore ks = loadKeyStore(keyStoreLocation, keyStorePassword);
            KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
            kmf.init(ks, keyStorePassword.toCharArray());

            for (KeyManager km : kmf.getKeyManagers()) {
                if (km instanceof X509KeyManager) {
                    return (X509KeyManager) km;
                }
            }
            throw new KeyManagerException("Couldn't find X509KeyManager");

        } catch (KeyStoreException | NoSuchAlgorithmException |
                CertificateException | UnrecoverableKeyException |
                IOException e) {
            throw new KeyManagerException(e);
        }
    }

    private static KeyStore loadKeyStore(final String keyStoreLocation,
                                  final String keyStorePassword)
    throws KeyStoreException, NoSuchAlgorithmException,
            CertificateException, IOException {
        char[] keyStorePasswordChars = keyStorePassword.toCharArray();
        File keyStoreFile = new File(keyStoreLocation);
        KeyStore ks = KeyStore.getInstance("JKS");
        try (final FileInputStream inputStream = new FileInputStream
                (keyStoreFile)) {
            ks.load(inputStream, keyStorePasswordChars);
        }
        return ks;
    }

    private static X509TrustManager createTrustManager(
            final ZKConfig config,
            final InetSocketAddress peerAddr,
            final String peerCertFingerPrintStr) {
        return new ZKPeerX509TrustManager(peerAddr, peerCertFingerPrintStr);
    }

    public static X509TrustManager createTrustManager(
            final ZKConfig config,
            final String trustStoreLocation, final String trustStorePassword)
            throws TrustManagerException, SSLContextException {
        String trustStoreCAAlias =
                config.getProperty(ZKConfig.SSL_TRUSTSTORE_CA_ALIAS);
        if (trustStoreCAAlias == null) {
            final String errStr = "No CA Alias provided, need one to work in " +
                    "CA mode.";
            LOG.error(errStr);
            throw new TrustManagerException(errStr);
        }
        try(final FileInputStream inputStream =
                    new FileInputStream(new File(trustStoreLocation))) {
                char[] trustStorePasswordChars =
                        trustStorePassword.toCharArray();
                KeyStore ts = KeyStore.getInstance("JKS");
                ts.load(inputStream, trustStorePasswordChars);
                TrustManagerFactory tmf =
                        TrustManagerFactory.getInstance("SunX509");
                tmf.init(ts);
                X509Certificate rootCA =
                        getCertWithAlias(ts, trustStoreCAAlias);
                if (rootCA == null) {
                    final String str = "failed to find root CA from: " +
                            trustStoreLocation + " with alias: " +
                            trustStoreCAAlias;
                    LOG.error(str);
                    throw new TrustManagerException(str);
                }

                return createTrustManager(rootCA);
        } catch (IOException | KeyStoreException | NoSuchAlgorithmException
                | CertificateException e) {
            final String errStr = "Could not load truststore: "
                    + trustStoreLocation;
            LOG.error("{}", errStr, e);
            throw new TrustManagerException(errStr, e);
        }
    }

    private static X509TrustManager createTrustManager(
            final X509Certificate rootCA) {
        return new ZKX509TrustManager(rootCA);
    }

    public static X509TrustManager createTrustManager(
            final QuorumPeer quorumPeer) {
        return new ZKDynamicX509TrustManager(quorumPeer);
    }
    private static X509Certificate getCertWithAlias(
            final KeyStore trustStore, final String alias)
            throws KeyStoreException {
        X509Certificate cert;
        try {
            cert = (X509Certificate) trustStore.getCertificate(alias);
        } catch (KeyStoreException exp) {
            LOG.error("failed to load CA cert, exp: " + exp);
            throw exp;
        }

        return cert;
    }

    /**
     * Parse parsed system property and find a valid algo that matches
     * the finger print passed. Will return null if it couldn't
     * @param config ZKConfig
     * @param fingerPrint Digest of cert
     * @return MessageDigest object, null on error.
     */
    public static MessageDigest getSupportedMessageDigestForFpStr(
            final ZKConfig config, final String fingerPrint) {
        final String[] algos = getConfigureDigestAlgos(config);
        String validAlgo = null;

        for (int i = 0; i < algos.length; i++) {
            LOG.info("Trying available algo: " + algos[i]);
            if (fingerPrint.toLowerCase().startsWith(algos[i])) {
                validAlgo = algos[i];
                break;
            }
        }

        // If there is no valid algo then return null
        if (validAlgo == null) {
            LOG.error("Could not find valid algo str in fingerprint: " +
                    fingerPrint);
            return null;
        }

        MessageDigest md = getMessageDigestByAlgo(config, validAlgo);
        if (md == null) {
            return null;
        }

        // Validate that given input matches expected length for
        // the supported algorithm
        final String fp = fingerPrint.trim().toUpperCase()
                .replace(md.getAlgorithm(), "")
                .replace("-", "").toLowerCase();

        byte[] b = DatatypeConverter.parseHexBinary(fp);
        if (b.length != md.getDigestLength()) {
            LOG.error("Invalid digest, length mismatch for fingerprint: " +
                    fingerPrint + "has length: " + b.length + "algo: " +
                    md.getAlgorithm() + " needs length: " +
                    md.getDigestLength());
            return null;
        }
        LOG.info("given str fp: " + fp + ", got fp: " +
                printHexBinary(b));
        return md;
    }

    private static String[] getConfigureDigestAlgos(final ZKConfig config) {
        String digest_algos = config.getProperty(ZKConfig.SSL_DIGEST_ALGOS);
        if (digest_algos == null) {
            digest_algos = ZKConfig.SSL_DIGEST_DEFAULT_ALGO;
        }

        return digest_algos.trim().toLowerCase().split(",");
    }

    private static MessageDigest getMessageDigestByAlgo(
            final ZKConfig config, final String validAlgo) {
        MessageDigest md = null;
        try {
            LOG.info("Valid algo: " + validAlgo);
            md = MessageDigest.getInstance(validAlgo.toUpperCase());
        } catch (NoSuchAlgorithmException e) {
            LOG.error("Invalid algo: " + validAlgo + " support algos: " +
                    String.join(",", getConfigureDigestAlgos(config)));
        }

        return md;
    }

    /**
     * Get the right MessageDigest i.e only if it is configured and validate
     * the cert with the given finger print.
     * @param fingerPrint
     * @param cert
     * @return True on success
     * @throws CertificateEncodingException
     */
    public static boolean validateCert(final ZKConfig config,
                                       final String fingerPrint,
                                       final X509Certificate cert)
            throws CertificateEncodingException, NoSuchAlgorithmException {
        final MessageDigest fpMsgDigest =
                getSupportedMessageDigestForFpStr(config, fingerPrint);
        if (fpMsgDigest == null) {
            return false;
        }

        return validateCert(fpMsgDigest, fingerPrint, cert);
    }

    public static boolean validateCert(final MessageDigest messageDigest,
                                       final String fingerPrintStr,
                                       final X509Certificate cert)
            throws CertificateEncodingException, NoSuchAlgorithmException {
        return fingerPrintStr.toLowerCase().equals(
                SSLCertCfg.getDigestToCertFp(
                        getMessageDigestFromCert(cert,
                                messageDigest.getAlgorithm())).toLowerCase());
    }

    public static MessageDigest getMessageDigestFromCert(
            final X509Certificate cert, final String messageDigestAlgo)
            throws NoSuchAlgorithmException, CertificateEncodingException {
        final MessageDigest certMsgDigest =
                MessageDigest.getInstance(messageDigestAlgo);

        certMsgDigest.update(cert.getEncoded());
        return certMsgDigest;
    }

    /**
     * Checks whether given X.509 certificate is self-signed.
     */
    public static boolean verifySelfSigned(X509Certificate cert)
            throws CertificateException {
        try {
            // Try to verify certificate signature with its own public key
            final PublicKey key = cert.getPublicKey();
            cert.verify(key);
            return true;
        } catch (InvalidKeyException | SignatureException |
                NoSuchAlgorithmException | NoSuchProviderException exp) {
            // Invalid signature --> not self-signed
            final String errStr = "Invalid not self-signed";
            LOG.error("{}", errStr, exp);
            throw new CertificateException(exp);
        }
    }
}
