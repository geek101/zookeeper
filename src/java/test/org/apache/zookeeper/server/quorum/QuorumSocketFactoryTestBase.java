package org.apache.zookeeper.server.quorum;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * TODO: Remove the file dependency.
 * Created by powell on 12/10/15.
 */
public class QuorumSocketFactoryTestBase {
    protected static ArrayList<String> keyStore
            = new ArrayList<>(Arrays.asList("x509ca/java/node1.ks",
            "x509ca/java/node2.ks",
            "x509ca/java/node3.ks"));
    protected static ArrayList<String> keyPassword
            = new ArrayList<>(
            Arrays.asList("CertPassword1", "CertPassword1", "CertPassword1"));
    protected static ArrayList<String> trustStore = new ArrayList<>(
            Arrays.asList("x509ca/java/truststore.ks"));
    protected static ArrayList<String> trustPassword
            = new ArrayList<>(Arrays.asList("StorePass"));
    protected static String trustStoreCAAlias = "ca";

    protected static ArrayList<String> badKeyStore
            = new ArrayList<>(Arrays.asList("x509ca2/java/node1.ks",
            "x509ca2/java/node2.ks",
            "x509ca2/java/node3.ks"));
    protected static ArrayList<String> badKeyPassword
            = new ArrayList<>(
            Arrays.asList("CertPassword1", "CertPassword1", "CertPassword1"));
    protected static ArrayList<String> badTrustStore = new ArrayList<>(
            Arrays.asList("x509ca2/java/truststore.ks"));
    protected static ArrayList<String> badTrustPassword
            = new ArrayList<>(Arrays.asList("StorePass"));
    protected static String badTrustStoreCAAlias = "ca";
}
