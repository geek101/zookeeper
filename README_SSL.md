# Zookeeper Quorum SSL

Provides SSL for Leader Election and ZAB i.e ports 3888 and 2888.

Each Zookeeper ensemble will need one self-signed certificate, lets call this root CA and each Zookeeper server will its own certificate signed by the root CA.
Servers exchange credentials on connect and they are verified by the public key of the stored root CA.

### How to Run

Pass the following args to JVM enable SSL:
```
-Dquorum.ssl.enabled="true"
-Dquorum.ssl.keyStore.location="<Private key and signed cert, key store file>"
-Dquorum.ssl.keyStore.password="<Password for the above>"
-Dquorum.ssl.trustStore.location="<Root CA cert, key store file"
-Dquorum.ssl.trustStore.password="<Password for the above"
```

Example:
```
java -Djavax.net.debug=ssl:handshake -Dlog4j.debug -Dlog4j.configuration="file:${PWD}/node1.log4j.properties" -Dquorum.ssl.enabled="true" -Dquorum.ssl.keyStore.location="${spath}/x509ca/java/node1.ks" -Dquorum.ssl.keyStore.password="CertPassword1" -Dquorum.ssl.trustStore.location="${spath}/x509ca/java/truststore.ks" -Dquorum.ssl.trustStore.password="StorePass" -Dquorum.ssl.trustStore.rootCA.alias="ca" -cp ${cpath}/zookeeper.jar:${cpath}/lib/* org.apache.zookeeper.server.quorum.QuorumPeerMain $PWD/zoo1.cfg
```

##### Note

Keystore password must be the same as password used to store the private key of the node.
Keystore cannot have more than 1 key.
To help with debug please add -Djavax.net.debug=ssl:handshake.

#### Script helpers

##### Generating Root CA and certs for Zookeeper nodes
Use the scripts and config files in *resources/* directory.

###### Step 1
To generate root CA cd to x509ca dir and perform the following steps:

```
resources/x509ca$ ../init.sh
```

> use defaults and enter yes to load root self-signed cert to truststore>

###### Step 2

Now generate certs for every node, ex:

```
resources/x509ca$ ../gencert.sh node1
```

> you will be prompted for private key password, enter: CertPassword1
> note: you can enter any password but remember to change the script to support that if you do so.
> Repeat Step 2 for as many nodes as you want.

###### Step 3

Running a three node zookeeper cluster

Create three loopback interfaces 127.0.1.1, 127.0.1.2, 127.0.1.3 and run *start_quorum.sh* in *config/multi/*
```
$ sudo ifconfig lo:1 127.0.1.1 netmask 255.255.255.0
$ cd conf/multi/
$ ./start_quorum.sh
```

> Verify the logs in */tmp/zookeeper/multi/node<id>.log* and SSL debug data in
> *conf/multi/node<id>.out*
> If logs look good then you could use zkCli.sh to test the cluster.

```
bin/zkCli.sh -server 127.0.1.1:2181
```

##### Unit test

Currently unit test expects keystore files to be available via absolute path.
Edit *src/java/test/org/apache/zookeeper/server/quorum/QuorumSocketFactoryTest.java* and point **PATH** to *resources/* directory

##### Todo

1. Remove keystore file dependency for UT
2. Automate fat jar systest to test with SSL.
3. Dream up a way to write some junit with few SSL enabled QuorumPeers!.
4. Support for third party providers.
5. Learn more about X509 verification and what more can apply to this context.

#### License
[Apache 2.0](http://www.apache.org/licenses/LICENSE-2.0)

#### Disclaimer

This is experimental work subject to change without notice.
