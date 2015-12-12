# Zookeeper Quorum SSL

Provides SSL for Leader Election and ZAB i.e ports 3888 and 2888.

Each Zookeeper ensemble will need one self-signed certificate, lets call this root CA and each Zookeeper server will its own certificate signed by the root CA.
Servers exchange credentials on connect and they are verified by the public key of the stored root CA.

### How to Run

**Depends Java 1.7+**

##### Building


```
git checkout branch-3.4
ant jar
```

Args to enable SSL:
```
-Dquorum.ssl.enabled="true"
-Dquorum.ssl.keyStore.location="<Private key and signed cert, key store file>"
-Dquorum.ssl.keyStore.password="<Password for the above>"
-Dquorum.ssl.trustStore.location="<Root CA cert, key store file"
-Dquorum.ssl.trustStore.password="<Password for the above"
```

Example run command:
```
java -Dquorum.ssl.enabled="true" -Dquorum.ssl.keyStore.location="node1.ks" -Dquorum.ssl.keyStore.password="CertPassword1" -Dquorum.ssl.trustStore.location="truststore.ks" -Dquorum.ssl.trustStore.password="StorePass" -Dquorum.ssl.trustStore.rootCA.alias="ca" -cp zookeeper.jar:lib/* org.apache.zookeeper.server.quorum.QuorumPeerMain zoo1.cfg
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
Edit *src/java/test/org/apache/zookeeper/server/quorum/QuorumSocketFactoryTest.java* and point **PATH** to *resources/* directory.

Also generate certs and keys in *resources/x509ca2* for the negative test to pass.

##### Benchmark

Reference: [Zookeeper performance doc](https://wiki.apache.org/hadoop/ZooKeeper/ServiceLatencyOverview)

###### Configuration
A 3 Server ensemble with config:
 * OS - Ubuntu 14.04 x86_64 VM
 * Java 1.8.0_60
 * 4 GB Memory
 * Intel(R) Core(TM) i7-3615QM CPU @ 2.30GHz
 * 4 Core reserved for VM
 * 8 Core Macbook with hyperthreading enabled.
 * SSD for logging and data

###### Runtime args

 * Log level set to WARN
 * 1 Client
 * Command used
  ```
zk-latencies.py --cluster "127.0.1.1:2181,127.0.1.2:2181,127.0.1.3:2181"--znode_size=100 --znode_count=10000 --timeout=5000 --watch_multiple=5
```
 * 5 runs each

###### Results

**SSL Disabled**

```
110000 ops took 16441.6 ms
```

Data average of 5 runs
```
created 10000 permanent : min=1493, avg=1674.4, max=2283, var=1.11202e+07
set 10000 : min=1523, avg=1696.2, max=1905, var=11492844
get 10000 : min=1372, avg=1435.2, max=1500, var=8.23678e+06
deleted 10000 permanent : min=1191, avg=1376.4, max=1606, var=7.5591e+06
created 10000 ephemeral : min=1485, avg=1548.4, max=1712, var=9.58304e+06
watched 50000 : min=7128, avg=7241, max=7346, var=209722940
deleted 10000 ephemeral : min=1410, avg=1470, max=1587, var=8.63879e+06
```

**SSL Enabled**

```
110000 ops took 16721.2 ms
```

Data average of 5 runs
```
created 10000 permanent : min=1514, avg=1745.6, max=2210, var=1.21277e+07
set 10000 : min=1583, avg=1742, max=1957, var=1.21195e+07
get 10000 : min=1460, avg=1483.4, max=1513, var=8.80151e+06
deleted 10000 permanent : min=1059, avg=1353.4, max=1519, var=7.30167e+06
created 10000 ephemeral : min=1481, avg=1604.8, max=1804, var=1.02897e+07
watched 50000 : min=7152, avg=7288.8, max=7442, var=2.12497e+08
deleted 10000 ephemeral : min=1253, avg=1503.2, max=1659, var=9.02019e+06
```

###### Conclusion

SSL run is 1.7% slower. For this specific test SSL support provided by default Java provider does not seem to impact performance by a large margin. More detailed test with increase in client number and run count is needed

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
