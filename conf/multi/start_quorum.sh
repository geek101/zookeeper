#! /bin/bash

set -x
# Get classpath
cpath=$1
spath=$2

if [ -z "$cpath" ]; then
    echo "usage: $0 <classpath> <ssl cert path>"
    exit 1;
fi

if [ -z "$spath" ]; then
    echo "usage: $0 <classpath> <ssl cert path>"
    exit 1;
fi

if [ -z "${cpath}/zookeeper-3.4.8-SNAPSHOT.jar" ]; then
    echo "jar not found"
    exit 1;
fi

rm -f ${cpath}/zookeeper.jar > /dev/null 2>&1
ln -s ${cpath}/zookeeper-3.4.8-SNAPSHOT.jar ${cpath}/zookeeper.jar

rm -rf /tmp/zookeeper/* > /dev/null 2>&1

mkdir -p /tmp/zookeeper/multi/node1
echo "01" > /tmp/zookeeper/multi/node1/myid
mkdir -p /tmp/zookeeper/multi/node2
echo "02" > /tmp/zookeeper/multi/node2/myid
mkdir -p /tmp/zookeeper/multi/node3
echo "03" > /tmp/zookeeper/multi/node3/myid

nohup java -Djavax.net.debug=ssl:handshake -Dlog4j.debug -Dlog4j.configuration="file:${PWD}/node1.log4j.properties" -Dquorum.ssl.enabled="true" -Dquorum.ssl.keyStore.location="${spath}/x509ca/java/node1.ks" -Dquorum.ssl.keyStore.password="CertPassword1" -Dquorum.ssl.trustStore.location="${spath}/x509ca/java/truststore.ks" -Dquorum.ssl.trustStore.password="StorePass" -Dquorum.ssl.trustStore.rootCA.alias="ca" -cp ${cpath}/zookeeper.jar:${cpath}/lib/* org.apache.zookeeper.server.quorum.QuorumPeerMain $PWD/zoo1.cfg &>node1.out &

nohup java -Djavax.net.debug=ssl:handshake -Dlog4j.debug -Dlog4j.configuration="file:${PWD}/node2.log4j.properties" -Dquorum.ssl.enabled="true" -Dquorum.ssl.keyStore.location="${spath}/x509ca/java/node2.ks" -Dquorum.ssl.keyStore.password="CertPassword1" -Dquorum.ssl.trustStore.location="${spath}/x509ca/java/truststore.ks" -Dquorum.ssl.trustStore.password="StorePass" -Dquorum.ssl.trustStore.rootCA.alias="ca" -cp ${cpath}/zookeeper.jar:${cpath}/lib/* org.apache.zookeeper.server.quorum.QuorumPeerMain $PWD/zoo2.cfg &>node2.out &

nohup java -Djavax.net.debug=ssl:handshake -Dlog4j.debug -Dlog4j.configuration="file:${PWD}/node3.log4j.properties" -Dquorum.ssl.enabled="true" -Dquorum.ssl.keyStore.location="${spath}/x509ca/java/node3.ks" -Dquorum.ssl.keyStore.password="CertPassword1" -Dquorum.ssl.trustStore.location="${spath}/x509ca/java/truststore.ks" -Dquorum.ssl.trustStore.password="StorePass" -Dquorum.ssl.trustStore.rootCA.alias="ca" -cp ${cpath}/zookeeper.jar:${cpath}/lib/* org.apache.zookeeper.server.quorum.QuorumPeerMain $PWD/zoo3.cfg &>node3.out &
