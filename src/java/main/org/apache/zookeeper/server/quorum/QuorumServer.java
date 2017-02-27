/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>http://www.apache.org/licenses/LICENSE-2.0</p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.server.quorum;

import java.io.StringWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.SSLCertCfg;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class for a single server which is part of the Quorum.
 */
public class QuorumServer implements AbstractServer {
    private static final Logger LOG =
            LoggerFactory.getLogger(QuorumServer.class);
    public InetSocketAddress addr = null;

    public InetSocketAddress electionAddr = null;

    public InetSocketAddress clientAddr = null;
    public InetSocketAddress secureClientAddr = null;

    public long id;

    public QuorumPeer.LearnerType type = QuorumPeer.LearnerType.PARTICIPANT;

    private SSLCertCfg sslCertCfg;

    private List<InetSocketAddress> myAddrs;

    public QuorumServer(long id, InetSocketAddress addr,
                        InetSocketAddress electionAddr, InetSocketAddress clientAddr) {
        this(id, addr, electionAddr, clientAddr, QuorumPeer.LearnerType.PARTICIPANT);
    }

    public QuorumServer(long id, InetSocketAddress addr,
                        InetSocketAddress electionAddr) {
        this(id, addr, electionAddr, (InetSocketAddress)null, QuorumPeer.LearnerType.PARTICIPANT);
    }

    public QuorumServer(long id, InetSocketAddress addr) {
        this(id, addr, (InetSocketAddress)null, (InetSocketAddress)null, QuorumPeer.LearnerType.PARTICIPANT);
    }

    public long id() {
        return id;
    }
    /**
     * Performs a DNS lookup for server address and election address.
     *
     * If the DNS lookup fails, this.addr and electionAddr remain
     * unmodified.
     */
    public void recreateSocketAddresses() {
        if (this.addr == null) {
            LOG.warn("Server address has not been initialized");
            return;
        }
        if (this.electionAddr == null) {
            LOG.warn("Election address has not been initialized");
            return;
        }
        String host = this.addr.getHostString();
        InetAddress address = null;
        try {
            address = InetAddress.getByName(host);
        } catch (UnknownHostException ex) {
            LOG.warn("Failed to resolve address: {}", host, ex);
            return;
        }
        LOG.debug("Resolved address for {}: {}", host, address);
        int port = this.addr.getPort();
        this.addr = new InetSocketAddress(address, port);
        port = this.electionAddr.getPort();
        this.electionAddr = new InetSocketAddress(address, port);
    }

    private void setType(final HashMap<String, Integer> propKvMap)
            throws QuorumPeerConfig.ConfigException {
        if (propKvMap.containsKey("participant") &&
                propKvMap.containsKey("observer")) {
            throw new QuorumPeerConfig.ConfigException("Contains both participant and " +
                    "observer type");
        } else if (propKvMap.containsKey("participant")) {
            type = QuorumPeer.LearnerType.PARTICIPANT;
        } else if (propKvMap.containsKey("observer")) {
            type = QuorumPeer.LearnerType.OBSERVER;
        }
    }

    private void setType(String s) throws QuorumPeerConfig.ConfigException {
        if (s.toLowerCase().equals("observer")) {
            type = QuorumPeer.LearnerType.OBSERVER;
        } else if (s.toLowerCase().equals("participant")) {
            type = QuorumPeer.LearnerType.PARTICIPANT;
        } else {
            throw new QuorumPeerConfig.ConfigException("Unrecognised peertype: " + s);
        }
    }

    public SSLCertCfg getSslCertCfg() {
        return sslCertCfg;
    }

    /**
     * Used in UT for now.
     * TODO: Remove once UT creates QuorumServer with this configure before
     * hand.
     * @param sslCertCfg Give SSL cert.
     */
    public void setSslCertCfg(final SSLCertCfg sslCertCfg) {
        this.sslCertCfg = sslCertCfg;
    }

    private static String[] splitWithLeadingHostname(String s)
            throws QuorumPeerConfig.ConfigException
    {
            /* Does it start with an IPv6 literal? */
        if (s.startsWith("[")) {
            int i = s.indexOf("]:");
            if (i < 0) {
                throw new QuorumPeerConfig.ConfigException(s + " starts with '[' but has no matching ']:'");
            }

            String[] sa = s.substring(i + 2).split(":");
            String[] nsa = new String[sa.length + 1];
            nsa[0] = s.substring(1, i);
            System.arraycopy(sa, 0, nsa, 1, sa.length);

            return nsa;
        } else {
            return s.split(":");
        }
    }

    private static final String wrongFormat = " does not have the form server_config or server_config;client_config"+
            " where server_config is host:port:port or host:port:port:type and " +
            "client_config is port or host:port or plain:host:port;secure:host:port";

    public QuorumServer(long sid, String addressStr) throws QuorumPeerConfig.ConfigException {
        // LOG.warn("sid = " + sid + " addressStr = " + addressStr);
        this.id = sid;
        String serverClientParts[] = addressStr.split(";");
        String serverParts[] = splitWithLeadingHostname(serverClientParts[0]);
        if ((serverClientParts.length > 3) || (serverParts.length < 3)
                || (serverParts.length > 6)) {
            throw new QuorumPeerConfig.ConfigException(addressStr + wrongFormat);
        }

        // Compatible part is host:port or just port which is used for non
        // secure socket
        // Support for both sockets or either of them is as follows
        // plain:host:port;secure:host:port and similar config for ipv6
        if (serverClientParts.length > 1) {
            parseHostString(serverClientParts[1], addressStr);
            if (serverClientParts.length > 2) {
                parseHostString(serverClientParts[2], addressStr);
            }
        }

        if (serverClientParts.length > 3) {
            throw new QuorumPeerConfig.ConfigException("Invalid server string, more than " +
                    "two sections parsed after ; for given string: " +
                    addressStr);
        }

        // server_config should be either host:port:port or host:port:port:type
        try {
            addr = new InetSocketAddress(serverParts[0],
                    Integer.parseInt(serverParts[1]));
        } catch (NumberFormatException e) {
            throw new QuorumPeerConfig.ConfigException("Address unresolved: " + serverParts[0] + ":" + serverParts[1]);
        }
        try {
            electionAddr = new InetSocketAddress(serverParts[0],
                    Integer.parseInt(serverParts[2]));
        } catch (NumberFormatException e) {
            throw new QuorumPeerConfig.ConfigException("Address unresolved: " + serverParts[0] + ":" + serverParts[2]);
        }

        // Now we can expect the following
        // participant:cert:<sha256 cert fp>
        this.sslCertCfg = new SSLCertCfg();
        if (serverParts.length == 4) {
            // backward compatibility first.
            setType(serverParts[3]);
        } else if (serverParts.length >= 4) {
            // Parse this: cert:SHA-256-XXXX
            final HashMap<String, Integer> propKvMap = new HashMap<>();
            for (int i = 3; i < serverParts.length; i++) {
                propKvMap.put(serverParts[i].trim().toLowerCase(), i);
            }

            setType(propKvMap);
            this.sslCertCfg =
                    SSLCertCfg.parseCertCfgStr(serverClientParts[0]);
        }

        setMyAddrs();
    }

    public QuorumServer(long id, InetSocketAddress addr,
                        InetSocketAddress electionAddr, QuorumPeer.LearnerType type) {
        this(id, addr, electionAddr, (InetSocketAddress)null, type);
    }

    public QuorumServer(long id, InetSocketAddress addr,
                        InetSocketAddress electionAddr, InetSocketAddress clientAddr, QuorumPeer.LearnerType type) {
        this.id = id;
        this.addr = addr;
        this.electionAddr = electionAddr;
        this.type = type;
        this.clientAddr = clientAddr;

        setMyAddrs();
    }

    private void setMyAddrs() {
        this.myAddrs = new ArrayList<InetSocketAddress>();
        this.myAddrs.add(this.addr);
        this.myAddrs.add(this.clientAddr);
        this.myAddrs.add(this.electionAddr);
        this.myAddrs = excludedSpecialAddresses(this.myAddrs);
    }

    private static InetSocketAddress parseCompatibleHostString(
            final String hostStr, final String serverStr)
            throws QuorumPeerConfig.ConfigException {
        //LOG.warn("ClientParts: " + serverClientParts[1]);
        final String clientParts[] = splitWithLeadingHostname(hostStr);
        if (clientParts.length > 2) {
            throw new QuorumPeerConfig.ConfigException(serverStr + wrongFormat);
        }

        // is client_config a host:port or just a port
        final String hostname = (clientParts.length == 2) ?
                clientParts[0] : "0.0.0.0";
        try {
            return new InetSocketAddress(hostname,
                    Integer.parseInt(clientParts[clientParts.length - 1]));
            //LOG.warn("Set clientAddr to " + clientAddr);
        } catch (NumberFormatException e) {
            throw new QuorumPeerConfig.ConfigException("Address unresolved: " + hostname +
                    ":" + clientParts[clientParts.length - 1]);
        }
    }

    private void parseHostString(final String hostStr,
                                 final String serverStr)
            throws QuorumPeerConfig.ConfigException {
        final String[] parts = hostStr.split(":");
        if (parts[0].toLowerCase().equals("plain")) {
            clientAddr = parseCompatibleHostString(
                    removeLeadingItem(parts), serverStr);
        } else if (parts[0].toLowerCase().equals("secure")) {
            secureClientAddr = parseCompatibleHostString(
                    removeLeadingItem(parts), serverStr);
        } else {
            clientAddr  = parseCompatibleHostString(hostStr, serverStr);
        }
    }

    private String removeLeadingItem(final String[] parts) {
        final List<String> partsList =
                new ArrayList<>(Arrays.asList(parts));
        partsList.remove(0);
        return String.join(":", partsList);
    }

    private static String delimitedHostString(InetSocketAddress addr)
    {
        String host = addr.getHostString();
        if (host.contains(":")) {
            return "[" + host + "]";
        } else {
            return host;
        }
    }

    public String toString(){
        StringWriter sw = new StringWriter();
        //addr should never be null, but just to make sure
        if (addr !=null) {
            sw.append(delimitedHostString(addr));
            sw.append(":");
            sw.append(String.valueOf(addr.getPort()));
        }
        if (electionAddr!=null){
            sw.append(":");
            sw.append(String.valueOf(electionAddr.getPort()));
        }
        if (type == QuorumPeer.LearnerType.OBSERVER) sw.append(":observer");
        else if (type == QuorumPeer.LearnerType.PARTICIPANT) sw.append(":participant");
        if (hasCertFp()) {
            sw.append(":cert:");
            sw.append(sslCertCfg.getCertFingerPrintStr());
        }
        if (clientAddr!=null){
            sw.append(";");
            if (secureClientAddr != null) {
                sw.append("plain:");
            }
            sw.append(delimitedHostString(clientAddr));
            sw.append(":");
            sw.append(String.valueOf(clientAddr.getPort()));
        }
        if (secureClientAddr != null) {
            sw.append(";secure:");
            sw.append(delimitedHostString(secureClientAddr));
            sw.append(":");
            sw.append(String.valueOf(secureClientAddr.getPort()));
        }
        return sw.toString();
    }

    public boolean hasCertFp() {
        return sslCertCfg != null && sslCertCfg.getCertFingerPrintStr()
                != null;
    }

    public int hashCode() {
        assert false : "hashCode not designed";
        return 42; // any arbitrary constant will do
    }

    private boolean checkAddressesEqual(InetSocketAddress addr1, InetSocketAddress addr2){
        if ((addr1 == null && addr2!=null) ||
                (addr1!=null && addr2==null) ||
                (addr1!=null && addr2!=null && !addr1.equals(addr2))) return false;
        return true;
    }

    public boolean equals(Object o){
        if (!(o instanceof QuorumServer)) return false;
        QuorumServer qs = (QuorumServer)o;
        if ((qs.id != id) || (qs.type != type)) return false;
        if (!checkAddressesEqual(addr, qs.addr)) return false;
        if (!checkAddressesEqual(electionAddr, qs.electionAddr)) return false;
        if (!checkAddressesEqual(clientAddr, qs.clientAddr)) return false;
        return true;
    }

    public void checkAddressDuplicate(QuorumServer s) throws KeeperException.BadArgumentsException {
        List<InetSocketAddress> otherAddrs = new ArrayList<InetSocketAddress>();
        otherAddrs.add(s.addr);
        otherAddrs.add(s.clientAddr);
        otherAddrs.add(s.electionAddr);
        otherAddrs = excludedSpecialAddresses(otherAddrs);

        for (InetSocketAddress my: this.myAddrs) {

            for (InetSocketAddress other: otherAddrs) {
                if (my.equals(other)) {
                    String error = String.format("%s of server.%d conflicts %s of server.%d", my, this.id, other, s.id);
                    throw new KeeperException.BadArgumentsException(error);
                }
            }
        }
    }

    private List<InetSocketAddress> excludedSpecialAddresses(List<InetSocketAddress> addrs) {
        List<InetSocketAddress> included = new ArrayList<InetSocketAddress>();
        InetAddress wcAddr = new InetSocketAddress(0).getAddress();

        for (InetSocketAddress addr : addrs) {
            if (addr == null) {
                continue;
            }
            InetAddress inetaddr = addr.getAddress();

            if (inetaddr == null ||
                    inetaddr.equals(wcAddr) || // wildCard address(0.0.0.0)
                    inetaddr.isLoopbackAddress()) { // loopback address(localhost/127.0.0.1)
                continue;
            }
            included.add(addr);
        }
        return included;
    }

    @Override
    public InetSocketAddress getElectionAddr() {
        return electionAddr;
    }

    @Override
    public String getCertFingerPrintStr() {
        return sslCertCfg == null ? null : sslCertCfg.getCertFingerPrintStr();
    }
}