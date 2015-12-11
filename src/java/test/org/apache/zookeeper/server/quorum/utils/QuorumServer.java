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

package org.apache.zookeeper.server.quorum.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.regex.PatternSyntaxException;

/**
 * Class for a single server which is part of the Quorum.
 */
public class QuorumServer implements AbstractServer {
    private static final Logger LOG =
            LoggerFactory.getLogger(QuorumServer.class);
    private long id;                        /// A unique id for this server.
    private InetSocketAddress addr = null;  /// IP and Port for this server.

    /**
     * addressStr example: "localhost:2777"
     *
     * @param id
     * @param addressStr
     * @throws PatternSyntaxException
     */
    public QuorumServer(long id, String addressStr)
            throws PatternSyntaxException, UnknownHostException {
        String[] parts =  null;
        try {
            parts = addressStr.split(":", 2);
        } catch (PatternSyntaxException e) {
            StringBuilder sb = new StringBuilder();
            sb.append("addressStr[" )
                    .append(addressStr)
                    .append("] parse err: ")
                    .append(e);
            LOG.error(sb.toString());
            throw e;
        }

        InetAddress address = null;
        try {
            address = InetAddress.getByName(parts[0]);
        } catch (UnknownHostException exp) {
            StringBuilder sb = new StringBuilder();
            sb.append("Failed to resolve address:")
                    .append(parts[0])
                    .append(" exp:")
                    .append(exp);
            LOG.warn(sb.toString());
            throw exp;
        }

        int port = -1;
        try {
            port = Integer.parseInt(parts[1]);
        } catch (NumberFormatException exp) {
            StringBuilder sb = new StringBuilder();
            sb.append("Failed to parse port:")
                    .append(parts[1])
                    .append(" exp:")
                    .append(exp);
            LOG.warn(sb.toString());
            throw exp;
        }

        if (port > (Short.MAX_VALUE - 1)) {
            StringBuilder sb = new StringBuilder();
            sb.append("Invalid parsed port: ")
                    .append(port);
            LOG.warn(sb.toString());
            throw new NumberFormatException(sb.toString());
        }

        InetSocketAddress sockAddr = null;
        try {
            sockAddr = new InetSocketAddress(address, port);
        } catch (IllegalArgumentException exp) {
            StringBuilder sb = new StringBuilder();
            sb.append("Error InetSocketAddress exp: ")
                    .append(exp);
            LOG.warn(sb.toString());
            throw exp;
        }

        this.id = id;
        this.addr = sockAddr;
    }

    /**
     * Set id and address of the server.
     * @param id
     * @param addr
     */
    public QuorumServer(long id, InetSocketAddress addr) {
        this.id = id;
        this.addr = addr;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("server.")
                .append(id)
                .append("=")
                .append(addr.toString());
        return sb.toString();
    }

    final public InetSocketAddress addr() {
        return addr;
    }

    public long id() {
        return id;
    }

    final public int hashCode() { return (int)id; }
}
