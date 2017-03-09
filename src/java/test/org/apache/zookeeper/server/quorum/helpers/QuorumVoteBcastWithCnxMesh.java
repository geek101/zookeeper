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
package org.apache.zookeeper.server.quorum.helpers;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

import org.apache.zookeeper.server.quorum.QuorumServer;
import org.apache.zookeeper.server.quorum.netty.QuorumVoteBroadcast;
import org.apache.zookeeper.server.quorum.util.ChannelException;
import io.netty.channel.EventLoopGroup;

public class QuorumVoteBcastWithCnxMesh extends QuorumVoteBroadcast {
    private final QuorumCnxMesh quorumCnxMesh;

    public QuorumVoteBcastWithCnxMesh(
            Long myId, List<QuorumServer> quorumServerList,
            InetSocketAddress electionAddr,
            EventLoopGroup eventLoopGroup,
            long readMsgTimeoutMs, long connectTimeoutMs,
            long keepAliveTimeoutMs, int keepAliveCount,
            boolean sslEnabled, final QuorumCnxMesh quorumCnxMesh)
            throws ChannelException, IOException {
        super(myId, quorumServerList, electionAddr, eventLoopGroup,
                readMsgTimeoutMs, connectTimeoutMs, keepAliveTimeoutMs,
                keepAliveCount, sslEnabled);
        this.quorumCnxMesh = quorumCnxMesh;
    }

    public boolean isConnected(final long sid) {
        return quorumCnxMesh.connected(this.sid(), sid);
    }
}
