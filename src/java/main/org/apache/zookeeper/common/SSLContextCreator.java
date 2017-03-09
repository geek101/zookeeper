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

import java.net.InetSocketAddress;

import javax.net.ssl.SSLContext;

/**
 * Helper to abstract away QuorumPeer and gory details of X509Util
 * from both client and server side code. Currently targeted for
 * Quorum Netty SSL implementation
 */
public interface SSLContextCreator {
    SSLContext forClient(final InetSocketAddress peerAddr,
                         final String peerCertFingerPrintStr)
            throws X509Exception.SSLContextException;
    SSLContext forServer()  throws X509Exception.SSLContextException;
}
