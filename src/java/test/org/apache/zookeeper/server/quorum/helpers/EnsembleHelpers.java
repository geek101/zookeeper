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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Stack;

import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.server.quorum.Vote;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.slf4j.Logger;

public class EnsembleHelpers {
    public static Collection<Collection<Collection<
            ImmutablePair<Long, QuorumPeer.ServerState>>>>
    quorumMajorityWithLeaderServerStateCombinations(int quorumSize) {
        final Collection<Collection<Long>> quorumMajCombs =
                quorumMajorityCombinations(quorumSize);
        final Collection<QuorumPeer.ServerState> serverStates =
                new ArrayList<>(Arrays.asList(
                        QuorumPeer.ServerState.LEADING,
                        QuorumPeer.ServerState.FOLLOWING,
                        QuorumPeer.ServerState.LOOKING));
        return quorumMajorityServerStateCombinationsHelper(quorumSize,
                quorumMajCombs, serverStates,
                ImmutableTriple.of(1, quorumSize-1, quorumSize));
    }

    public static Collection<Collection<Collection<
            ImmutablePair<Long, QuorumPeer.ServerState>>>>
    quorumMajorityServerStateCombinationsHelper(
            final int quorumSize,
            Collection<Collection<Long>> quorumMajCombs,
            Collection<QuorumPeer.ServerState> serverStates,
            final ImmutableTriple<Integer, Integer, Integer> triple) {
        Collection<Collection<Collection<
                ImmutablePair<Long, QuorumPeer.ServerState>>>> result
                = new ArrayList<>();
        for (final Collection<Long> quorum : quorumMajCombs) {
            Collection<Collection<ImmutablePair<Long, QuorumPeer.ServerState>>>
                    resultForQuorum = new ArrayList<>();
            Stack<ImmutablePair<Long, QuorumPeer.ServerState>> set =
                    new Stack<>();
            Long[] k = new Long[quorum.size()];
            quorum.toArray(k);
            QuorumPeer.ServerState[] ss = new QuorumPeer
                    .ServerState[serverStates.size()];
            serverStates.toArray(ss);
            getQuorumMajorityServerStateCombinations(quorumSize, k, 0, ss,
                    triple, set, resultForQuorum);
            result.add(resultForQuorum);
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    public static void getQuorumMajorityServerStateCombinations(
            final int quorumSize,
            final Long[] quorum,
            final int quorumIdx,
            final QuorumPeer.ServerState[] serverStates,
            final ImmutableTriple<Integer, Integer, Integer> triple,
            Stack<ImmutablePair<Long, QuorumPeer.ServerState>> set,
            final Collection<Collection<
                    ImmutablePair<Long, QuorumPeer.ServerState>>> result) {
        if (set.size() == quorum.length) {
            if (triple.getLeft() == 0 && triple.getMiddle() <= quorumSize/2) {
                result.add((Stack<ImmutablePair<Long, QuorumPeer.ServerState>>)
                        set.clone());
            }
            return;
        }

        for (int k = quorumIdx; k < quorum.length; k++) {
            final Long sid = quorum[k];
            if (triple.getLeft() > 0) {
                set.push(ImmutablePair.of(sid, serverStates[0]));
                getQuorumMajorityServerStateCombinations(quorumSize, quorum,
                        k + 1, serverStates, ImmutableTriple.of(
                                triple.getLeft() - 1,
                                triple.getMiddle(),
                                triple.getRight()), set, result);
                set.pop();
            }

            if (triple.getMiddle() > 0) {
                set.push(ImmutablePair.of(sid, serverStates[1]));
                getQuorumMajorityServerStateCombinations(quorumSize, quorum,
                        k + 1, serverStates, ImmutableTriple.of(
                                triple.getLeft(),
                                triple.getMiddle() - 1,
                                triple.getRight()), set, result);
                set.pop();
            }

            if (triple.getRight() > 0) {
                set.push(ImmutablePair.of(sid, serverStates[2]));
                getQuorumMajorityServerStateCombinations(quorumSize, quorum,
                        k + 1, serverStates, ImmutableTriple.of(
                                triple.getLeft(),
                                triple.getMiddle(),
                                triple.getRight() - 1), set, result);
                set.pop();
            }
        }
    }

    public static Collection<Collection<Long>> quorumMajorityCombinations(
            int quorumSize) {
        final Collection<Collection<Long>> retCombs = new ArrayList<>();
        for (int i = (quorumSize/2 + 1); i <= quorumSize; i++) {
            EnsembleHelpers.quorumCombinations(0, i, quorumSize,
                    new ArrayList<Long>(), retCombs);
        }
        return retCombs;
    }

    public static Collection<Collection<Long>> quorumCombinationsHelper(
            int size, int len) {
        final Collection<Collection<Long>> retCombs = new ArrayList<>();
        EnsembleHelpers.quorumCombinations(0, size, len,
                new ArrayList<Long>(), retCombs);
        return retCombs;
    }

    public static void quorumCombinations(final int k, final int size,
                                   final int len, final Collection<Long> set,
                                   Collection<Collection<Long>> result) {
        if (k == len || set.size() == size) {
            if (set.size() == size) {
                result.add(set);
            }
            return;
        }

        Collection<Long> incSet = new ArrayList<>(set);
        incSet.add((long)k+1);
        quorumCombinations(k+1, size, len, incSet, result);
        quorumCombinations(k+1, size, len, new ArrayList<>(set), result);
    }

    public static Collection<Collection<Long>> powerSetHelper(int size) {
        final Collection<Collection<Long>> retSet = new ArrayList<>();
        powerSet(0, size, new boolean[size], retSet);
        return retSet;
    }

    public static void powerSet(final int k, final int size,
                         final boolean[] vec,
                         Collection<Collection<Long>> result) {
        if (k == size) {
            Collection<Long> s = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                if (vec[i]) {
                    s.add((long)i+1L);
                }
            }
            result.add(s);
            return;
        }

        vec[k] = true;
        powerSet(k+1, size, vec, result);
        vec[k] = false;
        powerSet(k+1, size, vec, result);
    }

    public static void printQuorumMajorityServerStateCombinations(
            int quorumSize, final Logger log) {
        final Collection<Collection<Collection<
                ImmutablePair<Long, QuorumPeer.ServerState>>>> result =
                EnsembleHelpers.quorumMajorityWithLeaderServerStateCombinations
                        (quorumSize);
        int count = 1;
        for (final Collection<Collection<
                ImmutablePair<Long, QuorumPeer.ServerState>>> t : result) {
            log.info("Set[" + quorumSize + "] - " + count++ + " : "
                    + getQuorumCombServerStateStr(t));
        }
    }

    public static String getQuorumCombServerStateStr(
            final Collection<Collection<
            ImmutablePair<Long, QuorumPeer.ServerState>>> combWithState) {
        String str = "{";
        Iterator<Collection<
                ImmutablePair<Long, QuorumPeer.ServerState>>> it =
                combWithState.iterator();
        if (it.hasNext()) {
            str += getQuorumServerStateCollectionStr(it.next());
        } else {
            str += "}";
            return str;
        }

        while(it.hasNext()) {
            str += ", " + getQuorumServerStateCollectionStr(it.next());
        }
        return str + "}";
    }

    public static String getQuorumServerStateCollectionStr(
            final Collection<ImmutablePair<Long, QuorumPeer.ServerState>>
                    quorumServerStates) {
        String str = "{";
        Iterator<ImmutablePair<Long, QuorumPeer.ServerState>> it =
                quorumServerStates.iterator();
        if (it.hasNext()) {
            str += getSidWithServerStateStr(it.next());
        } else {
            str += "}";
            return str;
        }

        while(it.hasNext()) {
            str += "," + getSidWithServerStateStr(it.next());
        }
        return str + "}";
    }

    public static String getSidWithServerStateStr(
            final ImmutablePair<Long, QuorumPeer.ServerState> quorumServer) {
        return quorumServer.getLeft().toString() +
                getServerStateStr(quorumServer.getRight());
    }

    public static String getServerStateStr(final QuorumPeer.ServerState
                                            serverState) {
        return Vote.getServerStateStr(serverState);
    }

    public static QuorumPeer.ServerState getServerState(final char c) {
        final String str = "" + c;
        switch(str.toUpperCase()) {
            case "K":
                return QuorumPeer.ServerState.LOOKING;
            case "F":
                return QuorumPeer.ServerState.FOLLOWING;
            case "L":
                return QuorumPeer.ServerState.LEADING;
            case "O":
                return QuorumPeer.ServerState.OBSERVING;
            default:
                throw new IllegalArgumentException("unknown state: " + c);
        }
    }

    public static void printQuorumMajorityCombinations(
            int quorumSize, final Logger log) {
        log.info("QuorumMajority(" + quorumSize + ") = "
                + printHelper(EnsembleHelpers.quorumMajorityCombinations
                (quorumSize)));
    }

    public static  void printPowerSet(int size, final Logger log) {
        log.info("P(" + size + ") = "
                + printHelper(EnsembleHelpers.powerSetHelper(size)));
    }

    public static void printCombinations(int size, int len, final Logger log) {
        log.info("" + len + "C" + size + " = "
                + printHelper(EnsembleHelpers.quorumCombinationsHelper(
                size, len)));
    }

    public static String printHelper(final Collection<Collection<Long>> sets) {
        String str = "";
        final Iterator<Collection<Long>> iter = sets.iterator();
        while(iter.hasNext()) {
            final Iterator<Long> it = iter.next().iterator();
            str += "{";
            if (it.hasNext()) {
                str += it.next();
            }
            while(it.hasNext()) {
                str += "," + it.next();
            }
            str += "}";
            if (iter.hasNext()) {
                str += ", ";
            }
        }
        return str;
    }
}
