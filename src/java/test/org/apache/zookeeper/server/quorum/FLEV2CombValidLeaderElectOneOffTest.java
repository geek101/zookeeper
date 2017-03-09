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
package org.apache.zookeeper.server.quorum;


import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.server.quorum.helpers.Ensemble;
import org.apache.zookeeper.server.quorum.helpers.FLEV2BaseTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class FLEV2CombValidLeaderElectOneOffTest extends FLEV2BaseTest {
    @Parameterized.Parameters
    public static Collection quorumTypeAndSize() {
        return Arrays.asList( new Object[][] {
                //{ "mock", 1, 0, 0, 0, 0, TimeUnit.MILLISECONDS },
                { "mockbcast", 50, 0, 0, 0, 0, TimeUnit.MILLISECONDS},
                { "quorumbcast", 150, 150, 250, 50, 3, TimeUnit.MILLISECONDS},
                { "quorumbcast-ssl", 350, 350, 550, 50, 7,
                        TimeUnit.MILLISECONDS},
        });
    }

    public FLEV2CombValidLeaderElectOneOffTest(final String ensembleType,
                                               final int stableTimeoutMsec,
                                               final int readTimeoutMsec,
                                               final int connectTimeoutMsec,
                                               final int keepAliveTimeoutMsec,
                                               final int keepAliveCount,
                                               final TimeUnit timeoutUnit) {
        super(ensembleType, stableTimeoutMsec, readTimeoutMsec,
                connectTimeoutMsec, keepAliveTimeoutMsec, keepAliveCount,
                timeoutUnit);
    }

    @Test
    public void testLeaderForFiveLeaderGoesToLooking()
            throws ElectionException, InterruptedException, ExecutionException {
        final Ensemble ensemble = createEnsemble(1L, 5);
        final Ensemble parentEnsemble
                = ensemble.configure("{1F,2F,3K,4K,5L}");
        final Ensemble movedEnsemble
                = parentEnsemble.moveToLooking(5);
        final Ensemble doneEnsemble
                = movedEnsemble.runLooking();

        doneEnsemble.verifyLeaderAfterShutDown();
        verifyPrintHelper(parentEnsemble, movedEnsemble, doneEnsemble);
    }

    @Test
    public void testLookingJoinExistingEnsemble()
            throws ElectionException, InterruptedException, ExecutionException {
        final Ensemble ensemble = createEnsemble(1L, 5);
        final Ensemble parentEnsemble
                = ensemble.configure("{1k, 2L, 3F, 4F, 5K}");
        final Ensemble movedEnsemble
                = parentEnsemble.moveToLooking(1);
        final Ensemble doneEnsemble
                = movedEnsemble.runLooking();

        doneEnsemble.verifyLeaderAfterShutDown();
        verifyPrintHelper(parentEnsemble, movedEnsemble, doneEnsemble);
    }

    @Test
    public void testPartitionEnsemble() throws ElectionException,
            InterruptedException, ExecutionException {
        final Ensemble ensemble = createEnsemble(1L, 5);
        final Ensemble parentEnsemble
                = ensemble.configure("{{1k, 2K, 5L}, {3F, 4F, 5L}}");
        final Ensemble movedEnsemble
                = parentEnsemble.moveToLooking(3);
        final Ensemble doneEnsemble
                = movedEnsemble.runLooking();

        doneEnsemble.verifyLeaderAfterShutDown();
        verifyPrintHelper(parentEnsemble, movedEnsemble, doneEnsemble);
    }

    @Test
    public void testPartitionNonTerminatingEnsemble() throws ElectionException,
            InterruptedException, ExecutionException {
        final Ensemble ensemble = createEnsemble(1L, 5);
        final Ensemble parentEnsemble
                = ensemble.configure("{{1k, 2K}, {3K, 4K}, {2K, 5K}}");
        final Ensemble movedEnsemble
                = parentEnsemble.moveToLooking(3);
        final Ensemble doneEnsemble
                = movedEnsemble.runLooking();

        doneEnsemble.verifyLeaderAfterShutDown();
        verifyPrintHelper(parentEnsemble, movedEnsemble, doneEnsemble);
    }

    @Test
    public void testPartitionJoinLeader() throws ElectionException,
            InterruptedException, ExecutionException {
        final Ensemble ensemble = createEnsemble(1L, 5);
        final Ensemble parentEnsemble
                = ensemble.configure("{{1k, 2K, 5L}, {3F, 4F, 5L}}");
        final Ensemble movedEnsemble
                = parentEnsemble.moveToLooking(2);
        final Ensemble doneEnsemble
                = movedEnsemble.runLooking();

        doneEnsemble.verifyLeaderAfterShutDown();
        verifyPrintHelper(parentEnsemble, movedEnsemble, doneEnsemble);
    }

    @Test
    public void testPartitionAllLooking() throws ElectionException,
            InterruptedException, ExecutionException {
        final Ensemble ensemble = createEnsemble(1L, 5);
        final Ensemble parentEnsemble
                = ensemble.configure("{{1k, 2K}, {1K, 3K}, {1K, 4K}, {5K}}");
        final Ensemble movedEnsemble
                = parentEnsemble.moveToLooking(1);
        final Ensemble doneEnsemble
                = movedEnsemble.runLooking();

        doneEnsemble.verifyLeaderAfterShutDown();
        verifyPrintHelper(parentEnsemble, movedEnsemble, doneEnsemble);
    }
}
