/*
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
package org.apache.kylin.rest.helix.v1;

import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;

import java.util.concurrent.atomic.AtomicInteger;

/**
 */
public class TestJobEngineStateModelV1 extends AbstractJobEngineStateModelV1 {
    
    private static AtomicInteger offlineCount = new AtomicInteger(0);
    private static AtomicInteger standbyCount = new AtomicInteger(0);
    private static AtomicInteger leaderCount = new AtomicInteger(0);
    
    public TestJobEngineStateModelV1(String instanceName) {
        super(instanceName);
        offlineCount.incrementAndGet();
    }

    @Override
    public void onBecomeLeaderFromStandby(Message message, NotificationContext context) {
        leaderCount.incrementAndGet();
        standbyCount.decrementAndGet();
    }

    @Override
    public void onBecomeStandbyFromLeader(Message message, NotificationContext context) {
        standbyCount.incrementAndGet();
        leaderCount.decrementAndGet();
    }

    @Override
    public void onBecomeOfflineFromStandby(Message message, NotificationContext context) {
        offlineCount.incrementAndGet();
        standbyCount.decrementAndGet();
    }

    @Override
    public void onBecomeStandbyFromOffline(Message message, NotificationContext context) {
        standbyCount.incrementAndGet();
        offlineCount.decrementAndGet();
    }
    
    public static AtomicInteger getStandbyCount() {
        return standbyCount;
    }

    public static AtomicInteger getOfflineCount() {
        return offlineCount;
    }

    public static AtomicInteger getLeaderCount() {
        return leaderCount;
    }

    @Override
    public void onStartScheduler() {

    }

    @Override
    public void onStopScheduler() {

    }
}
