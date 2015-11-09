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
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.exception.SchedulerException;
import org.apache.kylin.job.impl.threadpool.DefaultScheduler;
import org.apache.kylin.job.lock.MockJobLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class DefaultJobEngineStateModelV1 extends AbstractJobEngineStateModelV1 {

    private static final Logger logger = LoggerFactory.getLogger(DefaultJobEngineStateModelV1.class);
    private KylinConfig kylinConfig;


    public DefaultJobEngineStateModelV1(String instanceName, KylinConfig kylinConfig) {
        super(instanceName);
        this.kylinConfig = kylinConfig;
    }

    @Override
    public void onBecomeLeaderFromStandby(Message message, NotificationContext context) {
        logger.info(getInstanceName() + " onBecomeLeaderFromStandby");
        try {
            DefaultScheduler scheduler = DefaultScheduler.createInstance();
            scheduler.init(new JobEngineConfig(kylinConfig), new MockJobLock());
            while (!scheduler.hasStarted()) {
                logger.error("scheduler has not been started");
                Thread.sleep(1000);
            }
        } catch (Exception e) {
            logger.error("error start DefaultScheduler", e);
            throw new RuntimeException(e);
        }

    }

    @Override
    public void onBecomeStandbyFromLeader(Message message, NotificationContext context) {
        logger.info(getInstanceName() + " onBecomeStandbyFromLeader");
        DefaultScheduler.destroyInstance();
    }

    @Override
    public void onBecomeOfflineFromStandby(Message message, NotificationContext context) {
        logger.info(getInstanceName() + " onBecomeOfflineFromStandby");
    }

    @Override
    public void onBecomeStandbyFromOffline(Message message, NotificationContext context) {
        logger.info(getInstanceName() + " onBecomeStandbyFromOffline");
    }

}
