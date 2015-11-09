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
package org.apache.kylin.rest.helix;

import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.rest.helix.v1.DefaultStateModelFactory;
import org.apache.kylin.rest.helix.v1.JobEngineSMDV1;

import static org.apache.kylin.rest.helix.JobControllerConstants.CLUSTER_NAME;

/**
 */
public class JobControllerConnector {

    private final String instanceName;
    private final HelixAdmin admin;
    private final StateModelFactory<StateModel> stateModelFactory;
    private final String zkAddress;
    private final String hostname;
    private final String port;
    
    private HelixManager manager;

    public JobControllerConnector(String hostname, String port, String zkAddress, StateModelFactory<StateModel> stateModelFactory) {
        this.instanceName = hostname + "_" + port;
        this.hostname = hostname;
        this.port = port;
        this.zkAddress = zkAddress;
        this.admin = new ZKHelixAdmin(zkAddress);
        this.stateModelFactory = stateModelFactory;
    }
    
    public void register() {
        if (!admin.getInstancesInCluster(CLUSTER_NAME).contains(instanceName)) {
            InstanceConfig instanceConfig = new InstanceConfig(instanceName);
            instanceConfig.setHostName(hostname);
            instanceConfig.setPort(port);
            admin.addInstance(CLUSTER_NAME, instanceConfig);
        }
    }


    public void start() throws Exception {
        this.manager = HelixManagerFactory.getZKHelixManager(CLUSTER_NAME, instanceName,
                InstanceType.PARTICIPANT, zkAddress);
        StateMachineEngine stateMach = manager.getStateMachineEngine();
        stateMach.registerStateModelFactory(JobEngineSMDV1.STATE_MODEL_NAME, stateModelFactory);
        manager.connect();
    }

    public void stop() {
        if (manager != null) {
            manager.disconnect();
        }
    }
}
