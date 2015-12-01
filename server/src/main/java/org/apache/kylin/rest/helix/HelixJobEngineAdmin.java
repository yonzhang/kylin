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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.helix.ExternalViewChangeListener;
import org.apache.helix.HelixManager;
import org.apache.helix.controller.HelixControllerMain;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.StateModelDefinition;
import org.apache.kylin.rest.helix.v1.JobEngineSMDV1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentMap;


/**
 */
public class HelixJobEngineAdmin {

    public static final String RESOURCE_NAME = "job_engine_v1";
    
    
    private static ConcurrentMap<String, HelixJobEngineAdmin> instanceMaps = Maps.newConcurrentMap();
    private static ConcurrentMap<String, HelixManager> controllerMaps = Maps.newConcurrentMap();
    
    public static HelixJobEngineAdmin getInstance(String zkAddress) {
        Preconditions.checkNotNull(zkAddress);
        instanceMaps.putIfAbsent(zkAddress, new HelixJobEngineAdmin(zkAddress));
        return instanceMaps.get(zkAddress);
    }

    public static String getCurrentInstanceName() {
        final String restAddress = System.getProperty("kylin.rest.address");
        if (StringUtils.isEmpty(restAddress)) {
            throw new RuntimeException("There is no -Dkylin.rest.address set; Please check bin/kylin.sh");
        }

        final String hostname = Preconditions.checkNotNull(restAddress.substring(0, restAddress.lastIndexOf(":")));
        final String port = Preconditions.checkNotNull(restAddress.substring(restAddress.lastIndexOf(":") + 1));
        return hostname + "_" + port;
    }

    private static final Logger logger = LoggerFactory.getLogger(HelixJobEngineAdmin.class);
    private final String zkAddress;
    private final ZKHelixAdmin admin;

    private HelixJobEngineAdmin(String zkAddress) {
        this.zkAddress = zkAddress;
        this.admin = new ZKHelixAdmin(zkAddress);
    }
    public void initV1(String clusterName, String resourceName) {
        final StateModelDefinition jobEngineSMDV1 = JobEngineSMDV1.getJobEngineStateModelDefinitionV1();
        admin.addCluster(clusterName, false);
        if (admin.getStateModelDef(clusterName, jobEngineSMDV1.getId()) == null) {
            admin.addStateModelDef(clusterName, jobEngineSMDV1.getId(), jobEngineSMDV1);
        }
        if (!admin.getResourcesInCluster(clusterName).contains(resourceName)) {
            admin.addResource(clusterName, resourceName, 1, jobEngineSMDV1.getId(), "AUTO");
        }

    }

    public void startController(String clusterName, ExternalViewChangeListener externalViewChangeListener) throws Exception {
        HelixManager manager = HelixControllerMain.startHelixController(zkAddress, clusterName, getCurrentInstanceName(),
                HelixControllerMain.STANDALONE);
        controllerMaps.put(clusterName, manager);
        manager.connect();
        if (externalViewChangeListener != null) {
            manager.addExternalViewChangeListener(externalViewChangeListener);
        }
    }
    
    public void stopController(String clusterName) {
        if (controllerMaps.containsKey(clusterName)) {
            controllerMaps.remove(clusterName).disconnect();
        } else {
            throw new IllegalArgumentException("Didn't find HelixManager with clusterName '" + clusterName + "'.");
        }
    }

    public String getInstanceState(String clusterName, String resourceName, String instanceName) {
        final ExternalView resourceExternalView = admin.getResourceExternalView(clusterName, resourceName);
        if (resourceExternalView == null) {
            logger.warn("fail to get ExternalView, clusterName:" + clusterName + " resourceName:" + resourceName);
            return "ERROR";
        }
        final Set<String> partitionSet = resourceExternalView.getPartitionSet();
        Preconditions.checkArgument(partitionSet.size() == 1);
        final Map<String, String> stateMap = resourceExternalView.getStateMap(partitionSet.iterator().next());
        if (stateMap.containsKey(instanceName)) {
            return stateMap.get(instanceName); 
        } else {
            logger.warn("fail to get state, clusterName:" + clusterName + " resourceName:" + resourceName + " instance:" + instanceName);
            return "ERROR";
        }
    }


    public String collectStateInfo(String msg, String clusterName, String resourceName) {
        StringBuilder sb = new StringBuilder("");
        sb.append("CLUSTER STATE: ").append(msg).append("\n");
        ExternalView resourceExternalView = admin.getResourceExternalView(clusterName, resourceName);
        if (resourceExternalView == null) {
            sb.append("no participant joined yet").append("\n");
            return sb.toString();
        }
        final List<String> instancesInCluster = admin.getInstancesInCluster(clusterName);
        TreeSet<String> sortedSet = new TreeSet<String>(resourceExternalView.getPartitionSet());
        sb.append("\t\t");
        for (String instance : instancesInCluster) {
            sb.append(instance).append("\t");
        }
        sb.append("\n");
        for (String partitionName : sortedSet) {
            sb.append(partitionName).append("\t");
            for (String instance : instancesInCluster) {
                Map<String, String> stateMap = resourceExternalView.getStateMap(partitionName);
                if (stateMap != null && stateMap.containsKey(instance)) {
                    sb.append(stateMap.get(instance)).append(
                            "\t\t");
                } else {
                    sb.append("-").append("\t\t");
                }
            }
            sb.append("\n");
        }
        sb.append("###################################################################").append("\n");
        return sb.toString();
    }

    public void rebalance(String clusterName, String resourceName) {
        final List<String> instancesInCluster = admin.getInstancesInCluster(clusterName);
        admin.rebalance(clusterName, resourceName, instancesInCluster.size(), instancesInCluster);
        logger.info("cluster:" + clusterName + " ideal state:" + admin.getResourceIdealState(clusterName, resourceName));
        logger.info("cluster:" + clusterName + " instances:" + instancesInCluster);
    }
    
    public boolean isLeaderRole(String clusterName, String instanceName) {
        final String instanceState = 
                getInstanceState(clusterName,
                        HelixJobEngineAdmin.RESOURCE_NAME,
                        instanceName);
        logger.debug("instance state: " + instanceState);
        if (JobEngineSMDV1.States.LEADER.toString().equalsIgnoreCase(instanceState)) {
            return true;
        }
        
        return false;
    }
}
