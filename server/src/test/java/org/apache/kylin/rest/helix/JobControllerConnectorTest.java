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

import com.google.common.collect.Lists;
import org.I0Itec.zkclient.IDefaultNameSpace;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkServer;
import org.apache.hadoop.fs.FileUtil;
import org.apache.helix.manager.zk.ZKHelixAdmin;

import static org.junit.Assert.assertEquals;

import org.apache.kylin.rest.helix.v1.TestJobEngineStateModelV1;
import org.apache.kylin.rest.helix.v1.TestStateModelFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.List;

/**
 */
public class JobControllerConnectorTest {


    String zkAddress = "localhost:2199";
    ZkServer server;

    List<JobControllerConnector> connectors ;
    HelixJobEngineAdmin helixJobEngineAdmin;
    
    private static final String CLUSTER_NAME = "test_cluster";


    @Before
    public void setup() throws Exception {
        System.setProperty("kylin.rest.address", "localhost:7070");
        // start zookeeper on localhost
        final File tmpDir = new File("/tmp/helix-quickstart");
        FileUtil.fullyDelete(tmpDir);
        tmpDir.mkdirs();
        server = new ZkServer("/tmp/helix-quickstart/dataDir", "/tmp/helix-quickstart/logDir",
                new IDefaultNameSpace() {
                    @Override
                    public void createDefaultNameSpace(ZkClient zkClient) {
                    }
                }, 2199);
        server.start();
        
        final ZKHelixAdmin zkHelixAdmin = new ZKHelixAdmin(zkAddress);
        zkHelixAdmin.dropCluster(CLUSTER_NAME);
        connectors = Lists.newArrayList();
        helixJobEngineAdmin = HelixJobEngineAdmin.getInstance(zkAddress);
        helixJobEngineAdmin.initV1(CLUSTER_NAME, HelixJobEngineAdmin.RESOURCE_NAME);
        helixJobEngineAdmin.startController(CLUSTER_NAME, null);

    }

    @Test
    public void test() throws Exception {
        JobControllerConnector connector = new JobControllerConnector("localhost", "10000", zkAddress, CLUSTER_NAME, new TestStateModelFactory("localhost", "10000"));
        connector.register();
        helixJobEngineAdmin.rebalance(CLUSTER_NAME, HelixJobEngineAdmin.RESOURCE_NAME);
        connector.start();
        connectors.add(connector);
        Thread.sleep(1000);
        System.out.println(helixJobEngineAdmin.collectStateInfo("add 1 nodes", CLUSTER_NAME, HelixJobEngineAdmin.RESOURCE_NAME));
        assertEquals(1, TestJobEngineStateModelV1.getLeaderCount().get());
        assertEquals(0, TestJobEngineStateModelV1.getStandbyCount().get());
        assertEquals(0, TestJobEngineStateModelV1.getOfflineCount().get());

        connector = new JobControllerConnector("localhost", "10001", zkAddress, CLUSTER_NAME, new TestStateModelFactory("localhost", "10001"));
        connector.register();
        helixJobEngineAdmin.rebalance(CLUSTER_NAME, HelixJobEngineAdmin.RESOURCE_NAME);
        connector.start();
        connectors.add(connector);
        Thread.sleep(1000);
        System.out.println(helixJobEngineAdmin.collectStateInfo("add 2 nodes", CLUSTER_NAME, HelixJobEngineAdmin.RESOURCE_NAME));
        assertEquals(1, TestJobEngineStateModelV1.getLeaderCount().get());
        assertEquals(1, TestJobEngineStateModelV1.getStandbyCount().get());
        assertEquals(0, TestJobEngineStateModelV1.getOfflineCount().get());

        
        connectors.remove(0).stop();
        TestJobEngineStateModelV1.getLeaderCount().decrementAndGet();
        Thread.sleep(1000);
        assertEquals(1, TestJobEngineStateModelV1.getLeaderCount().get());
        assertEquals(0, TestJobEngineStateModelV1.getStandbyCount().get());
        assertEquals(0, TestJobEngineStateModelV1.getOfflineCount().get());

        connectors.remove(0).stop();
        TestJobEngineStateModelV1.getLeaderCount().decrementAndGet();
        Thread.sleep(1000);
        assertEquals(0, TestJobEngineStateModelV1.getLeaderCount().get());
        assertEquals(0, TestJobEngineStateModelV1.getStandbyCount().get());
        assertEquals(0, TestJobEngineStateModelV1.getOfflineCount().get());
        
    }

    @After
    public void tearDown() {
        server.shutdown();
    }
}
