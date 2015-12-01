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

package org.apache.kylin.rest.controller;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import joptsimple.internal.Strings;
import org.apache.helix.ExternalViewChangeListener;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.ExternalView;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.restclient.Broadcaster;
import org.apache.kylin.job.JobInstance;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.helix.HelixJobEngineAdmin;
import org.apache.kylin.rest.helix.JobControllerConnector;
import org.apache.kylin.rest.helix.v1.DefaultStateModelFactory;
import org.apache.kylin.rest.helix.v1.JobEngineSMDV1;
import org.apache.kylin.rest.request.JobListRequest;
import org.apache.kylin.rest.service.JobService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import java.io.IOException;
import java.util.*;

/**
 * @author ysong1
 * @author Jack
 * 
 */
@Controller
@RequestMapping(value = "jobs")
public class JobController extends BasicController implements InitializingBean {
    private static final Logger logger = LoggerFactory.getLogger(JobController.class);

    @Autowired
    private JobService jobService;

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
     */
    @Override
    public void afterPropertiesSet() throws Exception {

        String timeZone = jobService.getConfig().getTimeZone();
        TimeZone tzone = TimeZone.getTimeZone(timeZone);
        TimeZone.setDefault(tzone);

        final String instanceName = HelixJobEngineAdmin.getCurrentInstanceName();
        final KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();

        final String zkAddress = Preconditions.checkNotNull(kylinConfig.getZookeeperAddress());
        final HelixJobEngineAdmin helixJobEngineAdmin = HelixJobEngineAdmin.getInstance(zkAddress);
        helixJobEngineAdmin.initV1(kylinConfig.getClusterName(), HelixJobEngineAdmin.RESOURCE_NAME);
        helixJobEngineAdmin.startController(kylinConfig.getClusterName(), new ExternalViewChangeListener() {
            @Override
            public void onExternalViewChange(List<ExternalView> list, NotificationContext notificationContext) {
                for (ExternalView view : list) {
                    if (view.getResourceName().equals(HelixJobEngineAdmin.RESOURCE_NAME)) {
                        final Set<String> partitionSet = view.getPartitionSet();
                        Preconditions.checkArgument(partitionSet.size() == 1);
                        final Map<String, String> stateMap = view.getStateMap(partitionSet.iterator().next());

                        List<String> liveInstances = Lists.newArrayList();
                        for (String instance : stateMap.keySet()) {
                            String state = stateMap.get(instance);
                            if (JobEngineSMDV1.States.LEADER.toString().equalsIgnoreCase(state)
                                    || JobEngineSMDV1.States.STANDBY.toString().equalsIgnoreCase(state)) {
                                liveInstances.add(instance);
                            }
                        }

                        updateKylinCluster(liveInstances);
                    }
                }
            }
        });
        final String hostname = instanceName.substring(0, instanceName.lastIndexOf("_"));
        final String port = instanceName.substring(instanceName.lastIndexOf("_") + 1);
        final JobControllerConnector jcc = new JobControllerConnector(hostname, port, zkAddress, kylinConfig.getClusterName(), new DefaultStateModelFactory(instanceName, kylinConfig));
        jcc.register();
        helixJobEngineAdmin.rebalance(kylinConfig.getClusterName(), HelixJobEngineAdmin.RESOURCE_NAME);
        jcc.start();

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                jcc.stop();
                helixJobEngineAdmin.stopController(kylinConfig.getClusterName());
            }
        }));

    }

    /**
     * get all cube jobs
     * 
     * @return
     * @throws IOException
     */
    @RequestMapping(value = "", method = { RequestMethod.GET })
    @ResponseBody
    public List<JobInstance> list(JobListRequest jobRequest) {

        List<JobInstance> jobInstanceList = Collections.emptyList();
        List<JobStatusEnum> statusList = new ArrayList<JobStatusEnum>();

        if (null != jobRequest.getStatus()) {
            for (int status : jobRequest.getStatus()) {
                statusList.add(JobStatusEnum.getByCode(status));
            }
        }

        try {
            jobInstanceList = jobService.listAllJobs(jobRequest.getCubeName(), jobRequest.getProjectName(), statusList, jobRequest.getLimit(), jobRequest.getOffset());
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            throw new InternalErrorException(e);
        }
        return jobInstanceList;
    }

    /**
     * Get a cube job
     * 
     * @return
     * @throws IOException
     */
    @RequestMapping(value = "/{jobId}", method = { RequestMethod.GET })
    @ResponseBody
    public JobInstance get(@PathVariable String jobId) {
        JobInstance jobInstance = null;
        try {
            jobInstance = jobService.getJobInstance(jobId);
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            throw new InternalErrorException(e);
        }

        return jobInstance;
    }

    /**
     * Get a job step output
     * 
     * @return
     * @throws IOException
     */
    @RequestMapping(value = "/{jobId}/steps/{stepId}/output", method = { RequestMethod.GET })
    @ResponseBody
    public Map<String, String> getStepOutput(@PathVariable String jobId, @PathVariable String stepId) {
        Map<String, String> result = new HashMap<String, String>();
        result.put("jobId", jobId);
        result.put("stepId", String.valueOf(stepId));
        result.put("cmd_output", jobService.getExecutableManager().getOutput(stepId).getVerboseMsg());
        return result;
    }

    /**
     * Resume a cube job
     * 
     * @return
     * @throws IOException
     */
    @RequestMapping(value = "/{jobId}/resume", method = { RequestMethod.PUT })
    @ResponseBody
    public JobInstance resume(@PathVariable String jobId) {
        try {
            final JobInstance jobInstance = jobService.getJobInstance(jobId);
            jobService.resumeJob(jobInstance);
            return jobService.getJobInstance(jobId);
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            throw new InternalErrorException(e);
        }
    }

    /**
     * Cancel a job
     * 
     * @return
     * @throws IOException
     */
    @RequestMapping(value = "/{jobId}/cancel", method = { RequestMethod.PUT })
    @ResponseBody
    public JobInstance cancel(@PathVariable String jobId) {

        try {
            final JobInstance jobInstance = jobService.getJobInstance(jobId);
            return jobService.cancelJob(jobInstance);
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            throw new InternalErrorException(e);
        }

    }

    public void setJobService(JobService jobService) {
        this.jobService = jobService;
    }

    private void updateKylinCluster(List<String> instances) {
        List<String> instanceRestAddresses = Lists.newArrayList();
        for (String instanceName : instances) {
            int indexOfUnderscore = instanceName.lastIndexOf("_");
            instanceRestAddresses.add(instanceName.substring(0, indexOfUnderscore) + ":" + instanceName.substring(indexOfUnderscore + 1));
        }
        String restServersInCluster = Strings.join(instanceRestAddresses, ",");
        KylinConfig.getInstanceFromEnv().setProperty(KylinConfig.KYLIN_REST_SERVERS, restServersInCluster);
        System.setProperty(KylinConfig.KYLIN_REST_SERVERS, restServersInCluster);
        Broadcaster.clearCache();

    }

}
