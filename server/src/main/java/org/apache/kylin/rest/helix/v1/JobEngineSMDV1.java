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

import org.apache.helix.HelixDefinedState;
import org.apache.helix.model.StateModelDefinition;

/**
 */
public final class JobEngineSMDV1 {

    public static final String STATE_MODEL_NAME = "job_engine_model_v1";

    public enum States {
        LEADER,
        STANDBY,
        OFFLINE
    }
    
    private static class StateModelDefinitionV1Holder {
        private static StateModelDefinition instance = build();
        private static StateModelDefinition build() {
            StateModelDefinition.Builder builder = new StateModelDefinition.Builder(STATE_MODEL_NAME);
            // init state
            builder.initialState(States.OFFLINE.name());

            // add states
            builder.addState(States.LEADER.name(), 0);
            builder.addState(States.STANDBY.name(), 2);
            builder.addState(States.OFFLINE.name(), 1);
            for (HelixDefinedState state : HelixDefinedState.values()) {
                builder.addState(state.name());
            }

            // add transitions
            builder.addTransition(States.LEADER.name(), States.STANDBY.name(), 0);
            builder.addTransition(States.STANDBY.name(), States.LEADER.name(), 1);
            builder.addTransition(States.OFFLINE.name(), States.STANDBY.name(), 2);
            builder.addTransition(States.STANDBY.name(), States.OFFLINE.name(), 3);
            builder.addTransition(States.OFFLINE.name(), HelixDefinedState.DROPPED.name());

            // bounds
            builder.upperBound(States.LEADER.name(), 1);
            builder.dynamicUpperBound(States.STANDBY.name(), "R");

            return builder.build();
        }
    }
    
    public static StateModelDefinition getJobEngineStateModelDefinitionV1() {
        return StateModelDefinitionV1Holder.instance;
    }
    
}
