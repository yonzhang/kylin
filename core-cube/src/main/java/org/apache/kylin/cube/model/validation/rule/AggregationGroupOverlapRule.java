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

package org.apache.kylin.cube.model.validation.rule;

import org.apache.kylin.cube.model.AggregationGroup;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.validation.IValidatorRule;
import org.apache.kylin.cube.model.validation.ResultLevel;
import org.apache.kylin.cube.model.validation.ValidateContext;

/**
 * find forbid overlaps in each AggregationGroup
 *  the include dims in AggregationGroup must contain all mandatory, hierarchy and joint
 */
public class AggregationGroupOverlapRule implements IValidatorRule<CubeDesc> {

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.kylin.metadata.validation.IValidatorRule#validate(java.lang.Object
     * , org.apache.kylin.metadata.validation.ValidateContext)
     */
    @Override
    public void validate(CubeDesc cube, ValidateContext context) {

        int index = 0;
        for (AggregationGroup agg : cube.getAggregationGroups()) {
            
            if ((agg.getMandatoryColumnMask() & agg.getHierarchyDimsMask()) != 0) {
                context.addResult(ResultLevel.ERROR, "Aggregation group " + index + " mandatory dims overlap with hierarchy dims");
            }
            if ((agg.getMandatoryColumnMask() & agg.getJointDimsMask()) != 0) {
                context.addResult(ResultLevel.ERROR, "Aggregation group " + index + " mandatory dims overlap with joint dims");
            }

            int jointDimNum = 0;
            for (Long joint : agg.getJointDims()) {
                jointDimNum += Long.bitCount(joint);
                if (jointDimNum < 2) {
                    context.addResult(ResultLevel.ERROR, "Aggregation group " + index + " require at least 2 dims in a joint");
                }

                int overlapHierarchies = 0;
                for (AggregationGroup.HierarchyMask mask : agg.getHierarchyMasks()) {
                    long share = (joint & mask.fullMask);
                    if (share != 0) {
                        overlapHierarchies++;
                    }
                    if (Long.bitCount(share) > 1) {
                        context.addResult(ResultLevel.ERROR, "Aggregation group " + index + " joint columns overlap with more than 1 dim in same hierarchy");
                    }
                }

                if (overlapHierarchies > 1) {
                    context.addResult(ResultLevel.ERROR, "Aggregation group " + index + " joint columns overlap with more than 1 hierarchies");
                }
            }

            if (jointDimNum != Long.bitCount(agg.getJointDimsMask())) {
                context.addResult(ResultLevel.ERROR, "Aggregation group " + index + " a dim exist in more than 1 joint");
            }

            index++;
        }

    }

}
