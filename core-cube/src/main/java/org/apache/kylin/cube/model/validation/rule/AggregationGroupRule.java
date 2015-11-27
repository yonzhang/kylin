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

import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.model.AggregationGroup;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.validation.IValidatorRule;
import org.apache.kylin.cube.model.validation.ResultLevel;
import org.apache.kylin.cube.model.validation.ValidateContext;

/**
 *  find forbid overlaps in each AggregationGroup
 *  the include dims in AggregationGroup must contain all mandatory, hierarchy and joint
 */
public class AggregationGroupRule implements IValidatorRule<CubeDesc> {

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.kylin.metadata.validation.IValidatorRule#validate(java.lang.Object
     * , org.apache.kylin.metadata.validation.ValidateContext)
     */
    @Override
    public void validate(CubeDesc cube, ValidateContext context) {
        innerValidateMaxSize(cube, context);
    }

    /**
     * @param cube
     * @param context
     */
    private void innerValidateMaxSize(CubeDesc cube, ValidateContext context) {
        int maxSize = getMaxAgrGroupSize();

        int index = 0;
        for (AggregationGroup agg : cube.getAggregationGroups()) {
            if (agg.getIncludes() == null) {
                context.addResult(ResultLevel.ERROR, "Aggregation group " + index + " include dims not set");
                continue;
            }

            if (agg.getSelectRule() == null) {
                continue;
            }

            Set<String> includeDims = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            if (agg.getIncludes() != null) {
                for (String include : agg.getIncludes()) {
                    includeDims.add(include);
                }
            }

            Set<String> mandatoryDims = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            if (agg.getSelectRule().mandatory_dims != null) {
                for (String include : agg.getSelectRule().mandatory_dims) {
                    mandatoryDims.add(include);
                }
            }

            Set<String> hierarchyDims = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            if (agg.getSelectRule().hierarchy_dims != null) {
                for (String[] ss : agg.getSelectRule().hierarchy_dims) {
                    for (String s : ss)
                        hierarchyDims.add(s);
                }
            }

            Set<String> jointDims = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            if (agg.getSelectRule().joint_dims != null) {
                for (String[] ss : agg.getSelectRule().joint_dims) {
                    for (String s : ss)
                        jointDims.add(s);
                }
            }

            if (!includeDims.containsAll(mandatoryDims) || !includeDims.containsAll(hierarchyDims) || !includeDims.containsAll(jointDims)) {
                context.addResult(ResultLevel.ERROR, "Aggregation group " + index + " Include dims not containing all the used dims");
            }

            Set<String> normalDims = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            normalDims.addAll(includeDims);
            normalDims.removeAll(mandatoryDims);
            normalDims.removeAll(hierarchyDims);
            normalDims.removeAll(jointDims);

            int normalDimSize = normalDims.size();
            int hierarchySize = (agg.getSelectRule().hierarchy_dims == null ? 0 : agg.getSelectRule().hierarchy_dims.length);
            int jointSize = agg.getSelectRule().joint_dims == null ? 0 : agg.getSelectRule().joint_dims.length;
            if (normalDimSize + hierarchySize + jointSize > maxSize) {
                context.addResult(ResultLevel.ERROR, "Aggregation group " + index + " has too many dimensions");
            }

            if (CollectionUtils.containsAny(mandatoryDims, hierarchyDims)) {
                context.addResult(ResultLevel.ERROR, "Aggregation group " + index + " mandatory dims overlap with hierarchy dims");
            }
            if (CollectionUtils.containsAny(mandatoryDims, jointDims)) {
                context.addResult(ResultLevel.ERROR, "Aggregation group " + index + " mandatory dims overlap with joint dims");
            }

            int jointDimNum = 0;
            if (agg.getSelectRule().joint_dims != null) {
                for (String[] joints : agg.getSelectRule().joint_dims) {

                    Set<String> oneJoint = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
                    for (String s : joints) {
                        oneJoint.add(s);
                    }

                    if (oneJoint.size() < 2) {
                        context.addResult(ResultLevel.ERROR, "Aggregation group " + index + " require at least 2 dims in a joint");
                    }
                    jointDimNum += oneJoint.size();

                    int overlapHierarchies = 0;
                    if (agg.getSelectRule().hierarchy_dims != null) {
                        for (String[] oneHierarchy : agg.getSelectRule().hierarchy_dims) {
                            Set<String> share = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
                            share.addAll(CollectionUtils.intersection(oneJoint, Arrays.asList(oneHierarchy)));

                            if (!share.isEmpty()) {
                                overlapHierarchies++;
                            }
                            if (share.size() > 1) {
                                context.addResult(ResultLevel.ERROR, "Aggregation group " + index + " joint columns overlap with more than 1 dim in same hierarchy");
                            }
                        }

                        if (overlapHierarchies > 1) {
                            context.addResult(ResultLevel.ERROR, "Aggregation group " + index + " joint columns overlap with more than 1 hierarchies");
                        }
                    }
                }

                if (jointDimNum != jointDims.size()) {
                    context.addResult(ResultLevel.ERROR, "Aggregation group " + index + " a dim exist in more than 1 joint");
                }
            }

            index++;
        }
    }

    protected int getMaxAgrGroupSize() {
        String size = KylinConfig.getInstanceFromEnv().getProperty(KEY_MAX_AGR_GROUP_SIZE, String.valueOf(DEFAULT_MAX_AGR_GROUP_SIZE));
        return Integer.parseInt(size);
    }
}
