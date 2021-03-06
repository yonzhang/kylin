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

package org.apache.kylin.common.util;

import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.io.IOException;

/**
 * @author ysong1
 */
public class HBaseMetadataTestCase extends AbstractKylinTestCase {

    static {
        if (useSandbox()) {
            try {
                File sandboxFolder = new File("../examples/test_case_data/sandbox/");
                if (sandboxFolder.exists() == false) {
                    throw new IOException("The sandbox folder doesn't exist: " + sandboxFolder.getAbsolutePath());
                }
                ClassUtil.addClasspath(sandboxFolder.getAbsolutePath());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void createTestMetadata() throws Exception {
        staticCreateTestMetadata();
    }

    @Override
    public void cleanupTestMetadata() {
        staticCleanupTestMetadata();
    }

    public static void staticCreateTestMetadata() throws Exception {
        if (useSandbox()) {
            staticCreateTestMetadata(SANDBOX_TEST_DATA);
        } else {
            staticCreateTestMetadata(MINICLUSTER_TEST_DATA);
            HBaseMiniclusterHelper.startupMinicluster();
        }

    }

    public static boolean useSandbox() {
        String useSandbox = System.getProperty("useSandbox");
        if (StringUtils.isEmpty(useSandbox)) {
            return true;
        }

        return Boolean.parseBoolean(useSandbox);
    }

}
