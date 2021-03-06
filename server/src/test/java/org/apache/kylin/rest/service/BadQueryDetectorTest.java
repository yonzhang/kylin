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

package org.apache.kylin.rest.service;

import static org.junit.Assert.*;

import java.util.ArrayList;

import org.apache.kylin.rest.request.SQLRequest;
import org.junit.Test;

public class BadQueryDetectorTest {

    @SuppressWarnings("deprecation")
    @Test
    public void test() throws InterruptedException {
        int alertMB = BadQueryDetector.getSystemAvailMB() * 2;
        int alertRunningSec = 5;
        String mockSql = "select * from just_a_test";
        final ArrayList<String[]> alerts = new ArrayList<>();

        BadQueryDetector badQueryDetector = new BadQueryDetector(alertRunningSec * 1000, alertMB, alertRunningSec, alertRunningSec * 5);
        badQueryDetector.registerNotifier(new BadQueryDetector.Notifier() {
            @Override
            public void badQueryFound(String adj, int runningSec, String sql, Thread t) {
                alerts.add(new String[] { adj, sql });
            }
        });
        badQueryDetector.start();

        {
            Thread.sleep(1000);
            
            SQLRequest sqlRequest = new SQLRequest();
            sqlRequest.setSql(mockSql);
            badQueryDetector.queryStart(Thread.currentThread(), sqlRequest);

            // make sure bad query check happens twice
            Thread.sleep((alertRunningSec * 2 + 1) * 1000);

            badQueryDetector.queryEnd(Thread.currentThread());
        }

        badQueryDetector.stop();

        assertEquals(3, alerts.size());
        // first check founds Low mem
        assertArrayEquals(new String[] { "Low mem", mockSql }, alerts.get(0));
        // second check founds Low mem & Slow
        assertArrayEquals(new String[] { "Slow", mockSql }, alerts.get(1));
        assertArrayEquals(new String[] { "Low mem", mockSql }, alerts.get(2));
    }
}
