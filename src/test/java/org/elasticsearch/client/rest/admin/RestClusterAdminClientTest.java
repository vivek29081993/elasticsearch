/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.client.rest.admin;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsResponse;
import org.elasticsearch.client.rest.AbstractRestClientTest;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

/**
 */
public class RestClusterAdminClientTest extends AbstractRestClientTest {

    @Test
    public void testClusterStats() {
        ClusterStatsResponse response = clusterAdminClient.prepareClusterStats().get();
    }

    @Test
    public void testState() {
        ClusterStateResponse response = clusterAdminClient.prepareState().get();
    }

    @Test
    public void testHealth() {
        ClusterHealthResponse response = clusterAdminClient.prepareHealth().get();
        assertNotNull(response.getClusterName());
    }


}
