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
package org.elasticsearch.client.rest;

import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.support.broadcast.BroadcastOperationResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.ClusterAdminClient;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.common.Strings;
import org.junit.After;
import org.junit.Before;

import java.io.InputStream;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Brandon Kearby
 *         September 26, 2016
 */
public abstract class AbstractRestClientTest {

    protected IndicesAdminClient indicesAdminClient;
    protected ClusterAdminClient clusterAdminClient;
    protected String index;
    protected RestClient client;

    @Before
    public void setUp() {
        client = new RestClient("localhost");
        this.indicesAdminClient = client.admin().indices();
        this.clusterAdminClient = client.admin().cluster();
        this.index = createIndex();
    }

    @After
    public void tearDown() {
        deleteIndex(index);
    }

    protected String loadTestIndex() {
        InputStream in = this.getClass().getResourceAsStream("/org/elasticsearch/client/rest/test-index.json");
        return Strings.valueOf(in);
    }

    protected String loadTestIndexTemplate() {
        InputStream in = this.getClass().getResourceAsStream("/org/elasticsearch/client/rest/test-index-template.json");
        return Strings.valueOf(in);
    }

    protected String loadTestIndexPutMapping() {
        InputStream in = this.getClass().getResourceAsStream("/org/elasticsearch/client/rest/test-index-put-mapping.json");
        return Strings.valueOf(in);
    }

    protected void deleteIndex(String index) {
        DeleteIndexResponse response = indicesAdminClient.prepareDelete(index).get();
        assertAcknowledged(response);
    }

    protected String createIndex() {
        String index = UUID.randomUUID().toString();
        CreateIndexResponse response = indicesAdminClient.prepareCreate(index)
                .setSource(loadTestIndex()).get();
        assertAcknowledged(response);
        return index;
    }

    protected void assertAcknowledged(AcknowledgedResponse response) {
        assertTrue(response.isAcknowledged());
    }


    protected void assertBroadcastOperationResponse(BroadcastOperationResponse response) {
        assertTrue(response.getSuccessfulShards() > 0);
        assertEquals(0, response.getFailedShards());
    }




}
