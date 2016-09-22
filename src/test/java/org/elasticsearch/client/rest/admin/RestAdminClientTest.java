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

import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.flush.FlushResponse;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.rest.RestClient;
import org.elasticsearch.common.Strings;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;
import java.util.UUID;

import static org.junit.Assert.assertTrue;

/**
 * @author Brandon Kearby
 *         September 22, 2016
 */
public class RestAdminClientTest {

    private AdminClient adminClient;
    private String index;

    @Before
    public void setUp() {
        RestClient client = new RestClient("localhost");
        this.adminClient = client.admin();
        this.index = createIndex();
    }

    @After
    public void tearDown() {
        deleteIndex(index);
    }

    private String loadTestIndex() {
        InputStream in = this.getClass().getResourceAsStream("/org/elasticsearch/client/rest/test-index.json");
        return Strings.valueOf(in);
    }

    private void deleteIndex(String index) {
        DeleteIndexResponse response = this.adminClient.indices().prepareDelete(index).get();
        assertAcknowledged(response);
    }

    private String createIndex() {
        String index = UUID.randomUUID().toString();
        CreateIndexResponse response = this.adminClient.indices().prepareCreate(index)
                .setSource(loadTestIndex()).get();
        assertAcknowledged(response);
        return index;
    }

    @Test
    public void testFlushIndex() {
        FlushResponse response = this.adminClient.indices().prepareFlush(index).get();
        assertTrue(response.getFailedShards() == 0);
        assertTrue(response.getSuccessfulShards() > 0);
    }

    @Test
    public void testCloseIndex() {
        closeIndex();
    }

    @Test
    public void testOpenIndex() throws InterruptedException {
        closeIndex();
        Thread.sleep(2000);

        OpenIndexResponse response = this.adminClient.indices().prepareOpen(index).get();
        assertAcknowledged(response);
    }

    @Test
    public void testExistsIndex() {
        IndicesExistsResponse response = this.adminClient.indices().prepareExists(index).get();
        assertTrue(response.isExists());
    }

    @Test
    public void testIndexTemplate() {
        //todo bdk
    }

    private void closeIndex() {
        CloseIndexResponse response = this.adminClient.indices().prepareClose(index).get();
        assertAcknowledged(response);
    }

    private void assertAcknowledged(AcknowledgedResponse response) {
        assertTrue(response.isAcknowledged());
    }


}
