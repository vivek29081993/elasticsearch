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

import com.carrotsearch.ant.tasks.junit4.dependencies.com.google.common.collect.Maps;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.alias.exists.AliasesExistRequestBuilder;
import org.elasticsearch.action.admin.indices.alias.exists.AliasesExistResponse;
import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheResponse;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.flush.FlushResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.delete.DeleteMappingResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateResponse;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateResponse;
import org.elasticsearch.action.support.broadcast.BroadcastOperationResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.rest.RestClient;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.index.query.FilterBuilders;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.*;

/**
 * @author Brandon Kearby
 *         September 22, 2016
 */
public class RestIndicesAdminClientTest {

    private IndicesAdminClient indicesAdminClient;
    private String index;

    @Before
    public void setUp() {
        RestClient client = new RestClient("localhost");
        this.indicesAdminClient = client.admin().indices();
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

    private String loadTestIndexTemplate() {
        InputStream in = this.getClass().getResourceAsStream("/org/elasticsearch/client/rest/test-index-template.json");
        return Strings.valueOf(in);
    }

    private String loadTestIndexPutMapping() {
        InputStream in = this.getClass().getResourceAsStream("/org/elasticsearch/client/rest/test-index-put-mapping.json");
        return Strings.valueOf(in);
    }

    private void deleteIndex(String index) {
        DeleteIndexResponse response = indicesAdminClient.prepareDelete(index).get();
        assertAcknowledged(response);
    }

    private String createIndex() {
        String index = UUID.randomUUID().toString();
        CreateIndexResponse response = indicesAdminClient.prepareCreate(index)
                .setSource(loadTestIndex()).get();
        assertAcknowledged(response);
        return index;
    }

    @Test
    public void testGetIndex() {
        GetIndexResponse response;
        response = indicesAdminClient.prepareGetIndex().addIndices(index).get();
        assertFalse(response.getMappings().get(index).isEmpty());
        assertFalse(response.getSettings().get(index).names().isEmpty());
        assertFalse(response.getAliases().get(index).isEmpty());
        assertFalse(response.getWarmers().get(index).isEmpty());
    }



    @Test
    public void testFlushIndex() {
        FlushResponse response = indicesAdminClient.prepareFlush(index).get();
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

        OpenIndexResponse response = indicesAdminClient.prepareOpen(index).get();
        assertAcknowledged(response);
    }

    @Test
    public void testExistsIndex() {
        IndicesExistsResponse response = indicesAdminClient.prepareExists(index).get();
        assertTrue(response.isExists());
    }

    @Test
    public void testPutTemplate() {
        putTemplate();
    }

    private void putTemplate() {
        PutIndexTemplateResponse response;
        response = indicesAdminClient.preparePutTemplate("logs_template").setSource(loadTestIndexTemplate()).get();
        assertAcknowledged(response);
    }

    @Test
    public void testDeleteTemplate() {
        putTemplate();

        DeleteIndexTemplateResponse response;
        response = indicesAdminClient.prepareDeleteTemplate("logs_template").get();
        assertAcknowledged(response);
    }

    @Test
    @Ignore
    public void testAliases() {
        IndicesAliasesResponse response;
        response = indicesAdminClient.prepareAliases()
                .addAlias(index, "myAliasNoArg")
                .addAlias(index, "blueGuy", FilterBuilders.termFilter("color", "blue"))
                .removeAlias(index, "alias_1").get();
        assertAcknowledged(response);
    }

    @Test
    public void testClearCache() {
        ClearIndicesCacheResponse response = indicesAdminClient.prepareClearCache(index).get();
        assertBroadcastOperationResponse(response);
    }

    @Test
    public void testDeleteMapping() {
        DeleteMappingResponse response = indicesAdminClient.prepareDeleteMapping(index).get();
        assertAcknowledged(response);
    }

    @Test
    public void testGetMapping() throws IOException {
        GetMappingsResponse response = indicesAdminClient.prepareGetMappings(index).get();
        ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> mappings;
        mappings = response.getMappings();
        assertTrue(!mappings.isEmpty());
        ImmutableOpenMap<String, MappingMetaData> mapping = mappings.get(index);
        assertNotNull(mapping);
        assertTrue(!mapping.isEmpty());
        MappingMetaData stats = mapping.get("stats");
        assertNotNull(stats);
    }

    @Test
    public void testPutMapping() throws IOException {
        String mapping = loadTestIndexPutMapping();
        PutMappingResponse response = indicesAdminClient.preparePutMapping(index)
                .setType("stats")
                .setSource(mapping)
                .setIgnoreConflicts(true).get();
        assertAcknowledged(response);
    }

    @Test
    @Ignore
    public void testAliasesExist() {
        
        AliasesExistResponse response;
        AliasesExistRequestBuilder builder = indicesAdminClient.prepareAliasesExist("alias_1");
        response = builder.get();
        assertFalse(response.exists());

        response = indicesAdminClient.prepareAliasesExist(UUID.randomUUID().toString()).get();
        assertFalse(response.exists());

    }

    private void assertBroadcastOperationResponse(BroadcastOperationResponse response) {
        assertTrue(response.getSuccessfulShards() > 0);
        assertEquals(0, response.getFailedShards());
    }

    private void closeIndex() {
        CloseIndexResponse response = indicesAdminClient.prepareClose(index).get();
        assertAcknowledged(response);
    }

    private void assertAcknowledged(AcknowledgedResponse response) {
        assertTrue(response.isAcknowledged());
    }


}
