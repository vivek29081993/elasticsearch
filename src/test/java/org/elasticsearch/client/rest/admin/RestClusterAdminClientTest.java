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

import com.google.common.collect.Maps;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.repositories.delete.DeleteRepositoryResponse;
import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesResponse;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryResponse;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsResponse;
import org.elasticsearch.client.rest.AbstractRestClientTest;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.junit.Test;

import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.*;

/**
 */
public class RestClusterAdminClientTest extends AbstractRestClientTest {

    @Test
    public void testClusterStats() {
        ClusterStatsResponse response = clusterAdminClient.prepareClusterStats().get();
    }

    @Test
    public void testClusterState() {
        ClusterStateResponse response = clusterAdminClient.prepareState().get();
        ClusterState state = response.getState();
        assertNotNull(state);
        assertNotNull(state.blocks());
        assertNotNull(state.routingTable());
        assertNotNull(state.getMetaData());
        assertNotNull(state.getMetaData().indices());
        ClusterName clusterName = response.getClusterName();
        assertNotNull(clusterName);
        assertNotNull(clusterName.value());
    }

    @Test
    public void testHealth() {
        ClusterHealthResponse response = clusterAdminClient.prepareHealth().get();
        assertNotNull(response.getClusterName());
    }

    @Test
    public void testCrudRepositories() {
        String repoName = "repo-" + UUID.randomUUID().toString();
        Map<String, Object> settings = Maps.newLinkedHashMap();
        settings.put("location", "/tmp/" + repoName);
        PutRepositoryResponse putResponse = clusterAdminClient.preparePutRepository(repoName)
                .setType("fs")
                .setSettings(settings)
                .get();
        assertAcknowledged(putResponse);

        GetRepositoriesResponse getResponse = clusterAdminClient.prepareGetRepositories(repoName).get();
        Iterator<RepositoryMetaData> metaDataIterator = getResponse.iterator();
        assertTrue(metaDataIterator.hasNext());
        RepositoryMetaData metaData = metaDataIterator.next();
        assertEquals(repoName, metaData.name());

        DeleteRepositoryResponse deleteResponse = clusterAdminClient.prepareDeleteRepository(repoName).get();
        assertAcknowledged(deleteResponse);
    }

    @Test
    public void testCrudSnapshots() {
        String repoName = "repo-" + UUID.randomUUID().toString();
        Map<String, Object> settings = Maps.newLinkedHashMap();
        settings.put("location", "/tmp/" + repoName);
        PutRepositoryResponse putResponse = clusterAdminClient.preparePutRepository(repoName)
                .setType("fs")
                .setSettings(settings)
                .get();
        assertAcknowledged(putResponse);

        String snapshotName = "snapshot-" + UUID.randomUUID().toString();
        CreateSnapshotResponse snapshotResponse;
        snapshotResponse = clusterAdminClient.prepareCreateSnapshot(repoName, snapshotName)
                .setIndices(index)
                .setWaitForCompletion(true)
                .get();
        SnapshotInfo snapshotInfo = snapshotResponse.getSnapshotInfo();
        assertNotNull(snapshotInfo);
        assertEquals(snapshotName, snapshotInfo.name());
        assertFalse(snapshotInfo.indices().isEmpty());

        DeleteSnapshotResponse deleteSnapshotResponse = clusterAdminClient.prepareDeleteSnapshot(repoName, snapshotName).get();
        assertAcknowledged(deleteSnapshotResponse);
    }


}
