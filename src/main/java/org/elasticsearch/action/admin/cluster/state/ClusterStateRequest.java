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

package org.elasticsearch.action.admin.cluster.state;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.MasterNodeReadOperationRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.metrics.Metric;
import org.elasticsearch.common.util.UriBuilder;
import org.elasticsearch.rest.RestRequest;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class ClusterStateRequest extends MasterNodeReadOperationRequest<ClusterStateRequest> implements IndicesRequest.Replaceable {

    private boolean routingTable = true;
    private boolean nodes = true;
    private boolean metaData = true;
    private boolean blocks = true;
    private String[] indices = Strings.EMPTY_ARRAY;

    public ClusterStateRequest() {
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public ClusterStateRequest all() {
        routingTable = true;
        nodes = true;
        metaData = true;
        blocks = true;
        indices = Strings.EMPTY_ARRAY;
        return this;
    }
    
    public ClusterStateRequest clear() {
        routingTable = false;
        nodes = false;
        metaData = false;
        blocks = false;
        indices = Strings.EMPTY_ARRAY;
        return this;
    }

    public boolean routingTable() {
        return routingTable;
    }

    public ClusterStateRequest routingTable(boolean routingTable) {
        this.routingTable = routingTable;
        return this;
    }

    public boolean nodes() {
        return nodes;
    }

    public ClusterStateRequest nodes(boolean nodes) {
        this.nodes = nodes;
        return this;
    }

    public boolean metaData() {
        return metaData;
    }

    public ClusterStateRequest metaData(boolean metaData) {
        this.metaData = metaData;
        return this;
    }

    public boolean blocks() {
        return blocks;
    }

    public ClusterStateRequest blocks(boolean blocks) {
        this.blocks = blocks;
        return this;
    }

    @Override
    public String[] indices() {
        return indices;
    }

    @Override
    public ClusterStateRequest indices(String... indices) {
        this.indices = indices;
        return this;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return IndicesOptions.lenientExpandOpen();
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        routingTable = in.readBoolean();
        nodes = in.readBoolean();
        metaData = in.readBoolean();
        blocks = in.readBoolean();
        indices = in.readStringArray();
        // fake support for indices in pre 1.2.0 versions
        if (in.getVersion().before(Version.V_1_2_0)) {
            in.readStringArray();
        }
        readLocal(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(routingTable);
        out.writeBoolean(nodes);
        out.writeBoolean(metaData);
        out.writeBoolean(blocks);
        out.writeStringArray(indices);
        // fake support for indices in pre 1.2.0 versions
        if (out.getVersion().before(Version.V_1_2_0)) {
            out.writeStringArray(Strings.EMPTY_ARRAY);
        }
        writeLocal(out);
    }

    @Override
    public RestRequest.Method getMethod() {
        return RestRequest.Method.GET;
    }

    @Override
    public Map<String, String> getParams() {
        List<String> metrics = getMetrics();
        return new MapBuilder<>(super.getParams())
                .putIf("indices", Joiner.on(',').join(indices()), indices.length > 0)
                .putIf("metrics", Joiner.on(',').join(metrics), !metrics.isEmpty())
                .map();
    }

    private List<String> getMetrics() {
        List<String> metrics = Lists.newArrayList();
        if (this.routingTable) {
            metrics.add(ClusterState.Metric.ROUTING_TABLE.getValue());
        }
        if (this.blocks) {
            metrics.add(ClusterState.Metric.BLOCKS.getValue());
        }
        if (this.metaData) {
            metrics.add(ClusterState.Metric.METADATA.getValue());
        }
        if (this.nodes) {
            metrics.add(ClusterState.Metric.NODES.getValue());
        }
        return metrics;
    }

    @Override
    public String getEndPoint() {
        return UriBuilder.newBuilder()
                .slash("_cluster")
                .slash("state")
                .build();
    }
}
