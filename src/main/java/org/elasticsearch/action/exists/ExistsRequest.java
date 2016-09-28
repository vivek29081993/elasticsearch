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

package org.elasticsearch.action.exists;

import com.google.common.base.Joiner;
import org.apache.http.HttpEntity;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.ElasticsearchGenerationException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionRestRequest;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequest;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.QuerySourceBuilder;
import org.elasticsearch.action.support.broadcast.BroadcastOperationRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.rest.support.HttpUtils;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.UriBuilder;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.rest.RestRequest;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

public class ExistsRequest extends BroadcastOperationRequest<ExistsRequest> {

    public static final float DEFAULT_MIN_SCORE = -1f;
    private float minScore = DEFAULT_MIN_SCORE;

    @Nullable
    protected String routing;

    @Nullable
    private String preference;

    private BytesReference source;
    private boolean sourceUnsafe;

    private String[] types = Strings.EMPTY_ARRAY;

    long nowInMillis;

    ExistsRequest() {
    }

    /**
     * Constructs a new exists request against the provided indices. No indices provided means it will
     * run against all indices.
     */
    public ExistsRequest(String... indices) {
        super(indices);
    }


    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = super.validate();
        return validationException;
    }

    @Override
    protected void beforeStart() {
        if (sourceUnsafe) {
            source = source.copyBytesArray();
            sourceUnsafe = false;
        }
    }

    /**
     * The minimum score of the documents to include in the count.
     */
    float minScore() {
        return minScore;
    }

    /**
     * The minimum score of the documents to include in the count. Defaults to <tt>-1</tt> which means all
     * documents will be considered.
     */
    public ExistsRequest minScore(float minScore) {
        this.minScore = minScore;
        return this;
    }

    /**
     * A comma separated list of routing values to control the shards the search will be executed on.
     */
    public String routing() {
        return this.routing;
    }

    /**
     * A comma separated list of routing values to control the shards the search will be executed on.
     */
    public ExistsRequest routing(String routing) {
        this.routing = routing;
        return this;
    }

    /**
     * The routing values to control the shards that the search will be executed on.
     */
    public ExistsRequest routing(String... routings) {
        this.routing = Strings.arrayToCommaDelimitedString(routings);
        return this;
    }

    /**
     * Routing preference for executing the search on shards
     */
    public ExistsRequest preference(String preference) {
        this.preference = preference;
        return this;
    }

    public String preference() {
        return this.preference;
    }

    /**
     * The source to execute.
     */
    BytesReference source() {
        return source;
    }

    /**
     * The source to execute.
     */
    public ExistsRequest source(QuerySourceBuilder sourceBuilder) {
        this.source = sourceBuilder.buildAsBytes(Requests.CONTENT_TYPE);
        this.sourceUnsafe = false;
        return this;
    }

    /**
     * The source to execute in the form of a map.
     */
    public ExistsRequest source(Map querySource) {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(Requests.CONTENT_TYPE);
            builder.map(querySource);
            return source(builder);
        } catch (IOException e) {
            throw new ElasticsearchGenerationException("Failed to generate [" + querySource + "]", e);
        }
    }

    public ExistsRequest source(XContentBuilder builder) {
        this.source = builder.bytes();
        this.sourceUnsafe = false;
        return this;
    }

    /**
     * The source to execute. It is preferable to use either {@link #source(byte[])}
     * or {@link #source(QuerySourceBuilder)}.
     */
    public ExistsRequest source(String querySource) {
        this.source = new BytesArray(querySource);
        this.sourceUnsafe = false;
        return this;
    }

    /**
     * The source to execute.
     */
    public ExistsRequest source(byte[] querySource) {
        return source(querySource, 0, querySource.length, false);
    }

    /**
     * The source to execute.
     */
    public ExistsRequest source(byte[] querySource, int offset, int length, boolean unsafe) {
        return source(new BytesArray(querySource, offset, length), unsafe);
    }

    public ExistsRequest source(BytesReference querySource, boolean unsafe) {
        this.source = querySource;
        this.sourceUnsafe = unsafe;
        return this;
    }

    /**
     * The types of documents the query will run against. Defaults to all types.
     */
    public String[] types() {
        return this.types;
    }

    /**
     * The types of documents the query will run against. Defaults to all types.
     */
    public ExistsRequest types(String... types) {
        this.types = types;
        return this;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        minScore = in.readFloat();
        routing = in.readOptionalString();
        preference = in.readOptionalString();
        sourceUnsafe = false;
        source = in.readBytesReference();
        types = in.readStringArray();

    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeFloat(minScore);
        out.writeOptionalString(routing);
        out.writeOptionalString(preference);
        out.writeBytesReference(source);
        out.writeStringArray(types);

    }

    @Override
    public String toString() {
        String sSource = "_na_";
        try {
            sSource = XContentHelper.convertToJson(source, false);
        } catch (Exception e) {
            // ignore
        }
        return "[" + Arrays.toString(indices) + "]" + Arrays.toString(types) + ", source[" + sSource + "]";
    }


    @Override
    public Map<String, String> getParams() {
        return  MapBuilder.<String, String>newMapBuilder()
                .putIfNotNull("routing", this.routing)
                .putIfNotNull("preference", this.preference)
                .putIf("min_score", String.valueOf(minScore), minScore != DEFAULT_MIN_SCORE).map();

    }

    @Override
    public ActionRestRequest getActionRestRequest(Version version) {
        if (version.onOrAfter(Version.V_5_0_0)) {
            return SearchAction.INSTANCE.newRequestBuilder(null)
                    .setIndices(this.indices)
                    .setTypes(this.types)
                    .setSource(this.source).setSize(0).request();
        }
        else {
            return new LegacyActionRestRequest(this);
        }
    }

    private static class LegacyActionRestRequest implements ActionRestRequest {
        private ExistsRequest request;

        LegacyActionRestRequest(ExistsRequest request) {
            this.request = request;
        }

        @Override
        public RestRequest.Method getMethod() {
            return RestRequest.Method.DELETE;
        }

        @Override
        public String getEndPoint() {
            UriBuilder uriBuilder = UriBuilder.newBuilder()
                    .csv(request.indices())
                    .csv(request.types())
                    .slash("_query");
            return uriBuilder.build();
        }

        @Override
        public HttpEntity getEntity() throws IOException {
            return new NStringEntity(XContentHelper.convertToJson(request.source, false), StandardCharsets.UTF_8);
        }

        @Override
        public Map<String, String> getParams() {
            return request.getParams();
        }

        @Override
        public HttpEntity getBulkEntity() throws IOException {
            return ActionRequest.EMPTY_ENTITY;
        }

    }

    private static class ActionRequestV5 implements ActionRestRequest {
        private ExistsRequest request;

        ActionRequestV5(ExistsRequest request) {
            this.request = request;
        }

        @Override
        public RestRequest.Method getMethod() {
            return RestRequest.Method.POST;
        }

        @Override
        public String getEndPoint() {
            UriBuilder uriBuilder = UriBuilder.newBuilder()
                    .csv(request.indices())
                    .csv(request.types())
                    .slash("_search");
            return uriBuilder.build();
        }

        @Override
        public HttpEntity getEntity() throws IOException {
            return new NStringEntity(XContentHelper.convertToJson(request.source, false), StandardCharsets.UTF_8);
        }

        @Override
        public Map<String, String> getParams() {
            return request.getParams();
        }

        @Override
        public HttpEntity getBulkEntity() throws IOException {
            return ActionRequest.EMPTY_ENTITY;
        }
    }


}
