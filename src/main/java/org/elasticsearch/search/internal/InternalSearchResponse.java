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

package org.elasticsearch.search.internal;

import com.google.common.collect.Maps;
import org.elasticsearch.Version;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.facet.Facets;
import org.elasticsearch.search.facet.InternalFacets;
import org.elasticsearch.search.suggest.Suggest;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.search.internal.InternalSearchHits.readSearchHits;

/**
 *
 */
public class InternalSearchResponse implements Streamable, ToXContent {

    public static InternalSearchResponse empty() {
        return new InternalSearchResponse(InternalSearchHits.empty(), null, null, null, false, null);
    }

    private InternalSearchHits hits;

    private InternalFacets facets;

    private InternalAggregations aggregations;

    private Suggest suggest;

    private boolean timedOut;

    private Boolean terminatedEarly = null;

    private InternalSearchResponse() {
    }

    public InternalSearchResponse(InternalSearchHits hits, InternalFacets facets, InternalAggregations aggregations, Suggest suggest, boolean timedOut, Boolean terminatedEarly) {
        this.hits = hits;
        this.facets = facets;
        this.aggregations = aggregations;
        this.suggest = suggest;
        this.timedOut = timedOut;
        this.terminatedEarly = terminatedEarly;
    }

    public boolean timedOut() {
        return this.timedOut;
    }

    public Boolean terminatedEarly() {
        return this.terminatedEarly;
    }

    public SearchHits hits() {
        return hits;
    }

    public Facets facets() {
        return facets;
    }

    public Aggregations aggregations() {
        return aggregations;
    }

    public Suggest suggest() {
        return suggest;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        hits.toXContent(builder, params);
        if (facets != null) {
            facets.toXContent(builder, params);
        }
        if (aggregations != null) {
            aggregations.toXContent(builder, params);
        }
        if (suggest != null) {
            suggest.toXContent(builder, params);
        }
        return builder;
    }

    public static InternalSearchResponse newInternalSearchResponse(StreamInput in) throws IOException {
        InternalSearchResponse response = new InternalSearchResponse();
        response.readFrom(in);
        return response;
    }

    public void readHits(VersionedXContentParser parser) throws IOException {
        this.hits = InternalSearchHits.readSearchHits(parser);
    }

    public void readHits(XContentObject source) throws IOException {
        this.hits = InternalSearchHits.readSearchHits(source);
    }

    public void readAggregations(XContentParser parser) throws IOException {
        this.aggregations = InternalAggregations.readAggregations(parser);
    }

    public void readAggregations(XContentObject source) throws IOException {
        this.aggregations = InternalAggregations.readAggregations(source);
    }



    @Override
    public void readFrom(StreamInput in) throws IOException {
        hits = readSearchHits(in);
        if (in.readBoolean()) {
            facets = InternalFacets.readFacets(in);
        }
        if (in.readBoolean()) {
            aggregations = InternalAggregations.readAggregations(in);
        }
        if (in.readBoolean()) {
            suggest = Suggest.readSuggest(Suggest.Fields.SUGGEST, in);
        }
        timedOut = in.readBoolean();

        if (in.getVersion().onOrAfter(Version.V_1_4_0_Beta1)) {
            terminatedEarly = in.readOptionalBoolean();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        hits.writeTo(out);
        if (facets == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            facets.writeTo(out);
        }
        if (aggregations == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            aggregations.writeTo(out);
        }
        if (suggest == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            suggest.writeTo(out);
        }
        out.writeBoolean(timedOut);

        if (out.getVersion().onOrAfter(Version.V_1_4_0_Beta1)) {
            out.writeOptionalBoolean(terminatedEarly);

        }
    }

    public static InternalSearchResponse newInternalSearchResponse(boolean timedOut) throws IOException {
        InternalSearchResponse internalSearchResponse = new InternalSearchResponse();
        internalSearchResponse.timedOut = timedOut;
        return internalSearchResponse;
    }



}