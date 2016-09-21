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

package org.elasticsearch.action.deletebyquery;

import com.google.common.collect.Maps;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentObject;
import org.elasticsearch.common.xcontent.XContentParsable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Maps.newLinkedHashMap;

/**
 * The response of delete by query action. Holds the {@link IndexDeleteByQueryResponse}s from all the
 * different indices.
 */
public class DeleteByQueryResponse extends ActionResponse implements Iterable<IndexDeleteByQueryResponse> {

    private Map<String, IndexDeleteByQueryResponse> indices = newLinkedHashMap();

    DeleteByQueryResponse() {

    }

    @Override
    public Iterator<IndexDeleteByQueryResponse> iterator() {
        return indices.values().iterator();
    }

    /**
     * The responses from all the different indices.
     */
    public Map<String, IndexDeleteByQueryResponse> getIndices() {
        return indices;
    }

    /**
     * The response of a specific index.
     */
    public IndexDeleteByQueryResponse getIndex(String index) {
        return indices.get(index);
    }

    public RestStatus status() {
        RestStatus status = RestStatus.OK;
        for (IndexDeleteByQueryResponse indexResponse : indices.values()) {
            if (indexResponse.getFailedShards() > 0) {
                RestStatus indexStatus = indexResponse.getFailures()[0].status();
                if (indexResponse.getFailures().length > 1) {
                    for (int i = 1; i < indexResponse.getFailures().length; i++) {
                        if (indexResponse.getFailures()[i].status().getStatus() >= 500) {
                            indexStatus = indexResponse.getFailures()[i].status();
                        }
                    }
                }
                if (status.getStatus() < indexStatus.getStatus()) {
                    status = indexStatus;
                }
            }
        }
        return status;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            IndexDeleteByQueryResponse response = new IndexDeleteByQueryResponse();
            response.readFrom(in);
            indices.put(response.getIndex(), response);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(indices.size());
        for (IndexDeleteByQueryResponse indexResponse : indices.values()) {
            indexResponse.writeTo(out);
        }
    }

    enum JsonFields implements XContentParsable<DeleteByQueryResponse> {
        _indices {
            @Override
            public void apply(XContentParser parser, DeleteByQueryResponse response) throws IOException {
                XContentObject xIndices = parser.xContentObject();
                Set<String> indexNames = xIndices.keySet();
                for (String indexName : indexNames) {
                    IndexDeleteByQueryResponse deleteByQueryResponse = new IndexDeleteByQueryResponse();
                    XContentObject xContentObject = xIndices.getAsXContentObject(indexName);
                    xContentObject.put("_index", indexName);
                    deleteByQueryResponse.readFrom(xContentObject);
                    response.indices.put(indexName, deleteByQueryResponse);
                }
            }
        };

        static Map<String, XContentParsable<DeleteByQueryResponse>> fields = Maps.newLinkedHashMap();
        static {
            for (DeleteByQueryResponse.JsonFields field : values()) {
                fields.put(field.name(), field);
            }
        }
    }
    public void readFrom(XContentParser parser) throws IOException {
        XContentHelper.populate(parser, DeleteByQueryResponse.JsonFields.fields, this);
    }
}
