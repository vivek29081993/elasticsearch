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

import com.google.common.collect.Maps;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.indexedscripts.delete.DeleteIndexedScriptResponse;
import org.elasticsearch.action.support.broadcast.BroadcastOperationResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParsable;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class ExistsResponse extends BroadcastOperationResponse {

    private boolean exists = false;

    ExistsResponse() {

    }

    ExistsResponse(boolean exists, int totalShards, int successfulShards, int failedShards, List<ShardOperationFailedException> shardFailures) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.exists = exists;
    }

    /**
     * Whether the documents matching the query provided exists
     */
    public boolean exists() {
        return exists;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        exists = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(exists);
    }

    enum JsonFields implements XContentParsable<ExistsResponse> {
        exists {
            @Override
            public void apply(XContentParser parser, ExistsResponse response) throws IOException {
                response.exists = parser.booleanValue();
            }
        };


        static Map<String, XContentParsable<ExistsResponse>> fields = Maps.newLinkedHashMap();

        static {
            for (ExistsResponse.JsonFields field : values()) {
                fields.put(field.name(), field);
            }
        }
    }

    @Override
    public void readFrom(XContentParser parser) throws IOException {
        XContentHelper.populate(parser, JsonFields.fields, this);
    }

}
