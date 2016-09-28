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

package org.elasticsearch.action.count;

import com.google.common.collect.Maps;
import org.elasticsearch.Version;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastOperationResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * The response of the count action.
 */
public class CountResponse extends BroadcastOperationResponse implements FromXContent {

    private boolean terminatedEarly;
    private long count;

    CountResponse() {

    }

    CountResponse(long count, boolean hasTerminatedEarly, int totalShards, int successfulShards, int failedShards, List<ShardOperationFailedException> shardFailures) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.count = count;
        this.terminatedEarly = hasTerminatedEarly;
    }

    /**
     * The count of documents matching the query provided.
     */
    public long getCount() {
        return count;
    }

    /**
     * True if the request has been terminated early due to enough count
     */
    public boolean terminatedEarly() {
        return this.terminatedEarly;
    }

    public RestStatus status() {
        if (getFailedShards() == 0) {
            if (getSuccessfulShards() == 0 && getTotalShards() > 0) {
                return RestStatus.SERVICE_UNAVAILABLE;
            }
            return RestStatus.OK;
        }
        // if total failure, bubble up the status code to the response level
        if (getSuccessfulShards() == 0 && getTotalShards() > 0) {
            RestStatus status = RestStatus.OK;
            for (ShardOperationFailedException shardFailure : getShardFailures()) {
                RestStatus shardStatus = shardFailure.status();
                if (shardStatus.getStatus() >= status.getStatus()) {
                    status = shardStatus;
                }
            }
            return status;
        }
        return RestStatus.OK;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        count = in.readVLong();
        if (in.getVersion().onOrAfter(Version.V_1_4_0_Beta1)) {
            terminatedEarly = in.readBoolean();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVLong(count);
        if (out.getVersion().onOrAfter(Version.V_1_4_0_Beta1)) {
            out.writeBoolean(terminatedEarly);
        }
    }

    enum JsonFields implements XContentParsable<CountResponse> {
        count {
            @Override
            public void apply(VersionedXContentParser versionedXContentParser, CountResponse response) throws IOException {
                response.count = versionedXContentParser.getParser().longValue();
            }
        },
        _shards {
            @Override
            public void apply(VersionedXContentParser versionedXContentParser, CountResponse response) throws IOException {
                //todo
            }
        };

        static Map<String, XContentParsable<CountResponse>> fields = Maps.newLinkedHashMap();
        static {
            for (CountResponse.JsonFields field : values()) {
                fields.put(field.name(), field);
            }
        }
    }

    @Override
    public void readFrom(VersionedXContentParser versionedXContentParser) throws IOException {
        XContentHelper.populate(versionedXContentParser, JsonFields.fields, this);
    }

}
