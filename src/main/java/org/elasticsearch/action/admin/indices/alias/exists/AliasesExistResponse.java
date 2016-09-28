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

package org.elasticsearch.action.admin.indices.alias.exists;

import com.google.common.collect.Maps;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.VersionedXContentParser;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParsable;

import java.io.IOException;
import java.util.Map;

/**
 */
public class AliasesExistResponse extends ActionResponse {

    private boolean exists;

    public AliasesExistResponse(boolean exists) {
        this.exists = exists;
    }

    AliasesExistResponse() {
    }

    public boolean exists() {
        return exists;
    }

    public boolean isExists() {
        return exists();
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

    enum JsonFields implements XContentParsable<AliasesExistResponse> {
        exists {
            @Override
            public void apply(VersionedXContentParser versionedXContentParser, AliasesExistResponse response) throws IOException {
                response.exists = versionedXContentParser.getParser().booleanValue();
            }
        };


        static Map<String, XContentParsable<AliasesExistResponse>> fields = Maps.newLinkedHashMap();

        static {
            for (JsonFields field : values()) {
                fields.put(field.name(), field);
            }
        }
    }

    @Override
    public void readFrom(VersionedXContentParser versionedXContentParser) throws IOException {
        XContentHelper.populate(versionedXContentParser, JsonFields.fields, this);
    }

}
