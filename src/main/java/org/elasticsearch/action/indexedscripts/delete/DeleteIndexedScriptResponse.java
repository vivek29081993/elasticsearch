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

package org.elasticsearch.action.indexedscripts.delete;

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
 * The response of the delete indexed script action.
 *
 * @see DeleteIndexedScriptRequest
 * @see org.elasticsearch.client.Client#deleteIndexedScript(DeleteIndexedScriptRequest)
 */
public class DeleteIndexedScriptResponse extends ActionResponse {

    private String index;
    private String id;
    private String type;
    private long version;
    private boolean found;

    public DeleteIndexedScriptResponse() {

    }

    public DeleteIndexedScriptResponse(String index, String type, String id, long version, boolean found) {
        this.index = index;
        this.id = id;
        this.type = type;
        this.version = version;
        this.found = found;
    }

    /**
     * The index the document was deleted from.
     */
    public String getIndex() {
        return this.index;
    }

    /**
     * The type of the document deleted.
     */
    public String getType() {
        return this.type;
    }

    /**
     * The id of the document deleted.
     */
    public String getId() {
        return this.id;
    }

    /**
     * The version of the delete operation.
     */
    public long getVersion() {
        return this.version;
    }

    /**
     * Returns <tt>true</tt> if a doc was found to delete.
     */
    public boolean isFound() {
        return found;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        index = in.readString();
        type = in.readString();
        id = in.readString();
        version = in.readLong();
        found = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(index);
        out.writeString(type);
        out.writeString(id);
        out.writeLong(version);
        out.writeBoolean(found);
    }

    enum JsonFields implements XContentParsable<DeleteIndexedScriptResponse> {
        _id {
            @Override
            public void apply(VersionedXContentParser versionedXContentParser, DeleteIndexedScriptResponse response) throws IOException {
                response.id = versionedXContentParser.getParser().text();
            }
        },
        _index {
            @Override
            public void apply(VersionedXContentParser versionedXContentParser, DeleteIndexedScriptResponse response) throws IOException {
                response.index = versionedXContentParser.getParser().text();
            }
        },
        _type {
            @Override
            public void apply(VersionedXContentParser versionedXContentParser, DeleteIndexedScriptResponse response) throws IOException {
                response.type = versionedXContentParser.getParser().text();
            }
        },
        found {
            @Override
            public void apply(VersionedXContentParser versionedXContentParser, DeleteIndexedScriptResponse response) throws IOException {
                response.found = versionedXContentParser.getParser().booleanValue();
            }
        },
        _version {
            @Override
            public void apply(VersionedXContentParser versionedXContentParser, DeleteIndexedScriptResponse response) throws IOException {
                response.version = versionedXContentParser.getParser().longValue();
            }
        };


        static Map<String, XContentParsable<DeleteIndexedScriptResponse>> fields = Maps.newLinkedHashMap();
        static {
            for (DeleteIndexedScriptResponse.JsonFields field : values()) {
                fields.put(field.name(), field);
            }
        }
    }

    @Override
    public void readFrom(VersionedXContentParser versionedXContentParser) throws IOException {
        XContentHelper.populate(versionedXContentParser, JsonFields.fields, this);
    }

}
