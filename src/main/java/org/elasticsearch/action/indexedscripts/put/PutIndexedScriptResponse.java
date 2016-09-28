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

package org.elasticsearch.action.indexedscripts.put;

import com.google.common.collect.Maps;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.VersionedXContentParser;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParsable;
import org.elasticsearch.script.ScriptService;

import java.io.IOException;
import java.util.Map;

/**
 * A response of an index operation,
 *
 * @see PutIndexedScriptRequest
 * @see org.elasticsearch.client.Client#putIndexedScript(PutIndexedScriptRequest)
 */
public class PutIndexedScriptResponse extends ActionResponse {

    private String index;
    private String id;
    private String scriptLang;
    private long version;
    private boolean created;

    public PutIndexedScriptResponse() {
    }

    public PutIndexedScriptResponse(String type, String id, long version, boolean created) {
        this.index = ScriptService.SCRIPT_INDEX;
        this.id = id;
        this.scriptLang = type;
        this.version = version;
        this.created = created;
    }

    /**
     * The index the document was indexed into.
     */
    public String getIndex() {
        return this.index;
    }

    /**
     * The type of the document indexed.
     */
    public String getScriptLang() {
        return this.scriptLang;
    }

    /**
     * The id of the document indexed.
     */
    public String getId() {
        return this.id;
    }

    /**
     * Returns the current version of the doc indexed.
     */
    public long getVersion() {
        return this.version;
    }

    /**
     * Returns true if the document was created, false if updated.
     */
    public boolean isCreated() {
        return this.created;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        index = ScriptService.SCRIPT_INDEX;
        scriptLang = in.readString();
        id = in.readString();
        version = in.readLong();
        created = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(scriptLang);
        out.writeString(id);
        out.writeLong(version);
        out.writeBoolean(created);
    }

    enum JsonFields implements XContentParsable<PutIndexedScriptResponse> {
        _id {
            @Override
            public void apply(VersionedXContentParser versionedXContentParser, PutIndexedScriptResponse response) throws IOException {
                response.id = versionedXContentParser.getParser().text();
            }
        },
        created {
            @Override
            public void apply(VersionedXContentParser versionedXContentParser, PutIndexedScriptResponse response) throws IOException {
                response.created = versionedXContentParser.getParser().booleanValue();
            }
        },
        _version {
            @Override
            public void apply(VersionedXContentParser versionedXContentParser, PutIndexedScriptResponse response) throws IOException {
                response.version = versionedXContentParser.getParser().longValue();
            }
        };


        static Map<String, XContentParsable<PutIndexedScriptResponse>> fields = Maps.newLinkedHashMap();
        static {
            for (PutIndexedScriptResponse.JsonFields field : values()) {
                fields.put(field.name(), field);
            }
        }
    }

    @Override
    public void readFrom(VersionedXContentParser versionedXContentParser) throws IOException {
        XContentHelper.populate(versionedXContentParser, JsonFields.fields, this);
    }
}
