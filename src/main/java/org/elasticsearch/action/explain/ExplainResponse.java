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

package org.elasticsearch.action.explain;

import com.google.common.collect.Maps;
import org.apache.lucene.search.Explanation;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentObject;
import org.elasticsearch.common.xcontent.XContentParsable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.get.GetResult;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.lucene.Lucene.readExplanation;
import static org.elasticsearch.common.lucene.Lucene.writeExplanation;

/**
 * Response containing the score explanation.
 */
public class ExplainResponse extends ActionResponse {

    private String index;
    private String type;
    private String id;
    private boolean exists;
    private Explanation explanation;
    private GetResult getResult;

    ExplainResponse() {
    }

    public ExplainResponse(String index, String type, String id, boolean exists) {
        this.index = index;
        this.type = type;
        this.id = id;
        this.exists = exists;
    }

    public ExplainResponse(String index, String type, String id, boolean exists, Explanation explanation) {
        this(index, type, id, exists);
        this.explanation = explanation;
    }

    public ExplainResponse(String index, String type, String id, boolean exists, Explanation explanation, GetResult getResult) {
        this(index, type, id, exists, explanation);
        this.getResult = getResult;
    }

    public String getIndex() {
        return index;
    }

    public String getType() {
        return type;
    }

    public String getId() {
        return id;
    }

    public Explanation getExplanation() {
        return explanation;
    }

    public boolean isMatch() {
        return explanation != null && explanation.isMatch();
    }

    public boolean hasExplanation() {
        return explanation != null;
    }

    public boolean isExists() {
        return exists;
    }

    public GetResult getGetResult() {
        return getResult;
    }

    enum JsonFields implements XContentParsable<ExplainResponse> {
        _index {
            @Override
            public void apply(XContentParser parser, ExplainResponse response) throws IOException {
                response.index = parser.text();
            }
        },
        _type {
            @Override
            public void apply(XContentParser parser, ExplainResponse response) throws IOException {
                response.type = parser.text();
            }
        },
        _id {
            @Override
            public void apply(XContentParser parser, ExplainResponse response) throws IOException {
                response.id = parser.text();
            }
        },
        matched {
            @Override
            public void apply(XContentParser parser, ExplainResponse response) throws IOException {
                response.exists = parser.booleanValue();
            }
        },
        explanation {
            @Override
            public void apply(XContentParser parser, ExplainResponse response) throws IOException {
                XContentObject source = parser.xContentObject();
                response.explanation = getExplanation(source);
            }

            private Explanation getExplanation(XContentObject source) {
                Explanation explanation = new Explanation();
                explanation.setValue(source.getAsFloat("value"));
                explanation.setDescription(source.get("description"));
                List<XContentObject> xDetails = source.getAsXContentObjectsOrEmpty("details");
                for (XContentObject xDetail : xDetails) {
                    explanation.addDetail(getExplanation(xDetail));
                }
                return explanation;
            }
        };


        static Map<String, XContentParsable<ExplainResponse>> fields = Maps.newLinkedHashMap();

        static {
            for (JsonFields field : values()) {
                fields.put(field.name(), field);
            }
        }
    }

    @Override
    public void readFrom(XContentParser parser) throws IOException {
        XContentHelper.populate(parser, JsonFields.fields, this);
    }

    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        if (in.getVersion().onOrAfter(Version.V_1_4_0_Beta1)) {
            index = in.readString();
            type = in.readString();
            id = in.readString();
        }
        exists = in.readBoolean();
        if (in.readBoolean()) {
            explanation = readExplanation(in);
        }
        if (in.readBoolean()) {
            getResult = GetResult.readGetResult(in);
        }
    }

    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getVersion().onOrAfter(Version.V_1_4_0_Beta1)) {
            out.writeString(index);
            out.writeString(type);
            out.writeString(id);
        }
        out.writeBoolean(exists);
        if (explanation == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            writeExplanation(out, explanation);
        }
        if (getResult == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            getResult.writeTo(out);
        }
    }

}
