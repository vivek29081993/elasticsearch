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

package org.elasticsearch.action.get;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.*;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class MultiGetResponse extends ActionResponse implements Iterable<MultiGetItemResponse>, ToXContent {

    /**
     * Represents a failure.
     */
    public static class Failure implements Streamable {
        private String index;
        private String type;
        private String id;
        private String message;

        Failure() {

        }

        public Failure(String index, String type, String id, String message) {
            this.index = index;
            this.type = type;
            this.id = id;
            this.message = message;
        }

        /**
         * The index name of the action.
         */
        public String getIndex() {
            return this.index;
        }

        /**
         * The type of the action.
         */
        public String getType() {
            return type;
        }

        /**
         * The id of the action.
         */
        public String getId() {
            return id;
        }

        /**
         * The failure message.
         */
        public String getMessage() {
            return this.message;
        }

        public static Failure readFailure(StreamInput in) throws IOException {
            Failure failure = new Failure();
            failure.readFrom(in);
            return failure;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            index = in.readString();
            type = in.readOptionalString();
            id = in.readString();
            message = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(index);
            out.writeOptionalString(type);
            out.writeString(id);
            out.writeString(message);
        }

        enum JsonFields implements XContentObjectParseable<Failure> {
            _index {
                @Override
                public void apply(XContentObject source, Failure object) throws IOException {
                    object.index = source.get(this);
                }
            },
            _type {
                @Override
                public void apply(XContentObject source, Failure object) throws IOException {
                    object.type = source.get(this);
                }
            },
            _id {
                @Override
                public void apply(XContentObject source, Failure object) throws IOException {
                    object.id = source.get(this);
                }
            },
            error {
                @Override
                public void apply(XContentObject source, Failure object) throws IOException {
                    object.message = source.get(this);
                }
            };

            static Map<String, XContentObjectParseable<Failure>> fields = Maps.newLinkedHashMap();
            static {
                for (JsonFields field : values()) {
                    fields.put(field.name(), field);
                }
            }

            }

        private static Failure readFailure(XContentObject in) throws IOException {
            Failure failure = new Failure();
            XContentHelper.populate(in, JsonFields.values(), failure);
            return failure;
        }
    }

    private MultiGetItemResponse[] responses;

    MultiGetResponse() {
    }

    public MultiGetResponse(MultiGetItemResponse[] responses) {
        this.responses = responses;
    }

    public MultiGetItemResponse[] getResponses() {
        return this.responses;
    }

    @Override
    public Iterator<MultiGetItemResponse> iterator() {
        return Iterators.forArray(responses);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray(Fields.DOCS);
        for (MultiGetItemResponse response : responses) {
            if (response.isFailed()) {
                builder.startObject();
                Failure failure = response.getFailure();
                builder.field(Fields._INDEX, failure.getIndex());
                builder.field(Fields._TYPE, failure.getType());
                builder.field(Fields._ID, failure.getId());
                builder.field(Fields.ERROR, failure.getMessage());
                builder.endObject();
            } else {
                GetResponse getResponse = response.getResponse();
                builder.startObject();
                getResponse.toXContent(builder, params);
                builder.endObject();
            }
        }
        builder.endArray();
        return builder;
    }

    static final class Fields {
        static final XContentBuilderString DOCS = new XContentBuilderString("docs");
        static final XContentBuilderString _INDEX = new XContentBuilderString("_index");
        static final XContentBuilderString _TYPE = new XContentBuilderString("_type");
        static final XContentBuilderString _ID = new XContentBuilderString("_id");
        static final XContentBuilderString ERROR = new XContentBuilderString("error");
    }

    enum JsonFields implements XContentParsable<MultiGetResponse> {
        docs {
            @Override
            public void apply(VersionedXContentParser versionedXContentParser, MultiGetResponse response) throws IOException {
                List<MultiGetItemResponse> items = Lists.newArrayList();
                for (versionedXContentParser.getParser().nextToken(); versionedXContentParser.getParser().currentToken() != XContentParser.Token.END_ARRAY; versionedXContentParser.getParser().nextToken()) {
                    Failure failure = null;
                    GetResponse getResponse = null;
                    XContentObject xContentObject = versionedXContentParser.getParser().xContentObject();
                    if (xContentObject.containsKey(Failure.JsonFields.error)) {
                        failure = Failure.readFailure(xContentObject);
                    }
                    else {
                        getResponse = GetResponse.readGetResponse(xContentObject);
                    }
                    MultiGetItemResponse item = new MultiGetItemResponse(getResponse, failure);
                    items.add(item);
                }
                response.responses = items.toArray(new MultiGetItemResponse[items.size()]);
            }
        };

        static Map<String, XContentParsable<MultiGetResponse>> fields = Maps.newLinkedHashMap();
        static {
            for (MultiGetResponse.JsonFields field : values()) {
                fields.put(field.name(), field);
            }
        }
    }

    @Override
    public void readFrom(VersionedXContentParser versionedXContentParser) throws IOException {
        XContentHelper.populate(versionedXContentParser, JsonFields.fields, this);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        responses = new MultiGetItemResponse[in.readVInt()];
        for (int i = 0; i < responses.length; i++) {
            responses[i] = MultiGetItemResponse.readItemResponse(in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(responses.length);
        for (MultiGetItemResponse response : responses) {
            response.writeTo(out);
        }
    }
}