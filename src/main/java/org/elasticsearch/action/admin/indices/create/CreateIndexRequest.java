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

package org.elasticsearch.action.admin.indices.create;

import com.google.common.base.Charsets;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.ElasticsearchGenerationException;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.rest.RestRequest;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Maps.newHashMap;
import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.common.settings.ImmutableSettings.readSettingsFromStream;
import static org.elasticsearch.common.settings.ImmutableSettings.writeSettingsToStream;
import static org.elasticsearch.common.settings.ImmutableSettings.Builder.EMPTY_SETTINGS;
import static org.elasticsearch.common.xcontent.ToXContent.EMPTY_PARAMS;

/**
 * A request to create an index. Best created with {@link org.elasticsearch.client.Requests#createIndexRequest(String)}.
 * <p/>
 * <p>The index created can optionally be created with {@link #settings(org.elasticsearch.common.settings.Settings)}.
 *
 * @see org.elasticsearch.client.IndicesAdminClient#create(CreateIndexRequest)
 * @see org.elasticsearch.client.Requests#createIndexRequest(String)
 * @see CreateIndexResponse
 */
public class CreateIndexRequest extends AcknowledgedRequest<CreateIndexRequest> implements IndicesRequest {

    private String cause = "";

    private String index;

    private Settings settings = EMPTY_SETTINGS;

    private final Map<String, String> mappings = newHashMap();

    private final Set<Alias> aliases = Sets.newHashSet();

    private final Map<String, IndexMetaData.Custom> customs = newHashMap();

    CreateIndexRequest() {
    }

    /**
     * Constructs a new request to create an index that was triggered by a different request,
     * provided as an argument so that its headers and context can be copied to the new request.
     */
    public CreateIndexRequest(ActionRequest request) {
        super(request);
    }

    /**
     * Constructs a new request to create an index with the specified name.
     */
    public CreateIndexRequest(String index) {
        this(index, EMPTY_SETTINGS);
    }

    /**
     * Constructs a new request to create an index with the specified name and settings.
     */
    public CreateIndexRequest(String index, Settings settings) {
        this.index = index;
        this.settings = settings;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (index == null) {
            validationException = addValidationError("index is missing", validationException);
        }
        Integer number_of_primaries = settings.getAsInt(IndexMetaData.SETTING_NUMBER_OF_SHARDS, null);
        Integer number_of_replicas = settings.getAsInt(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, null);
        if (number_of_primaries != null && number_of_primaries <= 0) {
            validationException = addValidationError("index must have 1 or more primary shards", validationException);
        }
        if (number_of_replicas != null && number_of_replicas < 0) {
            validationException = addValidationError("index must have 0 or more replica shards", validationException);
        }
        return validationException;
    }

    @Override
    public String[] indices() {
        return new String[]{index};
    }

    @Override
    public IndicesOptions indicesOptions() {
        return IndicesOptions.strictSingleIndexNoExpandForbidClosed();
    }

    /**
     * The index name to create.
     */
    String index() {
        return index;
    }

    public CreateIndexRequest index(String index) {
        this.index = index;
        return this;
    }

    /**
     * The settings to create the index with.
     */
    Settings settings() {
        return settings;
    }

    /**
     * The cause for this index creation.
     */
    String cause() {
        return cause;
    }

    /**
     * A simplified version of settings that takes key value pairs settings.
     */
    public CreateIndexRequest settings(Object... settings) {
        this.settings = ImmutableSettings.builder().put(settings).build();
        return this;
    }

    /**
     * The settings to create the index with.
     */
    public CreateIndexRequest settings(Settings settings) {
        this.settings = settings;
        return this;
    }

    /**
     * The settings to create the index with.
     */
    public CreateIndexRequest settings(Settings.Builder settings) {
        this.settings = settings.build();
        return this;
    }

    /**
     * The settings to create the index with (either json/yaml/properties format)
     */
    public CreateIndexRequest settings(String source) {
        this.settings = ImmutableSettings.settingsBuilder().loadFromSource(source).build();
        return this;
    }

    /**
     * Allows to set the settings using a json builder.
     */
    public CreateIndexRequest settings(XContentBuilder builder) {
        try {
            settings(builder.string());
        } catch (IOException e) {
            throw new ElasticsearchGenerationException("Failed to generate json settings from builder", e);
        }
        return this;
    }

    /**
     * The settings to create the index with (either json/yaml/properties format)
     */
    @SuppressWarnings("unchecked")
    public CreateIndexRequest settings(Map source) {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
            builder.map(source);
            settings(builder.string());
        } catch (IOException e) {
            throw new ElasticsearchGenerationException("Failed to generate [" + source + "]", e);
        }
        return this;
    }

    /**
     * Adds mapping that will be added when the index gets created.
     *
     * @param type   The mapping type
     * @param source The mapping source
     */
    public CreateIndexRequest mapping(String type, String source) {
        if (mappings.containsKey(type)) {
            throw new IllegalStateException("mappings for type \"" + type + "\" were already defined");
        }
        mappings.put(type, source);
        return this;
    }

    /**
     * The cause for this index creation.
     */
    public CreateIndexRequest cause(String cause) {
        this.cause = cause;
        return this;
    }

    /**
     * Adds mapping that will be added when the index gets created.
     *
     * @param type   The mapping type
     * @param source The mapping source
     */
    public CreateIndexRequest mapping(String type, XContentBuilder source) {
        if (mappings.containsKey(type)) {
            throw new IllegalStateException("mappings for type \"" + type + "\" were already defined");
        }
        try {
            mappings.put(type, source.string());
        } catch (IOException e) {
            throw new ElasticsearchIllegalArgumentException("Failed to build json for mapping request", e);
        }
        return this;
    }

    /**
     * Adds mapping that will be added when the index gets created.
     *
     * @param type   The mapping type
     * @param source The mapping source
     */
    @SuppressWarnings("unchecked")
    public CreateIndexRequest mapping(String type, Map source) {
        if (mappings.containsKey(type)) {
            throw new IllegalStateException("mappings for type \"" + type + "\" were already defined");
        }
        // wrap it in a type map if its not
        if (source.size() != 1 || !source.containsKey(type)) {
            source = MapBuilder.<String, Object>newMapBuilder().put(type, source).map();
        }
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
            builder.map(source);
            return mapping(type, builder.string());
        } catch (IOException e) {
            throw new ElasticsearchGenerationException("Failed to generate [" + source + "]", e);
        }
    }

    /**
     * A specialized simplified mapping source method, takes the form of simple properties definition:
     * ("field1", "type=string,store=true").
     */
    public CreateIndexRequest mapping(String type, Object... source) {
        mapping(type, PutMappingRequest.buildFromSimplifiedDef(type, source));
        return this;
    }

    /**
     * Sets the aliases that will be associated with the index when it gets created
     */
    @SuppressWarnings("unchecked")
    public CreateIndexRequest aliases(Map source) {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.map(source);
            return aliases(builder.bytes());
        } catch (IOException e) {
            throw new ElasticsearchGenerationException("Failed to generate [" + source + "]", e);
        }
    }

    /**
     * Sets the aliases that will be associated with the index when it gets created
     */
    public CreateIndexRequest aliases(XContentBuilder source) {
        return aliases(source.bytes());
    }

    /**
     * Sets the aliases that will be associated with the index when it gets created
     */
    public CreateIndexRequest aliases(String source) {
        return aliases(new BytesArray(source));
    }

    /**
     * Sets the aliases that will be associated with the index when it gets created
     */
    public CreateIndexRequest aliases(BytesReference source) {
        try {
            XContentParser parser = XContentHelper.createParser(source);
            //move to the first alias
            parser.nextToken();
            while ((parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                alias(Alias.fromXContent(parser));
            }
            return this;
        } catch(IOException e) {
            throw new ElasticsearchParseException("Failed to parse aliases", e);
        }
    }

    /**
     * Adds an alias that will be associated with the index when it gets created
     */
    public CreateIndexRequest alias(Alias alias) {
        this.aliases.add(alias);
        return this;
    }

    /**
     * Sets the settings and mappings as a single source.
     */
    public CreateIndexRequest source(String source) {
        return source(source.getBytes(Charsets.UTF_8));
    }

    /**
     * Sets the settings and mappings as a single source.
     */
    public CreateIndexRequest source(XContentBuilder source) {
        return source(source.bytes());
    }

    /**
     * Sets the settings and mappings as a single source.
     */
    public CreateIndexRequest source(byte[] source) {
        return source(source, 0, source.length);
    }

    /**
     * Sets the settings and mappings as a single source.
     */
    public CreateIndexRequest source(byte[] source, int offset, int length) {
        return source(new BytesArray(source, offset, length));
    }

    /**
     * Sets the settings and mappings as a single source.
     */
    public CreateIndexRequest source(BytesReference source) {
        XContentType xContentType = XContentFactory.xContentType(source);
        if (xContentType != null) {
            try {
                source(XContentFactory.xContent(xContentType).createParser(source).mapAndClose());
            } catch (IOException e) {
                throw new ElasticsearchParseException("failed to parse source for create index", e);
            }
        } else {
            settings(new String(source.toBytes(), Charsets.UTF_8));
        }
        return this;
    }

    /**
     * Sets the settings and mappings as a single source.
     */
    @SuppressWarnings("unchecked")
    public CreateIndexRequest source(Map<String, Object> source) {
        boolean found = false;
        for (Map.Entry<String, Object> entry : source.entrySet()) {
            String name = entry.getKey();
            if (name.equals("settings")) {
                found = true;
                settings((Map<String, Object>) entry.getValue());
            } else if (name.equals("mappings")) {
                found = true;
                Map<String, Object> mappings = (Map<String, Object>) entry.getValue();
                for (Map.Entry<String, Object> entry1 : mappings.entrySet()) {
                    mapping(entry1.getKey(), (Map<String, Object>) entry1.getValue());
                }
            } else if (name.equals("aliases")) {
                found = true;
                aliases((Map<String, Object>) entry.getValue());
            } else {
                // maybe custom?
                IndexMetaData.Custom.Factory factory = IndexMetaData.lookupFactory(name);
                if (factory != null) {
                    found = true;
                    try {
                        customs.put(name, factory.fromMap((Map<String, Object>) entry.getValue()));
                    } catch (IOException e) {
                        throw new ElasticsearchParseException("failed to parse custom metadata for [" + name + "]");
                    }
                }
            }
        }
        if (!found) {
            // the top level are settings, use them
            settings(source);
        }
        return this;
    }

    Map<String, String> mappings() {
        return this.mappings;
    }

    Set<Alias> aliases() {
        return this.aliases;
    }

    /**
     * Adds custom metadata to the index to be created.
     */
    public CreateIndexRequest custom(IndexMetaData.Custom custom) {
        customs.put(custom.type(), custom);
        return this;
    }

    Map<String, IndexMetaData.Custom> customs() {
        return this.customs;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        cause = in.readString();
        index = in.readString();
        settings = readSettingsFromStream(in);
        readTimeout(in);
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            mappings.put(in.readString(), in.readString());
        }
        int customSize = in.readVInt();
        for (int i = 0; i < customSize; i++) {
            String type = in.readString();
            IndexMetaData.Custom customIndexMetaData = IndexMetaData.lookupFactorySafe(type).readFrom(in);
            customs.put(type, customIndexMetaData);
        }
        if (in.getVersion().onOrAfter(Version.V_1_1_0)) {
            int aliasesSize = in.readVInt();
            for (int i = 0; i < aliasesSize; i++) {
                aliases.add(Alias.read(in));
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(cause);
        out.writeString(index);
        writeSettingsToStream(settings, out);
        writeTimeout(out);
        out.writeVInt(mappings.size());
        for (Map.Entry<String, String> entry : mappings.entrySet()) {
            out.writeString(entry.getKey());
            out.writeString(entry.getValue());
        }
        out.writeVInt(customs.size());
        for (Map.Entry<String, IndexMetaData.Custom> entry : customs.entrySet()) {
            out.writeString(entry.getKey());
            IndexMetaData.lookupFactorySafe(entry.getKey()).writeTo(entry.getValue(), out);
        }
        if (out.getVersion().onOrAfter(Version.V_1_1_0)) {
            out.writeVInt(aliases.size());
            for (Alias alias : aliases) {
                alias.writeTo(out);
            }
        }
    }


    @Override
    public String getEndPoint() {
        return "/" + this.index;
    }

    @Override
    public Map<String, String> getParams() {
        return super.getParams();
    }

    @Override
    public RestRequest.Method getMethod() {
        return RestRequest.Method.PUT;
    }

    @Override
    public HttpEntity getBulkEntity() throws IOException {
        return super.getBulkEntity();
    }

    @Override
    public HttpEntity getEntity() throws IOException {
        Map<String, Object> payload = toMap();
        String json = XContentHelper.convertToJson(payload, false);
        return new NStringEntity(json, StandardCharsets.UTF_8);
    }

    public Map<String, Object> toMap() throws IOException {
        Map<String, Object> mappings = Maps.newLinkedHashMap();
        for (String json : this.mappings.values()) {
            Map<String, Object> mapping = XContentHelper.fromJson(json);
            mappings.putAll(mapping);
        }

        Map<String, Object> aliases = Maps.newLinkedHashMap();
        for (Alias alias : this.aliases) {
            aliases.putAll(alias.asMap());
        }
        Map<String, Object> custom = Maps.newLinkedHashMap();
        for (Map.Entry<String, IndexMetaData.Custom> entry : this.customs.entrySet()) {
            IndexMetaData.Custom.Factory<IndexMetaData.Custom> customFactory;
            IndexMetaData.Custom customIndexMetaData = entry.getValue();
            customFactory = IndexMetaData.lookupFactory(customIndexMetaData.type());
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            builder.startObject(customIndexMetaData.type());
            customFactory.toXContent(customIndexMetaData, builder, EMPTY_PARAMS);
            builder.endObject();
            builder.endObject();
            Map<String, Object> customMap = XContentHelper.fromJson(builder.string());
            custom.put(entry.getKey(), customMap);
        }


        return new MapBuilder<String, Object>()
                .putIf("settings", settings.getAsMap(), settings != EMPTY_SETTINGS)
                .putIf("mappings", mappings, !mappings.isEmpty())
                .putAllIf(custom, !custom.isEmpty())
                .putIfNotNull("aliases", aliases).map();
    }

}