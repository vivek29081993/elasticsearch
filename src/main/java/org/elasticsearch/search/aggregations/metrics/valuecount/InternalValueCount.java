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
package org.elasticsearch.search.aggregations.metrics.valuecount;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentObject;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.CommonJsonField;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;

import java.io.IOException;

/**
 * An internal implementation of {@link ValueCount}.
 */
public class InternalValueCount extends InternalNumericMetricsAggregation.SingleValue implements ValueCount {

    public static final Type TYPE = new Type("value_count", "vcount");

    private static final AggregationStreams.Stream STREAM = new AggregationStreams.Stream() {
        @Override
        public InternalValueCount readResult(StreamInput in) throws IOException {
            InternalValueCount count = new InternalValueCount();
            count.readFrom(in);
            return count;
        }

//        @Override
        public InternalValueCount readResult(XContentObject in) throws IOException {
            InternalValueCount count = new InternalValueCount();
            count.readFrom(in);
            return count;

        }
    };

    @Override
    public void readFrom(XContentObject in) throws IOException {
        name = in.get(CommonJsonField._name);
        value = in.getAsLong(CommonJsonField.value);
    }

    public static void registerStreams() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
        AggregationStreams.registerStream(STREAM, new BytesArray(TYPE.name())); // added for es 5.0
    }

    private long value;

    InternalValueCount() {} // for serialization

    public InternalValueCount(String name, long value) {
        super(name);
        this.value = value;
    }

    @Override
    public long getValue() {
        return value;
    }

    @Override
    public double value() {
        return value;
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    public InternalAggregation reduce(ReduceContext reduceContext) {
        long valueCount = 0;
        for (InternalAggregation aggregation : reduceContext.aggregations()) {
            valueCount += ((InternalValueCount) aggregation).value;
        }
        return new InternalValueCount(name, valueCount);
    }

    public void readFrom(Settings in) {
        name = in.get(CommonJsonField._name.name());
        value = in.getAsLong(CommonJsonField.value.name(), 0L);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        name = in.readString();
        value = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeVLong(value);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        return builder.field(CommonFields.VALUE, value);
    }

    @Override
    public String toString() {
        return "count[" + value + "]";
    }
}