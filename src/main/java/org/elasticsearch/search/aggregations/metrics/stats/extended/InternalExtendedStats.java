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
package org.elasticsearch.search.aggregations.metrics.stats.extended;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.aggregations.CommonJsonField;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.stats.InternalStats;

import java.io.IOException;

/**
*
*/
public class InternalExtendedStats extends InternalStats implements ExtendedStats {

    public final static Type TYPE = new Type("extended_stats", "estats");

    public final static AggregationStreams.Stream STREAM = new AggregationStreams.Stream() {
        @Override
        public InternalExtendedStats readResult(StreamInput in) throws IOException {
            InternalExtendedStats result = new InternalExtendedStats();
            result.readFrom(in);
            return result;
        }

        @Override
        public InternalAggregation readResult(XContentObject in) throws IOException {
            InternalExtendedStats result = new InternalExtendedStats();
            result.readFrom(in);
            return result;
        }
    };

    public void readFrom(XContentObject in) throws IOException {
        this.name = in.get(CommonJsonField._name);
        XContentHelper.populate(in, Metrics.values(), this);
    }

    public static void registerStreams() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
        AggregationStreams.registerStream(STREAM, new BytesArray(TYPE.name())); // added for es 5.0
    }

    enum Metrics implements XContentObjectParseable<InternalExtendedStats> {

        count {
            @Override
            public void apply(XContentObject source, InternalExtendedStats object) throws IOException {
                object.count = source.getAsInt(this, 0);
            }
        }, sum {
            @Override
            public void apply(XContentObject source, InternalExtendedStats object) throws IOException {
                object.sum = source.getAsDouble(this, 0D);
            }
        }, min {
            @Override
            public void apply(XContentObject source, InternalExtendedStats object) throws IOException {
                object.min = source.getAsDouble(this, 0D);
            }
        }, max {
            @Override
            public void apply(XContentObject source, InternalExtendedStats object) throws IOException {
                object.max = source.getAsDouble(this, 0D);
            }
        }, avg {
            @Override
            public void apply(XContentObject source, InternalExtendedStats object) throws IOException {
                // derived
            }
        }, sum_of_squares {
            @Override
            public void apply(XContentObject source, InternalExtendedStats object) throws IOException {
                object.sumOfSqrs = source.getAsDouble(this, 0D);
            }
        }, variance {
            @Override
            public void apply(XContentObject source, InternalExtendedStats object) throws IOException {
                // derived
            }
        }, std_deviation {
            @Override
            public void apply(XContentObject source, InternalExtendedStats object) throws IOException {
                // derived
            }
        }, std_upper {
            @Override
            public void apply(XContentObject source, InternalExtendedStats object) throws IOException {
                // derived
            }
        }, std_lower {
            @Override
            public void apply(XContentObject source, InternalExtendedStats object) throws IOException {
                // derived
            }
        };

        public static Metrics resolve(String name) {
            return Metrics.valueOf(name);
        }
    }

    private double sumOfSqrs;

    InternalExtendedStats() {} // for serialization

    public InternalExtendedStats(String name, long count, double sum, double min, double max, double sumOfSqrs) {
        super(name, count, sum, min, max);
        this.sumOfSqrs = sumOfSqrs;
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    public double value(String name) {
        if ("sum_of_squares".equals(name)) {
            return sumOfSqrs;
        }
        if ("variance".equals(name)) {
            return getVariance();
        }
        if ("std_deviation".equals(name)) {
            return getStdDeviation();
        }
        return super.value(name);
    }

    @Override
    public double getSumOfSquares() {
        return sumOfSqrs;
    }

    @Override
    public double getVariance() {
        return (sumOfSqrs - ((sum * sum) / count)) / count;
    }

    @Override
    public double getStdDeviation() {
        return Math.sqrt(getVariance());
    }

    @Override
    public InternalExtendedStats reduce(ReduceContext reduceContext) {
        double sumOfSqrs = 0;
        for (InternalAggregation aggregation : reduceContext.aggregations()) {
            InternalExtendedStats stats = (InternalExtendedStats) aggregation;
            sumOfSqrs += stats.getSumOfSquares();
        }
        final InternalStats stats = super.reduce(reduceContext);
        return new InternalExtendedStats(name, stats.getCount(), stats.getSum(), stats.getMin(), stats.getMax(), sumOfSqrs);
    }

    @Override
    public void readOtherStatsFrom(StreamInput in) throws IOException {
        sumOfSqrs = in.readDouble();
    }

    @Override
    protected void writeOtherStatsTo(StreamOutput out) throws IOException {
        out.writeDouble(sumOfSqrs);
    }

    static class Fields {
        public static final XContentBuilderString SUM_OF_SQRS = new XContentBuilderString("sum_of_squares");
        public static final XContentBuilderString SUM_OF_SQRS_AS_STRING = new XContentBuilderString("sum_of_squares_as_string");
        public static final XContentBuilderString VARIANCE = new XContentBuilderString("variance");
        public static final XContentBuilderString VARIANCE_AS_STRING = new XContentBuilderString("variance_as_string");
        public static final XContentBuilderString STD_DEVIATION = new XContentBuilderString("std_deviation");
        public static final XContentBuilderString STD_DEVIATION_AS_STRING = new XContentBuilderString("std_deviation_as_string");
    }

    @Override
    protected XContentBuilder otherStatsToXCotent(XContentBuilder builder, Params params) throws IOException {
        builder.field(Fields.SUM_OF_SQRS, count != 0 ? sumOfSqrs : null);
        builder.field(Fields.VARIANCE, count != 0 ? getVariance() : null);
        builder.field(Fields.STD_DEVIATION, count != 0 ? getStdDeviation() : null);
        if (count != 0 && valueFormatter != null) {
            builder.field(Fields.SUM_OF_SQRS_AS_STRING, valueFormatter.format(sumOfSqrs));
            builder.field(Fields.VARIANCE_AS_STRING, valueFormatter.format(getVariance()));
            builder.field(Fields.STD_DEVIATION_AS_STRING, valueFormatter.format(getStdDeviation()));
        }
        return builder;
    }

}
