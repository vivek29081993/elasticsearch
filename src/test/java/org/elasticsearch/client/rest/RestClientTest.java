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
package org.elasticsearch.client.rest;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.count.CountRequest;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.deletebyquery.IndexDeleteByQueryResponse;
import org.elasticsearch.action.exists.ExistsResponse;
import org.elasticsearch.action.explain.ExplainRequestBuilder;
import org.elasticsearch.action.explain.ExplainResponse;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.indexedscripts.delete.DeleteIndexedScriptRequest;
import org.elasticsearch.action.indexedscripts.delete.DeleteIndexedScriptResponse;
import org.elasticsearch.action.indexedscripts.get.GetIndexedScriptRequest;
import org.elasticsearch.action.indexedscripts.get.GetIndexedScriptResponse;
import org.elasticsearch.action.indexedscripts.put.PutIndexedScriptResponse;
import org.elasticsearch.action.search.*;
import org.elasticsearch.action.support.QuerySourceBuilder;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.children.Children;
import org.elasticsearch.search.aggregations.bucket.children.ChildrenBuilder;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.filters.Filters;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoHashGrid;
import org.elasticsearch.search.aggregations.bucket.global.Global;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.missing.Missing;
import org.elasticsearch.search.aggregations.bucket.nested.Nested;
import org.elasticsearch.search.aggregations.bucket.nested.NestedBuilder;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.bucket.range.geodistance.GeoDistance;
import org.elasticsearch.search.aggregations.bucket.range.ipv4.IPv4Range;
import org.elasticsearch.search.aggregations.bucket.significant.SignificantTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality;
import org.elasticsearch.search.aggregations.metrics.geobounds.GeoBounds;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.min.Min;
import org.elasticsearch.search.aggregations.metrics.percentiles.Percentile;
import org.elasticsearch.search.aggregations.metrics.percentiles.PercentileRanks;
import org.elasticsearch.search.aggregations.metrics.percentiles.Percentiles;
import org.elasticsearch.search.aggregations.metrics.scripted.ScriptedMetric;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;
import org.elasticsearch.search.aggregations.metrics.stats.extended.ExtendedStats;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHits;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCount;
import org.elasticsearch.search.sort.SortOrder;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;

/**
 * @author Brandon Kearby
 *         September 08, 2016
 */
public class RestClientTest {
    public static final String TEST_INDEX = "test";
    public static final String POSTS_INDEX = "posts";
    public static final String POST_TYPE = "post";
    public static final String COMMENT_TYPE = "comment";
    public static final String STATS_TYPE = "stats";
    private RestClient client;

    enum Color {
        red,
        green,
        blue,
        orange,
        black,
        white,
        purple,
        brown,
        silver,
        gold
    }

    enum Genre {
        comedy,
        horror,
        drama,
        action,
        documentary
    }


    @Before
    public void setUp() {
        this.client = new RestClient("localhost");
    }

    @Test
    public void testGet() throws ExecutionException, InterruptedException {
        IndexResponse indexResponse = indexDocument();
        String id = indexResponse.getId();

        GetResponse getResponse = getDocument(id);
        assertEquals(id, getResponse.getId());
    }

    private GetResponse getDocument(String id) throws InterruptedException, ExecutionException {
        GetRequest getRequest = new GetRequest(TEST_INDEX, STATS_TYPE, id);
        ActionFuture<GetResponse> getResponseActionFuture = this.client.get(getRequest);
        return getResponseActionFuture.get();
    }

    @Test
    public void testIndex() throws ExecutionException, InterruptedException {
        String id = UUID.randomUUID().toString();
        IndexRequest request = new IndexRequest(TEST_INDEX, STATS_TYPE, id);
        Map<String, Object> source = Maps.newHashMap();
        source.put("datePretty", "2016-02-28T05:30:00+05:30");
        request.source(source);
        IndexResponse indexResponse = this.client.index(request).get();
        assertEquals(id, indexResponse.getId());
        assertEquals(TEST_INDEX, indexResponse.getIndex());
        assertEquals(STATS_TYPE, indexResponse.getType());

        GetResponse getResponse = getDocument(id);
        assertEquals(id, getResponse.getId());
        assertEquals(source.get("datePretty"), getResponse.getSourceAsMap().get("datePretty"));
    }

    @Test
    public void testDelete() throws ExecutionException, InterruptedException {
        // add test doc
        IndexResponse indexResponse = indexDocument();

        // delete the test doc
        DeleteRequest deleteRequest = new DeleteRequest(indexResponse.getIndex(), indexResponse.getType(), indexResponse.getId());
        DeleteResponse deleteResponse = client.delete(deleteRequest).get();

        assertEquals(indexResponse.getId(), deleteResponse.getId());
        assertEquals(TEST_INDEX, deleteResponse.getIndex());
        assertEquals(STATS_TYPE, deleteResponse.getType());
        assertTrue("Document should be found", deleteResponse.isFound());

        GetResponse getResponse = getDocument(indexResponse.getId());
        assertFalse("Document should not exist", getResponse.isExists());
    }

    @Test
    public void testUpdate() throws ExecutionException, InterruptedException {
        IndexResponse indexResponse = indexDocument();
        GetResponse document = getDocument(indexResponse.getId());
        UpdateRequest updateRequest = new UpdateRequest(document.getIndex(), document.getType(), document.getId());
        Map<String, Object> source = document.getSourceAsMap();
        source.put("datePretty", "2017-02-28T05:30:00+05:30");
        updateRequest.doc(source);
        client.update(updateRequest);
        GetResponse updatedDocument = getDocument(document.getId());
        assertEquals(source.get("datePretty"), updatedDocument.getSourceAsMap().get("datePretty"));
    }

    @Test
    public void testBulkIndex() throws ExecutionException, InterruptedException {
        BulkRequest request = new BulkRequest();
        int count = 10000;
        for (int i = 0; i < count; i++) {
            request.add(newIndexRequest());
        }
        BulkResponse bulkItemResponse = client.bulk(request).get();
        assertEquals(count, bulkItemResponse.getItems().length);
        for (BulkItemResponse itemResponse : bulkItemResponse.getItems()) {
            assertFalse("Item failed to index", itemResponse.isFailed());
        }
    }

    @Test
    public void testBulkWithErrors() throws ExecutionException, InterruptedException {
        BulkRequest request = new BulkRequest();
        int count = 10000;
        for (int i = 0; i < count; i++) {
            IndexRequest indexRequest = newIndexRequest();
            request.add(indexRequest);
            DeleteRequest deleteRequest = new DeleteRequest(indexRequest.index(), indexRequest.type(), indexRequest.id());
            request.add(deleteRequest);
            request.add(deleteRequest);
        }
        BulkResponse bulkItemResponse = client.bulk(request).get();
        for (BulkItemResponse itemResponse : bulkItemResponse.getItems()) {
            assertFalse("Item failed to index", itemResponse.isFailed());
        }
    }

    @Test
    public void testCount() throws ExecutionException, InterruptedException {
        IndexResponse indexResponse = indexDocument();
        CountRequest request;
        request = new CountRequest(TEST_INDEX);
        request.types(STATS_TYPE);
        CountResponse countResponse = client.count(request).get();
        assertTrue(countResponse.getCount() > 0);

        request = new CountRequest();
        request.types();
        countResponse = client.count(request).get();
        assertTrue(countResponse.getCount() > 0);

        request = new CountRequest();
        request.source(new QuerySourceBuilder().setQuery(new IdsQueryBuilder().addIds(indexResponse.getId())));

        request.types();
        countResponse = client.count(request).get();
        assertEquals(1, countResponse.getCount());
    }

    @Test
    public void testSearch() throws ExecutionException, InterruptedException {
        indexDocument();

        SearchRequestBuilder search = client.prepareSearch(TEST_INDEX).addSort("datePretty", SortOrder.DESC);
        SearchResponse response = client.search(search.request()).get();
        SearchHits hits = response.getHits();
        assertNotNull(hits);
        SearchHit[] hits1 = hits.hits();
        assertNotNull(hits1);
        assertTrue(hits1.length > 0);
    }

    @Test
    public void testSearchWithAggregationValueCount() throws ExecutionException, InterruptedException {
        indexDocument();

        SearchRequestBuilder search = client.prepareSearch(TEST_INDEX);
        String colorsAgg = "colors";
        search.addAggregation(AggregationBuilders.count(colorsAgg).field("color"));
        search.setSize(0); // no hits please

        SearchResponse response = client.search(search.request()).get();
        SearchHits hits = response.getHits();
        assertNotNull(hits);
        SearchHit[] hits1 = hits.hits();
        assertNotNull(hits1);
        assertEquals(0, hits1.length);
        Aggregations aggregations = response.getAggregations();
        assertNotNull(aggregations);
        Aggregation aggregation = aggregations.get(colorsAgg);
        assertNotNull(aggregation);
        assertTrue(aggregation instanceof ValueCount);
        ValueCount valueCount = (ValueCount) aggregation;
        assertTrue(valueCount.getValue() > 0);


    }

    @Test
    public void testSearchWithAggregationSum() throws ExecutionException, InterruptedException {
        indexDocument();

        SearchRequestBuilder search = client.prepareSearch(TEST_INDEX);
        String name = "amount_sum";
        search.addAggregation(AggregationBuilders.sum(name).field("amount"));
        search.setSize(0); // no hits please

        SearchResponse response = client.search(search.request()).get();
        SearchHits hits = response.getHits();
        assertNotNull(hits);
        SearchHit[] hits1 = hits.hits();
        assertNotNull(hits1);
        assertEquals(0, hits1.length);
        Aggregations aggregations = response.getAggregations();
        assertNotNull(aggregations);
        Aggregation aggregation = aggregations.get(name);
        assertNotNull(aggregation);
        assertTrue(aggregation instanceof Sum);
        Sum valueCount = (Sum) aggregation;
        assertTrue(valueCount.getValue() > 0);

    }

    @Test
    public void testSearchWithAggregationAvg() throws ExecutionException, InterruptedException {
        indexDocument();
        indexDocument();
        indexDocument();

        SearchRequestBuilder search = client.prepareSearch(TEST_INDEX);
        String name = "amount_value";
        search.addAggregation(AggregationBuilders.avg(name).field("amount"));
        search.setSize(0); // no hits please

        SearchResponse response = client.search(search.request()).get();
        SearchHits hits = response.getHits();
        assertNotNull(hits);
        SearchHit[] hits1 = hits.hits();
        assertNotNull(hits1);
        assertEquals(0, hits1.length);
        Aggregations aggregations = response.getAggregations();
        assertNotNull(aggregations);
        Aggregation aggregation = aggregations.get(name);
        assertNotNull(aggregation);
        assertTrue(aggregation instanceof Avg);
        Avg valueCount = (Avg) aggregation;
        assertTrue(valueCount.getValue() > 0);

    }

    @Test
    public void testSearchWithAggregationCardinality() throws ExecutionException, InterruptedException {
        indexDocument();
        indexDocument();
        indexDocument();

        SearchRequestBuilder search = client.prepareSearch(TEST_INDEX);
        String name = "amount_value";
        search.addAggregation(AggregationBuilders.cardinality(name).field("amount"));
        search.setSize(0); // no hits please

        SearchResponse response = client.search(search.request()).get();
        SearchHits hits = response.getHits();
        assertNotNull(hits);
        SearchHit[] hits1 = hits.hits();
        assertNotNull(hits1);
        assertEquals(0, hits1.length);
        Aggregations aggregations = response.getAggregations();
        assertNotNull(aggregations);
        Aggregation aggregation = aggregations.get(name);
        assertNotNull(aggregation);
        assertTrue(aggregation instanceof Cardinality);
        Cardinality valueCount = (Cardinality) aggregation;
        assertTrue(valueCount.getValue() > 0);

    }

    @Test
    public void testSearchWithAggregationGlobal() throws ExecutionException, InterruptedException {
        indexDocument();
        indexDocument();
        indexDocument();

        SearchRequestBuilder search = client.prepareSearch(TEST_INDEX);
        String name = "agg";
        search.addAggregation(AggregationBuilders.global(name).subAggregation(AggregationBuilders.terms("colors").field("color")));
        search.setSize(0); // no hits please

        SearchResponse response = client.search(search.request()).get();
        SearchHits hits = response.getHits();
        assertNotNull(hits);
        SearchHit[] hits1 = hits.hits();
        assertNotNull(hits1);
        assertEquals(0, hits1.length);
        Aggregations aggregations = response.getAggregations();
        assertNotNull(aggregations);
        Global global = aggregations.get(name);
        assertTrue(global.getDocCount() > 0);
    }

    @Test
    public void testSearchWithTermFilterAggregation() throws ExecutionException, InterruptedException {
        indexDocument();
        indexDocument();
        indexDocument();

        SearchRequestBuilder search = client.prepareSearch(TEST_INDEX);
        String name = "agg";
        search.addAggregation(AggregationBuilders.filter(name).filter(FilterBuilders.termFilter("color", "red")));
        search.setSize(0); // no hits please

        SearchResponse response = client.search(search.request()).get();
        SearchHits hits = response.getHits();
        assertNotNull(hits);
        SearchHit[] hits1 = hits.hits();
        assertNotNull(hits1);
        assertEquals(0, hits1.length);
        Aggregations aggregations = response.getAggregations();
        assertNotNull(aggregations);
        Filter aggregation = aggregations.get(name);
        Aggregations buckets = aggregation.getAggregations();
        System.out.println("buckets = " + buckets);
    }

    @Test
    public void testSearchWithTermFilterBucketAggregation() throws ExecutionException, InterruptedException {
        indexDocument();
        indexDocument();
        indexDocument();

        SearchRequestBuilder search = client.prepareSearch(TEST_INDEX);
        String name = "agg";
        AggregationBuilder aggregation =
                AggregationBuilders
                        .filters(name)
                        .filter("red", FilterBuilders.termFilter("color", "red"))
                        .filter("blue", FilterBuilders.termFilter("color", "blue"));

        search.addAggregation(aggregation);
        search.setSize(0); // no hits please

        SearchResponse response = client.search(search.request()).get();
        SearchHits hits = response.getHits();
        assertNotNull(hits);
        SearchHit[] hits1 = hits.hits();
        assertNotNull(hits1);
        assertEquals(0, hits1.length);
        Aggregations aggregations = response.getAggregations();
        assertNotNull(aggregations);
        Filters agg = aggregations.get(name);
        Collection<? extends Filters.Bucket> buckets = agg.getBuckets();
        assertEquals(2, buckets.size());
        // For each entry
        for (Filters.Bucket entry : agg.getBuckets()) {
            String key = entry.getKey();                    // bucket key
            long docCount = entry.getDocCount();            // Doc count
            System.out.println("key = " + key + " docCount = " + docCount);
        }
    }

    @Test
    public void testSearchWithMissingAggregation() throws ExecutionException, InterruptedException {
        indexDocument();
        indexDocument();
        indexDocument();

        SearchRequestBuilder search = client.prepareSearch(TEST_INDEX);
        String name = "agg";



        search.addAggregation(AggregationBuilders.missing(name).field("MyFakeField"));
        search.setSize(0); // no hits please

        SearchResponse response = client.search(search.request()).get();
        SearchHits hits = response.getHits();
        assertNotNull(hits);
        SearchHit[] hits1 = hits.hits();
        assertNotNull(hits1);
        assertEquals(0, hits1.length);
        Aggregations aggregations = response.getAggregations();
        assertNotNull(aggregations);
        Missing agg = aggregations.get(name);
        assertTrue(agg.getDocCount() > 0);
    }

    @Test
    public void testSearchWithNestedAggregation() throws ExecutionException, InterruptedException {
        indexDocument();
        indexDocument();
        indexDocument();

        SearchRequestBuilder search = client.prepareSearch(TEST_INDEX);
        String name = "agg";


        NestedBuilder nestedBuilder = AggregationBuilders.nested(name).path("author.books");
        String genere_count_agg = "genere_count";
        String avg_price_agg = "avg_price";
        nestedBuilder.subAggregation(AggregationBuilders.terms(genere_count_agg).field("author.books.genre"))
                      .subAggregation(AggregationBuilders.avg(avg_price_agg).field("author.books.price"));
        search.addAggregation(nestedBuilder);
        search.setSize(0); // no hits please

        SearchResponse response = client.search(search.request()).get();
        SearchHits hits = response.getHits();
        assertNotNull(hits);
        SearchHit[] hits1 = hits.hits();
        assertNotNull(hits1);
        assertEquals(0, hits1.length);
        Aggregations aggregations = response.getAggregations();
        Nested nested = aggregations.get(name);
        Terms genreCount = nested.getAggregations().get(genere_count_agg);
        List<Terms.Bucket> buckets = genreCount.getBuckets();
        assertTrue(buckets.size() > 0);
        Avg avgPrice = nested.getAggregations().get(avg_price_agg);
        assertTrue(avgPrice.getValue() > 0);

        assertNotNull(aggregations);
    }

    @Test
    @Ignore
    public void testSearchWithChildrenAggregation() throws ExecutionException, InterruptedException {
        IndexRequest post = newPost();
        index(post);
        index(newComment(post.id()));
        index(newComment(post.id()));
        index(newComment(post.id()));
        index(newComment(post.id()));

        SearchRequestBuilder search = client.prepareSearch(POSTS_INDEX).setTypes(POST_TYPE);
        String name = "agg";


        ChildrenBuilder builder = AggregationBuilders.children(name).childType(COMMENT_TYPE);
        search.addAggregation(builder);
        search.setSize(0); // no hits please

        SearchResponse response = client.search(search.request()).get();
        SearchHits hits = response.getHits();
        assertNotNull(hits);
        SearchHit[] hits1 = hits.hits();
        assertNotNull(hits1);
        assertEquals(0, hits1.length);
        Aggregations aggregations = response.getAggregations();
        assertNotNull(aggregations);

        Children children = aggregations.get(name);
        assertNotNull(children);

        assertTrue(children.getDocCount() > 0);
    }

    @Test
    public void testSearchWithMaxAggregation() throws ExecutionException, InterruptedException {
        indexDocument();

        SearchRequestBuilder search = client.prepareSearch(TEST_INDEX);
        String name = "agg";
        search.addAggregation(AggregationBuilders.max(name).field("amount"));
        search.setSize(0); // no hits please

        SearchResponse response = client.search(search.request()).get();
        SearchHits hits = response.getHits();
        assertNotNull(hits);
        SearchHit[] hits1 = hits.hits();
        assertNotNull(hits1);
        assertEquals(0, hits1.length);
        Aggregations aggregations = response.getAggregations();
        assertNotNull(aggregations);
        Max aggregation = aggregations.get(name);
        assertNotNull(aggregation);
        assertTrue(aggregation.getValue() > 0);
    }

    @Test
    public void testSearchWithMinAggregation() throws ExecutionException, InterruptedException {
        indexDocument();

        SearchRequestBuilder search = client.prepareSearch(TEST_INDEX);
        String name = "agg";
        search.addAggregation(AggregationBuilders.min(name).field("amount"));
        search.setSize(0); // no hits please

        SearchResponse response = client.search(search.request()).get();
        SearchHits hits = response.getHits();
        assertNotNull(hits);
        SearchHit[] hits1 = hits.hits();
        assertNotNull(hits1);
        assertEquals(0, hits1.length);
        Aggregations aggregations = response.getAggregations();
        assertNotNull(aggregations);
        Aggregation aggregation = aggregations.get(name);
        assertNotNull(aggregation);
        assertTrue(aggregation instanceof Min);
        Min valueCount = (Min) aggregation;
        assertTrue(valueCount.getValue() != 0);
    }


    @Test
    public void testSearchWithPercentilesAggregation() throws ExecutionException, InterruptedException {
        indexDocument(100);

        SearchRequestBuilder search = client.prepareSearch(TEST_INDEX);
        String name = "agg";
        search.addAggregation(AggregationBuilders.percentiles(name).field("amount").percentiles(.9D,.8D,.7D));
        search.setSize(0); // no hits please

        SearchResponse response = client.search(search.request()).get();
        SearchHits hits = response.getHits();
        assertNotNull(hits);
        SearchHit[] hits1 = hits.hits();
        assertNotNull(hits1);
        assertEquals(0, hits1.length);
        Aggregations aggregations = response.getAggregations();
        assertNotNull(aggregations);
        Percentiles percentiles = aggregations.get(name);
        double percentile = percentiles.percentile(.9D);
        assertTrue(percentile != 0D);
        Map<Double, Percentile> percentileMap = Maps.newHashMap();
        Iterator<Percentile> percentileIterator = percentiles.iterator();
        while (percentileIterator.hasNext()) {
            Percentile next = percentileIterator.next();
            percentileMap.put(next.getPercent(), next);
        }
        assertEquals(3, percentileMap.size());
        for (Map.Entry<Double, Percentile> entry : percentileMap.entrySet()) {
            assertEquals(percentiles.percentile(entry.getKey()), entry.getValue().getValue(), 1);
        }
    }

    @Test
    public void testSearchWithPercentileRanksAggregation() throws ExecutionException, InterruptedException {
        indexDocument(100);

        SearchRequestBuilder search = client.prepareSearch(TEST_INDEX);
        String name = "agg";
        search.addAggregation(AggregationBuilders.percentileRanks(name).field("amount").percentiles(.9D,.8D,.7D));
        search.setSize(0); // no hits please

        SearchResponse response = client.search(search.request()).get();
        SearchHits hits = response.getHits();
        assertNotNull(hits);
        SearchHit[] hits1 = hits.hits();
        assertNotNull(hits1);
        assertEquals(0, hits1.length);
        Aggregations aggregations = response.getAggregations();
        assertNotNull(aggregations);
        PercentileRanks percentileRanks = aggregations.get(name);
        double percentile = percentileRanks.percent(.9D);
        assertTrue(percentile != 0D);
        Map<Double, Percentile> percentileMap = Maps.newHashMap();
        Iterator<Percentile> percentileIterator = percentileRanks.iterator();
        while (percentileIterator.hasNext()) {
            Percentile next = percentileIterator.next();
            percentileMap.put(next.getPercent(), next);
        }
        assertEquals(3, percentileMap.size());
    }

    @Test
    public void testSearchWithRangeAggregation() throws ExecutionException, InterruptedException {
        indexDocument(100);

        SearchRequestBuilder search = client.prepareSearch(TEST_INDEX);
        String name = "agg";
        String maxKey = "1toMax";
        search.addAggregation(AggregationBuilders.range(name).field("amount")
                .addRange(0, 1)
                .addRange(maxKey, 1, Integer.MAX_VALUE));
        search.setSize(0); // no hits please

        SearchResponse response = client.search(search.request()).get();
        Aggregations aggregations = response.getAggregations();
        assertNotNull(aggregations);
        Range range = aggregations.get(name);
        Range.Bucket oneToMaxBucket = range.getBucketByKey(maxKey);
        assertNotNull(oneToMaxBucket);

        Collection<? extends Range.Bucket> buckets = range.getBuckets();
        assertNotNull(buckets);

    }

    @Test
    public void testSearchWithDateRangeAggregation() throws ExecutionException, InterruptedException {
        indexDocument(100);

        SearchRequestBuilder search = client.prepareSearch(TEST_INDEX);
        String name = "agg";
        String minKey = "first";
        search.addAggregation(AggregationBuilders.dateRange(name).field("datePretty")
                .addRange(minKey, new DateTime().minusDays(500), new DateTime().minusDays(400))
                .addRange(new DateTime().minusDays(400), new DateTime().minusDays(300))
                .addRange(new DateTime().minusDays(300), new DateTime().minusDays(200))
                .addRange(new DateTime().minusDays(200), new DateTime().minusDays(100))
                .addRange(new DateTime().minusDays(100), new DateTime().minusDays(0)));

        search.setSize(0); // no hits please

        SearchResponse response = client.search(search.request()).get();
        Aggregations aggregations = response.getAggregations();
        assertNotNull(aggregations);
        Range range = aggregations.get(name);
        Range.Bucket oneToMaxBucket = range.getBucketByKey(minKey);
        assertNotNull(oneToMaxBucket);

        Collection<? extends Range.Bucket> buckets = range.getBuckets();
        assertNotNull(buckets);

    }

    @Test
    public void testSearchWithTopHitsAggregation() throws ExecutionException, InterruptedException {
        indexDocument(100);

        SearchRequestBuilder search = client.prepareSearch(TEST_INDEX);
        String name = "agg";
        search.addAggregation(AggregationBuilders.topHits(name).addFieldDataField("datePretty"));

        search.setSize(0); // no hits please

        SearchResponse response = client.search(search.request()).get();
        Aggregations aggregations = response.getAggregations();
        assertNotNull(aggregations);
        TopHits topHits = aggregations.get(name);
        assertNotNull(topHits);

        SearchHits hits = topHits.getHits();
        assertTrue(hits.getTotalHits() > 0);
        assertTrue(hits.getMaxScore() > 0);

        assertNotNull(hits);
        assertTrue(hits.hits().length > 0);
        for (SearchHit hit : hits.hits()) {
            assertTrue(hit.getScore() > 0);
            assertNotNull(hit.getId());
            assertNotNull(hit.getFields());
            assertNotNull(hit.sourceAsMap());
        }

    }

    @Test
    public void testSearchWithStatsAggregation() throws ExecutionException, InterruptedException {
        indexDocument(100);

        SearchRequestBuilder search = client.prepareSearch(TEST_INDEX);
        String name = "agg";
        search.addAggregation(AggregationBuilders.stats(name).field("amount"));

        search.setSize(0); // no hits please

        SearchResponse response = client.search(search.request()).get();
        Aggregations aggregations = response.getAggregations();
        assertNotNull(aggregations);
        Stats stats = aggregations.get(name);
        assertNotNull(stats);
        assertTrue(stats.getAvg() > 0);
        assertTrue(stats.getMax() != 0);
        assertTrue(stats.getMin() < stats.getMax());
        assertTrue(stats.getCount() > 0);
        assertTrue(stats.getSum() > 0);
    }

    @Test
    public void testSearchWithExtendedStatsAggregation() throws ExecutionException, InterruptedException {
        indexDocument(100);

        SearchRequestBuilder search = client.prepareSearch(TEST_INDEX);
        String name = "agg";
        search.addAggregation(AggregationBuilders.extendedStats(name).field("amount"));

        search.setSize(0); // no hits please

        SearchResponse response = client.search(search.request()).get();
        Aggregations aggregations = response.getAggregations();
        assertNotNull(aggregations);
        ExtendedStats stats = aggregations.get(name);
        assertNotNull(stats);
        assertTrue(stats.getAvg() > 0);
        assertTrue(stats.getMax() != 0);
        assertTrue(stats.getMin() < stats.getMax());
        assertTrue(stats.getCount() > 0);
        assertTrue(stats.getSum() > 0);
        assertTrue(stats.getStdDeviation() != 0);
        assertTrue(stats.getSumOfSquares() != 0);
        assertTrue(stats.getVariance() != 0);
        assertTrue(stats.getStdDeviationBound(ExtendedStats.Bounds.UPPER) != 0);

    }

    @Test
    public void testSearchWithIpRangeAggregation() throws ExecutionException, InterruptedException {
        indexDocument(100);

        SearchRequestBuilder search = client.prepareSearch(TEST_INDEX);
        String name = "agg";
        String mask = "255.255.0.0/32";
        search.addAggregation(AggregationBuilders.ipRange(name).field("ipAddress").addMaskRange(mask));

        search.setSize(0); // no hits please

        SearchResponse response = client.search(search.request()).get();
        Aggregations aggregations = response.getAggregations();
        assertNotNull(aggregations);
        IPv4Range iPv4Range = aggregations.get(name);
        assertNotNull(iPv4Range);
        IPv4Range.Bucket bucketByKey = iPv4Range.getBucketByKey(mask);
        assertNotNull(bucketByKey);
    }

    @Test
    public void testSearchWithGeoBoundsAggregation() throws ExecutionException, InterruptedException {
        indexDocument(100);

        SearchRequestBuilder search = client.prepareSearch(TEST_INDEX);
        String name = "agg";
        search.addAggregation(AggregationBuilders.geoBounds(name).field("currentLocation"));

        search.setSize(0); // no hits please

        SearchResponse response = client.search(search.request()).get();
        Aggregations aggregations = response.getAggregations();
        assertNotNull(aggregations);
        GeoBounds geoBounds = aggregations.get(name);
        assertNotNull(geoBounds);
        validateRangeBucket(geoBounds.topLeft());
        validateRangeBucket(geoBounds.bottomRight());
    }

    @Test
    public void testSearchWithGeoDistanceAggregation() throws ExecutionException, InterruptedException {
        indexDocument(100);

        SearchRequestBuilder search = client.prepareSearch(TEST_INDEX);
        String name = "agg";
        search.addAggregation(AggregationBuilders.geoDistance(name)
                .field("currentLocation")
                .unit(DistanceUnit.MILES)
                .addRange(0, 2000)
                .lat(40)
                .lon(-71.34)
                .distanceType(org.elasticsearch.common.geo.GeoDistance.PLANE));

        search.setSize(0); // no hits please

        SearchResponse response = client.search(search.request()).get();
        Aggregations aggregations = response.getAggregations();
        assertNotNull(aggregations);
        GeoDistance geoDistance = aggregations.get(name);
        assertNotNull(geoDistance);
        Collection<? extends GeoDistance.Bucket> buckets = geoDistance.getBuckets();
        assertNotNull(buckets);
        validate(buckets);
    }

    @Test
    public void testSearchWithGeoHashGridAggregation() throws ExecutionException, InterruptedException {
        indexDocument(100);

        SearchRequestBuilder search = client.prepareSearch(TEST_INDEX);
        String name = "agg";
        search.addAggregation(AggregationBuilders.geohashGrid(name)
                .field("currentLocation")
                .precision(5)
                .size(5));

        search.setSize(0); // no hits please

        SearchResponse response = client.search(search.request()).get();
        Aggregations aggregations = response.getAggregations();
        assertNotNull(aggregations);
        GeoHashGrid geoHashGrid = aggregations.get(name);
        assertNotNull(geoHashGrid);
        Collection<GeoHashGrid.Bucket> buckets = geoHashGrid.getBuckets();
        assertNotNull(buckets);
        validate(buckets);
    }

    @Test
    public void testSearchWithHistogramAggregation() throws ExecutionException, InterruptedException {
        indexDocument(100);

        SearchRequestBuilder search = client.prepareSearch(TEST_INDEX);
        String name = "agg";
        search.addAggregation(AggregationBuilders.histogram(name)
                .field("amount")
                .interval(1000));

        search.setSize(0); // no hits please

        SearchResponse response = client.search(search.request()).get();
        Aggregations aggregations = response.getAggregations();
        assertNotNull(aggregations);
        Histogram histogram = aggregations.get(name);
        assertNotNull(histogram);
        List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
        validate(buckets);


    }
    @Test
    public void testSearchWithDateHistogramAggregation() throws ExecutionException, InterruptedException {
        indexDocument(100);

        SearchRequestBuilder search = client.prepareSearch(TEST_INDEX);
        String name = "agg";
        search.addAggregation(AggregationBuilders.dateHistogram(name)
                .field("datePretty")
                .interval(1000));

        search.setSize(0); // no hits please

        SearchResponse response = client.search(search.request()).get();
        Aggregations aggregations = response.getAggregations();
        assertNotNull(aggregations);
        Histogram histogram = aggregations.get(name);
        assertNotNull(histogram);
        List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
        validate(buckets);
    }

    @Test
    public void testSearchWithSignificantStringTermsAggregation() throws ExecutionException, InterruptedException {
        indexDocument(1000);

        SearchRequestBuilder search = client.prepareSearch(TEST_INDEX);
        String name = "agg";
        search.setQuery(QueryBuilders.termQuery("genre", "action"));
        search.addAggregation(AggregationBuilders.significantTerms(name)
                .field("color")
                .size(10));

        search.setSize(10); // no hits please

        SearchResponse response = client.search(search.request()).get();
        Aggregations aggregations = response.getAggregations();
        assertNotNull(aggregations);
        SignificantTerms significantTerms = aggregations.get(name);
        assertNotNull(significantTerms);
        for (MultiBucketsAggregation.Bucket bucket : significantTerms.getBuckets()) {
            assertNotNull(bucket.getKeyAsText());
            assertNotNull(bucket.getKey());
        }
    }

    @Test
    public void testSearchWithSignificantLongTermsAggregation() throws ExecutionException, InterruptedException {
        indexDocument(1000);

        SearchRequestBuilder search = client.prepareSearch(TEST_INDEX);
        String name = "agg";
        search.setQuery(QueryBuilders.termQuery("genre", "action"));
        search.addAggregation(AggregationBuilders.significantTerms(name)
                .minDocCount(1)
                .field("sentiment")
                .size(8));

        search.setSize(0); // no hits please

        SearchResponse response = client.search(search.request()).get();
        Aggregations aggregations = response.getAggregations();
        assertNotNull(aggregations);
        SignificantTerms significantTerms = aggregations.get(name);
        assertNotNull(significantTerms);
        for (MultiBucketsAggregation.Bucket bucket : significantTerms.getBuckets()) {
            assertNotNull(bucket.getKeyAsText());
            assertNotNull(bucket.getKey());
        }
    }

    @Test
    public void testSearchWithFiltersAggregation() throws ExecutionException, InterruptedException {
        indexDocument(100);

        SearchRequestBuilder search = client.prepareSearch(TEST_INDEX);
        String name = "agg";
        search.setQuery(QueryBuilders.termQuery("genre", "action"));
        search.addAggregation(AggregationBuilders.filters(name)
                .filter("sentiment_filter", FilterBuilders.termFilter("sentiment", 5))
                .filter("red_filter", FilterBuilders.termFilter("color", Color.red.name())));


        search.setSize(0); // no hits please

        SearchResponse response = client.search(search.request()).get();
        Aggregations aggregations = response.getAggregations();
        assertNotNull(aggregations);
        Filters filters = aggregations.get(name);
        assertNotNull(filters);
        validate(filters.getBuckets());

    }
    @Test
    public void testSearchWithScriptedMetricAggregation() throws ExecutionException, InterruptedException {
        indexDocument(100);

        SearchRequestBuilder search = client.prepareSearch(TEST_INDEX);
        String name = "agg";
        search.addAggregation(AggregationBuilders.scriptedMetric(name)
                .initScript("_agg['positive_sentiment'] = []")
                .mapScript(" if (doc['sentiment'].value > 4) { _agg.positive_sentiment.add(doc['sentiment']) } "));


        search.setSize(0); // no hits please

        SearchResponse response = client.search(search.request()).get();
        Aggregations aggregations = response.getAggregations();
        assertNotNull(aggregations);
        ScriptedMetric scriptedMetric = aggregations.get(name);
        assertNotNull(scriptedMetric);

        Object aggregation = scriptedMetric.aggregation();
        assertNotNull(aggregation);
    }

    @Test
    public void testScanAndScroll() throws ExecutionException, InterruptedException {
        indexDocument(100);

        TimeValue scrollKeepAlive = TimeValue.timeValueHours(1);
        SearchResponse response = client.prepareSearch(TEST_INDEX)
                .setSearchType(SearchType.SCAN)
                .setScroll(scrollKeepAlive)
                .setQuery(QueryBuilders.termQuery("color", Color.red))
                .setSize(2).execute().actionGet();

        assertNotNull(response.getScrollId());

        SearchResponse response2 = client.prepareSearchScroll(response.getScrollId()).setScroll(scrollKeepAlive).execute().actionGet();
        validateScrollResponse(response2);

        client.prepareClearScroll().addScrollId(response.getScrollId()).get();
        SearchResponse response3 = client.prepareSearchScroll(response.getScrollId()).setScroll(scrollKeepAlive).execute().actionGet();
        assertEquals(0, response3.getHits().hits().length);

    }

    @Test
    public void testDeleteByQuery() throws ExecutionException, InterruptedException {
        indexDocument(100);
        DeleteByQueryResponse response;
        response = client.prepareDeleteByQuery(TEST_INDEX)
                    .setQuery(QueryBuilders.termQuery("color", Color.red)).get();
        for (IndexDeleteByQueryResponse queryResponse : response) {
            assertEquals(TEST_INDEX, queryResponse.getIndex());
        }
    }

    @Test
    public void testIndexedScript() throws ExecutionException, InterruptedException {
        indexDocument(100);

        PutIndexedScriptResponse response;
        Map<String, Object> script = Maps.newHashMap();
        script.put("script", "log(_score * 2) + my_modifier");
        String id = "indexedCalculateScore-" + UUID.randomUUID().toString();
        response = client.preparePutIndexedScript()
                .setScriptLang("groovy")
                .setId(id)
                .setSource(script)
                .get();
        assertNotNull(response);
        assertNotNull(response.getId());
        assertTrue(response.isCreated());

        GetIndexedScriptResponse getIndexedScriptResponse;
        getIndexedScriptResponse = client.getIndexedScript(new GetIndexedScriptRequest("groovy", id)).actionGet();
        assertNotNull(getIndexedScriptResponse);
        assertTrue(getIndexedScriptResponse.isExists());

        DeleteIndexedScriptResponse deleteIndexedScriptResponse;
        deleteIndexedScriptResponse = client.deleteIndexedScript(new DeleteIndexedScriptRequest("groovy", id)).actionGet();
        assertNotNull(deleteIndexedScriptResponse);
        assertEquals(id, deleteIndexedScriptResponse.getId());
    }

    @Test
    public void testExistsRequest() throws ExecutionException, InterruptedException {
        indexDocument(100);

        ExistsResponse existsResponse = client.prepareExists(TEST_INDEX)
                .setTypes(STATS_TYPE)
                .setQuery(QueryBuilders.termQuery("color", "red")).get();
        assertNotNull(existsResponse);
        assertTrue(existsResponse.exists());
    }

    @Test
    public void testExplainRequest() throws ExecutionException, InterruptedException {
        IndexRequest request = newIndexRequest();
        index(request);
        ExplainResponse response = client.prepareExplain(TEST_INDEX, STATS_TYPE, request.id())
                .setQuery(QueryBuilders.termQuery("color", request.sourceAsMap().get("color"))).get();
        assertNotNull(response);
    }

    @Test
    public void testMultiGetRequest() throws ExecutionException, InterruptedException {
        List<IndexResponse> indexResponses = indexDocument(3);
        MultiGetRequestBuilder requestBuilder = client.prepareMultiGet();

        for (IndexResponse indexResponse : indexResponses) {
            requestBuilder.add(indexResponse.getIndex(), indexResponse.getType(), indexResponse.getId());
        }
        MultiGetResponse responses = requestBuilder.get();
        assertNotNull(responses);
        MultiGetItemResponse[] responses1 = responses.getResponses();
        assertEquals(indexResponses.size(), responses1.length);
        for (MultiGetItemResponse response : responses) {
            assertNotNull(response.getIndex());
            assertNotNull(response.getType());
            assertNotNull(response.getId());
            Map<String, Object> sourceAsMap = response.getResponse().getSourceAsMap();
            assertNotNull(sourceAsMap);
            assertTrue(sourceAsMap.size() > 0);
        }
    }

    @Test
    public void testMultiSearchRequest() throws ExecutionException, InterruptedException {
        List<IndexResponse> indexResponses = indexDocument(3);
        MultiSearchRequestBuilder requestBuilder = client.prepareMultiSearch();

        for (IndexResponse indexResponse : indexResponses) {
            IdsQueryBuilder query = QueryBuilders.idsQuery(indexResponse.getType()).ids(indexResponse.getId());
            requestBuilder.add(client.prepareSearch(indexResponse.getIndex()).setQuery(query));
        }
        MultiSearchResponse multiSearchResponse = requestBuilder.get();
        assertNotNull(multiSearchResponse);

        MultiSearchResponse.Item[] responses = multiSearchResponse.getResponses();
        assertNotNull(responses);
        assertEquals(indexResponses.size(), responses.length);
        for (MultiSearchResponse.Item item : responses) {
            SearchResponse response = item.getResponse();
            assertNotNull(response);
            assertNotNull(response.getHits());
            assertEquals(1, response.getHits().getHits().length);
        }
    }

    @Test
    public void testMultiSearchRequestWithFailures() throws ExecutionException, InterruptedException {
        List<IndexResponse> indexResponses = indexDocument(3);
        MultiSearchRequestBuilder requestBuilder = client.prepareMultiSearch();

        for (IndexResponse indexResponse : indexResponses) {
            IdsQueryBuilder query = QueryBuilders.idsQuery(indexResponse.getType()).ids(indexResponse.getId());
            SearchRequestBuilder searchRequestBuilder = client.prepareSearch(indexResponse.getIndex() + " " + indexResponse.getIndex());
            requestBuilder.add(searchRequestBuilder.setQuery(query));
        }
        MultiSearchResponse multiSearchResponse = requestBuilder.get();
        assertNotNull(multiSearchResponse);

        MultiSearchResponse.Item[] responses = multiSearchResponse.getResponses();
        assertNotNull(responses);
        assertEquals(indexResponses.size(), responses.length);
        for (MultiSearchResponse.Item item : responses) {
            assertTrue(item.isFailure());
            assertTrue(Strings.isNotEmpty(item.getFailureMessage()));
        }
    }




    private void validateScrollResponse(SearchResponse response) {
        assertNotNull(response);
        assertNotNull(response.getHits());
        assertNotNull(response.getHits().hits());
        assertEquals(2, response.getHits().hits().length);
    }


    private void validate(Collection<? extends MultiBucketsAggregation.Bucket> buckets) {
        for (MultiBucketsAggregation.Bucket bucket : buckets) {
            assertTrue(bucket.getDocCount() > 0);
            assertNotNull(bucket.getKeyAsText());
            assertNotNull(bucket.getKey());
            assertTrue(bucket.getDocCount() > 0);

            if (bucket instanceof Range.Bucket) {
                Range.Bucket rangeBucket = (Range.Bucket) bucket;
                assertNotNull(rangeBucket.getFrom());
                assertNotNull(rangeBucket.getTo());
            }
        }
    }

    private void validateRangeBucket(GeoPoint topLeft) {
        assertNotNull(topLeft);
        assertTrue(topLeft.getLat() != 0);
        assertTrue(topLeft.getLon() != 0);
    }


    private List<IndexResponse> indexDocument(int numberOfDocs) throws InterruptedException, ExecutionException {
        List<IndexResponse> responses = Lists.newArrayList();
        for (int i =0; i < numberOfDocs; i++) {
            responses.add(indexDocument());
        }
        return responses;
    }

    private IndexResponse indexDocument() throws InterruptedException, ExecutionException {
        IndexRequest request = newIndexRequest();
        IndexResponse indexResponse = this.client.index(request).get();
        assertTrue(indexResponse.isCreated());

        return indexResponse;
    }


    private IndexResponse index(IndexRequest request) throws ExecutionException, InterruptedException {
        IndexResponse indexResponse = this.client.index(request).get();
        assertTrue(indexResponse.isCreated());
        return indexResponse;
    }

    private IndexRequest newPost() {
        IndexRequest request = newCommentOrPost(POST_TYPE);
        return request;
    }

    private IndexRequest newComment(String postId) {
        assert Strings.isNotEmpty(postId);
        IndexRequest indexRequest = newCommentOrPost(COMMENT_TYPE);
        indexRequest.parent(postId);
        return indexRequest;
    }


    private IndexRequest newCommentOrPost(String type) {
        String id = UUID.randomUUID().toString();
        IndexRequest request = new IndexRequest(POSTS_INDEX, type, id);
        Map<String, Object> source = Maps.newHashMap();
        source.put("title", randomName() + " " + randomName());
        source.put("description", randomName() + " " + randomName() + " " + randomName() + " " + randomName());
        source.put("dateCreated", new DateTime());
        request.source(source);
        return request;
    }

    private IndexRequest newIndexRequest() {
        String id = UUID.randomUUID().toString();
        IndexRequest request = new IndexRequest(TEST_INDEX, STATS_TYPE, id);
        Map<String, Object> source = Maps.newHashMap();
        source.put("datePretty", new DateTime().minusDays(Math.abs(new Random().nextInt() % 500)));
        source.put("sentiment", Math.abs(new Random().nextInt() % 10));
        source.put("color", randomColor().name());
        source.put("genre", randomGenre().name());
        source.put("amount", Math.abs(new Random().nextDouble()));
        Map<String, Object> reach = Maps.newHashMap();
        reach.put("type", "point");
        reach.put("coordinates", Arrays.asList(-1 * Math.abs(new Random().nextDouble() % 300), Math.abs(new Random().nextDouble() % 300)));
        source.put("reach", reach);

        Map<String, Object> latLon = Maps.newLinkedHashMap();
        latLon.put("lat", -1 * Math.abs(new Random().nextInt() % 360));
        latLon.put("lon", Math.abs(new Random().nextInt() % 360));
        source.put("currentLocation", latLon);
        source.put("ipAddress", Joiner.on('.').join(Math.abs(new Random().nextInt() % 255), Math.abs(new Random().nextInt() % 255), Math.abs(new Random().nextInt() % 255), Math.abs(new Random().nextInt() % 255)));

        Map<String, Object> author = Maps.newHashMap();
        author.put("name", randomName());
        List<Map<String, Object>> books = Lists.newArrayList();
        for (int i = Math.abs(new Random().nextInt()) % 15; i >= 0; i--) {
            Map<String, Object> book = Maps.newHashMap();
            book.put("title", randomName());
            book.put("genre", randomGenre());
            book.put("price", Math.abs(new Random().nextFloat()));
            books.add(book);
        }
        author.put("books", books);
        source.put("author", author);

        request.source(source);
        request.refresh(true);
        return request;
    }

    private List<String> names;
    private String randomName() {
        if (names == null) {
            names = Lists.newArrayList();
            InputStream in = this.getClass().getResourceAsStream("/config/names.txt");
            BufferedReader reader = new BufferedReader(new InputStreamReader(in, Charsets.UTF_8));
            String name;
            try {
                while( (name=reader.readLine()) != null) {
                    names.add(name);
                }
            } catch (IOException e) {
                throw new RuntimeException(e.getMessage(), e);
            }
            finally {
                try {
                    reader.close();
                } catch (IOException e) {
                    throw new RuntimeException(e.getMessage(), e);
                }

            }
        }
        assert names.size() > 0;
        return names.get(Math.abs(new Random().nextInt()) % names.size());

    }

    private Color randomColor() {
        return Color.values()[Math.abs(new Random().nextInt()) % Color.values().length];
    }

    private Genre randomGenre() {
        return Genre.values()[Math.abs(new Random().nextInt()) % Genre.values().length];
    }
}