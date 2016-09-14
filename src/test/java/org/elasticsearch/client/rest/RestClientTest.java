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

import com.carrotsearch.ant.tasks.junit4.dependencies.com.google.common.collect.Lists;
import com.google.common.base.Charsets;
import com.google.common.collect.Maps;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.count.CountRequest;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.QuerySourceBuilder;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.Names;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.*;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.filters.Filters;
import org.elasticsearch.search.aggregations.bucket.filters.FiltersAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.global.Global;
import org.elasticsearch.search.aggregations.bucket.global.InternalGlobal;
import org.elasticsearch.search.aggregations.bucket.missing.Missing;
import org.elasticsearch.search.aggregations.bucket.nested.Nested;
import org.elasticsearch.search.aggregations.bucket.nested.NestedBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCount;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URL;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static com.google.common.collect.Lists.newArrayList;
import static org.junit.Assert.*;

/**
 * @author Brandon Kearby
 *         September 08, 2016
 */
public class RestClientTest {
    public static final String INDEX = "test";
    public static final String TYPE = "stats";
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
        GetRequest getRequest = new GetRequest(INDEX, TYPE, id);
        ActionFuture<GetResponse> getResponseActionFuture = this.client.get(getRequest);
        return getResponseActionFuture.get();
    }

    @Test
    public void testIndex() throws ExecutionException, InterruptedException {
        String id = UUID.randomUUID().toString();
        IndexRequest request = new IndexRequest(INDEX, TYPE, id);
        Map<String, Object> source = Maps.newHashMap();
        source.put("datePretty", "2016-02-28T05:30:00+05:30");
        request.source(source);
        IndexResponse indexResponse = this.client.index(request).get();
        assertEquals(id, indexResponse.getId());
        assertEquals(INDEX, indexResponse.getIndex());
        assertEquals(TYPE, indexResponse.getType());

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
        assertEquals(INDEX, deleteResponse.getIndex());
        assertEquals(TYPE, deleteResponse.getType());
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
        assertEquals(count, bulkItemResponse.getItems().length);
        for (BulkItemResponse itemResponse : bulkItemResponse.getItems()) {
            assertFalse("Item failed to index", itemResponse.isFailed());
        }
    }

    @Test
    public void testCount() throws ExecutionException, InterruptedException {
        IndexResponse indexResponse = indexDocument();
        CountRequest request;
        request = new CountRequest(INDEX);
        request.types(TYPE);
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

        SearchRequestBuilder search = client.prepareSearch(INDEX).addSort("datePretty", SortOrder.DESC);
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

        SearchRequestBuilder search = client.prepareSearch(INDEX);
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

        SearchRequestBuilder search = client.prepareSearch(INDEX);
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

        SearchRequestBuilder search = client.prepareSearch(INDEX);
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

        SearchRequestBuilder search = client.prepareSearch(INDEX);
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

        SearchRequestBuilder search = client.prepareSearch(INDEX);
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
        Global aggregation = aggregations.get(name);
        assertTrue(aggregation instanceof InternalGlobal);
        InternalGlobal internalGlobal = (InternalGlobal) aggregation;
        assertTrue(internalGlobal.getDocCount() > 0);
        InternalAggregations children = internalGlobal.getAggregations();
        assertNotNull(children);
    }

    @Test
    public void testSearchWithTermFilterAggregation() throws ExecutionException, InterruptedException {
        indexDocument();
        indexDocument();
        indexDocument();

        SearchRequestBuilder search = client.prepareSearch(INDEX);
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

        SearchRequestBuilder search = client.prepareSearch(INDEX);
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

        SearchRequestBuilder search = client.prepareSearch(INDEX);
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

        SearchRequestBuilder search = client.prepareSearch(INDEX);
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



    private IndexResponse indexDocument() throws InterruptedException, ExecutionException {
        IndexRequest request = newIndexRequest();
        IndexResponse indexResponse = this.client.index(request).get();
        assertTrue(indexResponse.isCreated());

        return indexResponse;
    }


    private IndexRequest newIndexRequest() {
        String id = UUID.randomUUID().toString();
        IndexRequest request = new IndexRequest(INDEX, TYPE, id);
        Map<String, Object> source = Maps.newHashMap();
        source.put("datePretty", "2016-02-28T05:30:00+05:30");
        source.put("color", randomColor().name());
        source.put("amount", Math.abs(new Random().nextDouble()));
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