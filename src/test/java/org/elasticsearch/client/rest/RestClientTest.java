package org.elasticsearch.client.rest;

import com.google.common.collect.Maps;
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
import org.elasticsearch.index.query.ConstantScoreQueryBuilder;
import org.elasticsearch.index.query.FilteredQueryBuilder;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;

/**
 * @author Brandon Kearby
 *         September 08, 2016
 */
public class RestClientTest {
    public static final String INDEX = "test";
    public static final String TYPE = "stats";
    private RestClient client;

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
        assertTrue(countResponse.getCount() == 1);
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
        request.source(source);
        return request;
    }
}