package org.elasticsearch.client.rest;

import com.google.common.collect.Maps;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
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
        IndexResponse indexResponse = indexTestDocument();
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
        IndexResponse indexResponse = indexTestDocument();

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



    private IndexResponse indexTestDocument() throws InterruptedException, ExecutionException {
        String id = UUID.randomUUID().toString();
        IndexRequest request = new IndexRequest(INDEX, TYPE, id);
        Map<String, Object> source = Maps.newHashMap();
        source.put("datePretty", "2016-02-28T05:30:00+05:30");
        request.source(source);
        IndexResponse indexResponse = this.client.index(request).get();
        assertTrue(indexResponse.isCreated());

        return indexResponse;
    }
}