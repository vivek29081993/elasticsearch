package org.elasticsearch.client.rest;

import com.google.common.collect.Maps;
import org.elasticsearch.action.ActionFuture;
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
    public static final String INDEX = "bm_content_p9993342_v11_20150702_0000";
    public static final String TYPE = "stats";
    private RestClient client;

    @Before
    public void setUp() {
        this.client = new RestClient("localhost");
    }

    @Test
    public void testGet() throws ExecutionException, InterruptedException {
        //bm_content_p9993342_v11_20150702_0000/stats/38630590-4242-4397-a1bb-d647218aea06:2016-2-28
        String id = "38630590-4242-4397-a1bb-d647218aea06:2016-2-28";
        GetRequest getRequest = new GetRequest(INDEX, TYPE, id);
        ActionFuture<GetResponse> getResponseActionFuture = this.client.get(getRequest);
        GetResponse getResponse = getResponseActionFuture.get();
        assertEquals(id, getResponse.getId());
    }

    @Test
    public void testIndex() throws ExecutionException, InterruptedException {
        //bm_content_p9993342_v11_20150702_0000/stats/38630590-4242-4397-a1bb-d647218aea06:2016-2-28
        String id = UUID.randomUUID().toString();
        IndexRequest request = new IndexRequest(INDEX, TYPE, id);
        Map<String, Object> source = Maps.newHashMap();
        source.put("datePretty", "2016-02-28T05:30:00+05:30");
        request.source(source);
        IndexResponse indexResponse = this.client.index(request).get();
        assertEquals(id, indexResponse.getId());
        assertEquals(INDEX, indexResponse.getIndex());
        assertEquals(TYPE, indexResponse.getType());

        GetRequest getRequest = new GetRequest(INDEX, TYPE, id);
        ActionFuture<GetResponse> getResponseActionFuture = this.client.get(getRequest);
        GetResponse getResponse = getResponseActionFuture.get();
        assertEquals(id, getResponse.getId());
        assertEquals(source.get("datePretty"), getResponse.getSourceAsMap().get("datePretty"));

    }
}