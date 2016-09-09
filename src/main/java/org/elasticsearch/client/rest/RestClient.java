package org.elasticsearch.client.rest;

import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.*;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.rest.support.HttpUtils;
import org.elasticsearch.client.rest.support.InternalRestClient;
import org.elasticsearch.client.rest.support.RestResponse;
import org.elasticsearch.client.support.AbstractClient;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;

/**
 * @author Brandon Kearby
 *         September 08, 2016
 */
public class RestClient extends AbstractClient implements Client {

    private static final int DEFAULT_PORT = 9200;

    private InternalRestClient internalRestClient;

    public RestClient(String hostname) {
        internalRestClient = InternalRestClient.builder(new HttpHost(hostname, DEFAULT_PORT)).build();
    }

    @Override
    public void close() throws ElasticsearchException {
        try {
            internalRestClient.close();
        } catch (IOException e) {
            throw new ElasticsearchException(e.getMessage(), e);
        }
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse,
            RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder, Client>>
            ActionFuture<Response> execute(Action<Request, Response, RequestBuilder, Client> action, Request request) {
        PlainActionFuture<Response> actionFuture = PlainActionFuture.newFuture();
        execute(action, request, actionFuture);
        return actionFuture;
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder, Client>> void execute(Action<Request, Response, RequestBuilder, Client> action, Request request, ActionListener<Response> listener) {
        System.out.println("action = " + action);
        System.out.println("request = " + request);
        System.out.println("listener = " + listener);

        try {
            RestResponse restResponse = internalRestClient.performRequest(request.getRestMethod().name(), request.getRestEndPoint(), request.getRestParams(),  request.getRestEntity(), request.getRestHeaders());
            System.out.println("response = " + restResponse);
            Response response = action.newResponse();
            //todo replace with streaming BytesReference
            String content = HttpUtils.readUtf8(restResponse.getEntity());
            XContentParser parser = XContentHelper.createParser(new BytesArray(content));
            response.readFrom(parser);
            listener.onResponse(response);

        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }


    @Override
    public ThreadPool threadPool() {
        return null;
    }

    @Override
    public AdminClient admin() {
        return null;
    }

    @Override
    public Settings settings() {
        return null;
    }
}
