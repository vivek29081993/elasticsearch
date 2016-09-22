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
package org.elasticsearch.client.rest.support;

import org.apache.http.HttpHost;
import org.elasticsearch.action.*;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.ClusterAdminClient;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.rest.support.HttpUtils;
import org.elasticsearch.client.rest.support.InternalRestClient;
import org.elasticsearch.client.rest.support.RestResponse;
import org.elasticsearch.client.support.AbstractClusterAdminClient;
import org.elasticsearch.client.support.AbstractIndicesAdminClient;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.TransportSearchModule;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;

/**
 * @author Brandon Kearby
 *         September 22, 2016
 */
public class InternalRestIndicesAdminClient extends AbstractIndicesAdminClient implements IndicesAdminClient {

    private InternalRestClient internalRestClient;

    public InternalRestIndicesAdminClient(InternalRestClient internalRestClient) {
        this.internalRestClient = internalRestClient;
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder, IndicesAdminClient>> ActionFuture<Response> execute(Action<Request, Response, RequestBuilder, IndicesAdminClient> action, Request request) {
        PlainActionFuture<Response> actionFuture = PlainActionFuture.newFuture();
        execute(action, request, actionFuture);
        return actionFuture;
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder, IndicesAdminClient>> void execute(Action<Request, Response, RequestBuilder, IndicesAdminClient> action, Request request, ActionListener<Response> listener) {
        RestExecuteUtil.execute(internalRestClient, action, request, listener);
    }

    @Override
    public ThreadPool threadPool() {
        return null;
    }
}
