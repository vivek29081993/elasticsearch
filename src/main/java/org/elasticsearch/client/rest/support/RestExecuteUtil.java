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

import org.apache.http.HttpEntity;
import org.elasticsearch.action.*;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.RestRequest;

import java.io.IOException;

/**
 * @author Brandon Kearby
 *         September 22, 2016
 */
public class RestExecuteUtil {

    public static final int STATUS_OK = 200;

    public static <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder, ?>>
    void execute(InternalRestClient internalRestClient,
                 Action<Request, Response, RequestBuilder, ?> action, Request request, ActionListener<Response> listener) {
        try {
            RestResponse restResponse = internalRestClient.performRequest(request.getRestMethod().name(), request.getRestEndPoint(), request.getRestParams(),  request.getRestEntity(), request.getRestHeaders());
            Response response = action.newResponse();
            if (request.getRestMethod() == RestRequest.Method.HEAD) {
                response.exists(restResponse.getHttpResponse().getStatusLine().getStatusCode() == STATUS_OK);
            }
            else {
                HttpEntity entity = restResponse.getEntity();
                assert entity != null;
                String content = HttpUtils.readUtf8(entity);
                XContentParser parser = XContentHelper.createParser(new BytesArray(content));
                response.readFrom(parser);
            }
            listener.onResponse(response);

        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}
