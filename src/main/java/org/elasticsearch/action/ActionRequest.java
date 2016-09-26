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

package org.elasticsearch.action;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;

/**
 *
 */
public abstract class ActionRequest<T extends ActionRequest> extends TransportRequest {

    public static final Header[] EMPTY_HEADERS = new Header[0];
    public static final NStringEntity EMPTY_ENTITY = new NStringEntity("", StandardCharsets.UTF_8);

    private boolean listenerThreaded = false;

    protected ActionRequest() {
        super();
    }

    protected ActionRequest(ActionRequest request) {
        super(request);
        // this does not set the listenerThreaded API, if needed, its up to the caller to set it
        // since most times, we actually want it to not be threaded...
        //this.listenerThreaded = request.listenerThreaded();
    }

    /**
     * Should the response listener be executed on a thread or not.
     * <p/>
     * <p>When not executing on a thread, it will either be executed on the calling thread, or
     * on an expensive, IO based, thread.
     */
    public final boolean listenerThreaded() {
        return this.listenerThreaded;
    }

    /**
     * Sets if the response listener be executed on a thread or not.
     */
    @SuppressWarnings("unchecked")
    public final T listenerThreaded(boolean listenerThreaded) {
        this.listenerThreaded = listenerThreaded;
        return (T) this;
    }

    public abstract ActionRequestValidationException validate();

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
    }

    public RestRequest.Method getRestMethod() {
        throw new UnsupportedOperationException("Implement me in " + this.getClass());
    }

    public String getRestEndPoint() {
        throw new UnsupportedOperationException("Implement me in " + this.getClass());
    }

    public HttpEntity getRestEntity() throws IOException {
        return EMPTY_ENTITY;
    }

    public Map<String, String> getRestParams() {
        return Collections.emptyMap();
    }


    public HttpEntity getBulkRestEntity() throws IOException {
        throw new UnsupportedOperationException("Implement me in " + this.getClass());
    }

    public Header[] getRestHeaders() {
        return EMPTY_HEADERS;
    }


}
