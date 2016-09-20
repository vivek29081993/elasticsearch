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

package org.elasticsearch.action.search;

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import org.apache.http.HttpEntity;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.rest.RestRequest;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;
import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 */
public class ClearScrollRequest extends ActionRequest<ClearScrollRequest> {

    private List<String> scrollIds;

    public List<String> getScrollIds() {
        return scrollIds;
    }

    public void setScrollIds(List<String> scrollIds) {
        this.scrollIds = scrollIds;
    }

    public void addScrollId(String scrollId) {
        if (scrollIds == null) {
            scrollIds = newArrayList();
        }
        scrollIds.add(scrollId);
    }

    public List<String> scrollIds() {
        return scrollIds;
    }

    public void scrollIds(List<String> scrollIds) {
        this.scrollIds = scrollIds;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (scrollIds == null || scrollIds.isEmpty()) {
            validationException = addValidationError("no scroll ids specified", validationException);
        }
        return validationException;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        scrollIds = Arrays.asList(in.readStringArray());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (scrollIds == null) {
            out.writeVInt(0);
        } else {
            out.writeStringArray(scrollIds.toArray(new String[scrollIds.size()]));
        }
    }

    @Override
    public String getRestEndPoint() {
        return "_search/scroll";
    }

    @Override
    public RestRequest.Method getRestMethod() {
        return RestRequest.Method.DELETE;
    }

    @Override
    public HttpEntity getRestEntity() throws IOException {
/*
    //todo for version 2.x
        Map<String, Object> payload = Maps.newHashMap();
        payload.put("scroll_id", scrollIds.toArray(new String[scrollIds.size()]));
        return new NStringEntity(XContentHelper.convertToJson(payload, false), StandardCharsets.UTF_8);
*/
        return new NStringEntity(Joiner.on(',').join(scrollIds), StandardCharsets.UTF_8);

    }
}
