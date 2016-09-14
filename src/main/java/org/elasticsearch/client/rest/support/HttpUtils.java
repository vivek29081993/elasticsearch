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
import org.apache.http.nio.entity.NStringEntity;

import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * @author Brandon Kearby
 *         September 09, 2016
 */
public class HttpUtils {

    public static final NStringEntity EMPTY_ENTITY = new NStringEntity("", StandardCharsets.UTF_8);

    public static String readUtf8(HttpEntity entity) throws IOException {
        char[] buffer = new char[8192];
        StringBuilder builder = new StringBuilder();
        InputStreamReader reader = new InputStreamReader(entity.getContent(), Charset.forName("UTF-8"));

        for (int read; (read = reader.read(buffer)) >= 0; ) {
            builder.append(buffer, 0, read);
        }
        return builder.toString();
    }

}
