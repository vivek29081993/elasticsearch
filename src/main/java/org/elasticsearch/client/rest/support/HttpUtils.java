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
