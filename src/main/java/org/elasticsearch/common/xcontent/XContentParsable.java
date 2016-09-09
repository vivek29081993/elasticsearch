package org.elasticsearch.common.xcontent;

import java.io.IOException;

/**
 * @author Brandon Kearby
 *         September 09, 2016
 */
public interface XContentParsable<T> {

    void apply(XContentParser parser, T object) throws IOException;

}
