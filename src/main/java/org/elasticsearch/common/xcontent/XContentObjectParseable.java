package org.elasticsearch.common.xcontent;

import java.io.IOException;

/**
 * @author Brandon Kearby
 *         September 19, 2016
 */
public interface XContentObjectParseable<T> {

    void apply(XContentObject source, T object) throws IOException;
}
