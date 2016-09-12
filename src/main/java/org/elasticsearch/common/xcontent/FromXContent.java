package org.elasticsearch.common.xcontent;

import java.io.IOException;

/**
 * @author Brandon Kearby
 *         September 10, 2016
 */
public interface FromXContent {

    void readFrom(XContentParser parser) throws IOException;
}
