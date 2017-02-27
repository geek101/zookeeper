package org.apache.zookeeper.server.quorum.util;

/**
 * Created by powell on 12/24/15.
 */
public interface ErrCallback {
    void caughtException(Exception exp) throws Exception;
}
