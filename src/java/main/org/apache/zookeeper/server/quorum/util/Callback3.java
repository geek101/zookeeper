package org.apache.zookeeper.server.quorum.util;

/**
 * Created by powell on 11/30/15.
 */
public interface Callback3 {
    void done(Object o, Object p, Object q) throws ChannelException;
}
