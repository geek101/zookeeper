package org.apache.zookeeper.server.quorum.util;

/**
 * Created by powell on 11/16/15.
 */
@SuppressWarnings("serial")
public class ChannelException extends Exception {
    public ChannelException(String message, Object... args) {
        super(String.format(message, args));
    }
}
