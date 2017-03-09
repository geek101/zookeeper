package org.apache.zookeeper.server.quorum;

@SuppressWarnings("serial")
public class ElectionException extends Exception {
    public ElectionException(String message, Object... args) {
        super(String.format(message, args));
    }
}
