package org.apache.zookeeper.server.quorum.helpers.netty;

import java.nio.ByteBuffer;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

/**
 * Created by powell on 12/6/15.
 */
public class MockSendDataChannel extends MockSndHdrChannel {
    public MockSendDataChannel(final long key, final ByteBuffer hdrData) {
        super(key, hdrData, 0);
    }

    public MockSendDataChannel(final long key, final ByteBuffer hdrData,
                             long connectTimeoutMsec) {
        super(key, hdrData, connectTimeoutMsec);
    }

    public MockSendDataChannel(final long key, final ByteBuffer hdrData,
                               long connectTimeoutMsec,
                               final long keepAliveTimeoutMsec,
                               final int keepAliveCount) {
        super(key, hdrData, connectTimeoutMsec, keepAliveTimeoutMsec,
                keepAliveCount);
    }

    @Override
    protected ByteBuf buildHdr(ChannelHandlerContext ctx) {
        return getHdr();
    }
}
