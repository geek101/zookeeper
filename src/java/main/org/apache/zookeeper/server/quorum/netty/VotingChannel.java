/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.zookeeper.server.quorum.netty;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import org.apache.zookeeper.server.quorum.QuorumServer;
import org.apache.zookeeper.server.quorum.Vote;
import org.apache.zookeeper.server.quorum.util.Callback;
import org.apache.zookeeper.server.quorum.util.ChannelException;
import org.apache.zookeeper.server.quorum.util.InitMessageCtx;
import org.apache.zookeeper.server.quorum.util.NotNull;

/**
 * A duplex messaging channel that uses netty and provides abstractions
 * typically used by leader election and leader follower channels.
 * All exceptions lead to closing of the channel. Read timer used by read
 * message state machine. Connection timer helps with closing the connection
 * on timeout.
 */
public abstract class VotingChannel extends NettyChannel {
    protected final Long mySid;
    protected final InetSocketAddress myElectionAddr;
    private QuorumServer server ;
    private final Callback<Vote> msgRxCb;
    private ByteBuf voteBuf = Unpooled.buffer(Vote.getMsgHdrLen()
            + Integer.BYTES); // used by message
    // read state machine
    protected enum ReaderState {
        UNKNOWN, HDR, MSGLEN, MSG,
    }

    protected ReaderState readerState = ReaderState.UNKNOWN;

    private Vote voteSent;    /// The last Vote sent.
    private Vote voteRcv;     /// The last Vote received.

    private static final WriteListener WRITE_ERR_LISTENER = new WriteListener();

    private static final class WriteListener implements ChannelFutureListener {
        @Override
        public void operationComplete(final ChannelFuture future)
                throws InterruptedException {
            if (!future.isSuccess()) {
                // TODO: figure out a way to log this.
                future.channel().close();
            }
        }
    }

    /**
     * Constructor with timeout
     *
     * @param mysid              this server id
     * @param readMsgTimeoutMsec read message timeout in milleseconds.
     */
    public VotingChannel(final long mysid,
                         final InetSocketAddress myElectionAddr,
                         final QuorumServer server,
                         final long readMsgTimeoutMsec,
                         final long connectTimeoutMsec,
                         final long keepAliveTimeoutMsec,
                         final int keepAliveCount,
                         final Callback<Vote> msgRxCb) {
        super(TimeUnit.MILLISECONDS.toNanos(readMsgTimeoutMsec),
                TimeUnit.MILLISECONDS.toNanos(connectTimeoutMsec),
                TimeUnit.MILLISECONDS.toNanos(keepAliveTimeoutMsec),
                keepAliveCount);
        this.mySid = mysid;
        this.myElectionAddr = myElectionAddr;
        this.server = server;
        this.msgRxCb = msgRxCb;
    }

    /**
     * Constructor with timeout
     *
     * @param mysid              this server id
     * @param readMsgTimeoutMsec read message timeout in milleseconds.
     */
    public VotingChannel(final long mysid,
                         final InetSocketAddress myElectionAddr,
                         final QuorumServer server,
                         final long readMsgTimeoutMsec,
                         final long connectTimeoutMsec,
                         final Callback<Vote> msgRxCb) {
        this(mysid, myElectionAddr, server, readMsgTimeoutMsec,
                connectTimeoutMsec, 0, 0, msgRxCb);
    }

    @Override
    public Long key() {
        if (server == null) {
            return null;
        }

        return server.id();
    }

    protected void setServer(final QuorumServer server)
            throws ChannelException {
        if (this.server != null) {
            throw new ChannelException("Invalid server is already set to: "
                    + this.server);
        }
        this.server = server;
    }

    @Override
    protected void connected(final ChannelHandlerContext ctx)
            throws ChannelException {
        final InitMessageCtx initMessageCtx = new InitMessageCtx();
        ctx.channel().attr(InitMessageCtx.KEY).set(initMessageCtx);
        LOG.debug("connected");
    }

    @Override
    protected void disconnected(final ChannelHandlerContext ctx)
            throws ChannelException {
        LOG.debug("disconnected");
    }

    /**
     * Override for keepalive timeout for send side.
     * @throws ChannelException
     * @throws IOException
     */
    @Override
    protected void keepAliveSend(final ChannelHandlerContext ctx)
            throws ChannelException, IOException {
        NotNull.check(voteSent, "keepalive needs voteSent set.", LOG);
        ctx.writeAndFlush(voteSent.buildMsg());
    }

    @Override
    protected void readMsg(final ChannelHandlerContext ctx, final ByteBuf inBuf)
            throws ChannelException {
        if (readerState == ReaderState.MSGLEN) {
            if (inBuf.readableBytes() < Integer.BYTES) {
                return;
            }

            // Length received start the timer.
            resetReadTimer(ctx);

            // Read timer is set so delay keep alive timer.
            delayKeepAliveReadTimer();

            final int remainder = inBuf.readInt();
            if (remainder == 0) {
                final String errStr = "Invalid remainder: " + remainder;
                LOG.error(errStr);
                throw new ChannelException(errStr);
            }

            voteBuf.writeInt(remainder);

            readerState = ReaderState.MSG;
        } else if (readerState == ReaderState.MSG) {
            if (inBuf.readableBytes() < voteBuf.writableBytes()) {
                return; // not done yet.
            }

            // we have the full vote, write to it.
            inBuf.readBytes(voteBuf);

            // Store the message in the in-bound map.
            try {
                final Vote vote = Vote.buildVote(voteBuf, server.id());
                if (vote == null) {
                    throw new ChannelException(
                            "Invalid message received, closing channel");
                }

                /**
                 * TODO: Use this
                 */
                if (voteRcv == null ||
                        !vote.match(voteRcv)) {
                    msgRxCb.call(vote);
                    voteRcv = vote;
                }
            } catch (ChannelException | IOException exp) {
                LOG.error("Error processing msg from server: " + server);
                errClose(ctx);
            } finally {
                // Msg received hence stop the timer.
                stopReadTimer(ctx);

                // Finished reading a message so delay keep alive.
                delayKeepAliveReadTimer();

                voteBuf.clear();
                readerState = ReaderState.MSGLEN;
            }
        } else {
            LOG.error("Invalid data processing state: " + readerState);
            errClose(ctx);
        }
    }

    protected void resetReadTimer(final ChannelHandlerContext ctx) {
        try {
            resetReadTimer();
        } catch (ChannelException exp) {
            LOG.error("Err on reset of Read Timer exp: " + exp);
            return;
        }
    }

    protected void stopReadTimer(final ChannelHandlerContext ctx) {
        try {
            stopReadTimer();
        } catch (ChannelException exp) {
            LOG.error("Error on stop of Read Timer exp: " + exp);
            return;
        }
    }

    /**
     * .
     * Given a message write the data.
     */
    @Override
    public void write(final ChannelHandlerContext ctx, final Object msg,
                      final ChannelPromise promise) throws ChannelException {
        if (msg instanceof Vote) {
            startKeepAliveTimer(ctx);
            ctx.writeAndFlush(((Vote)msg).buildMsg(), promise)
                    .addListener(WRITE_ERR_LISTENER);
        } else {
            LOG.error("Invalid message: " + msg);
        }
    }

    /**
     * API to send message, will not be sent till hdr is sent.
     *
     * @param vote
     * @return
     */
    public boolean sendVote(final Vote vote) throws ChannelException {
        if (!isWriteReady()) {
            return false;
        }

        // Handle case where we are asked to send same message again.
        if (checkForSameVote(vote)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("asked to send same vote again: " + vote);
            }
            return true;
        }

        LOG.info("Sending msg: " + vote);
        super.sendMsg(vote);

        // Store the last message sent.
        voteSent = vote;
        return true;
    }

    protected boolean isWriteReady() {
        return readerState == ReaderState.MSGLEN ||
                readerState == ReaderState.MSG;
    }

    private boolean checkForSameVote(final Vote vote) {
        NotNull.check(vote, "vote is null, not supported", LOG);
        return voteSent != null && vote.match(voteSent);
    }
}
