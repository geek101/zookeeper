package org.apache.zookeeper.server.quorum.helpers;

import java.util.concurrent.Future;

import org.apache.zookeeper.server.quorum.Vote;
import org.apache.zookeeper.server.quorum.VoteViewBase;

public class VoteViewBaseWrapper extends VoteViewBase {
    public VoteViewBaseWrapper(final long mysid) {
        super(mysid);
    }
    public Future<Void> msgRxHelper(final Vote vote) {
        return super.msgRx(vote);
    }
}
