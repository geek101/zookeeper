package org.apache.zookeeper.server.quorum.helpers;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.server.quorum.Vote;
import org.apache.zookeeper.server.quorum.VoteViewChangeConsumer;
import org.apache.zookeeper.server.quorum.util.Predicate;

public class VoteViewChangeConsumerWrapper extends VoteViewChangeConsumer {
    @Override
    public Collection<Vote> consume(int timeout, TimeUnit unit, Predicate<Collection<Vote>> changePredicate) throws InterruptedException {
        return null;
    }

    @Override
    public Collection<Vote> consume(int timeout, TimeUnit unit) throws InterruptedException {
        return null;
    }
}
