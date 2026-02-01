package com.learning.LearningLoadBalancer.server;

import io.netty.channel.Channel;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;

public interface ConnectionPool {
    Future<Channel> acquire();

    Future<Channel> acquire(Promise<Channel> promise);

    Future<Void> release(Channel channel);

    Future<Void> release(Channel channel, Promise<Void> promise);

    void close();
}
