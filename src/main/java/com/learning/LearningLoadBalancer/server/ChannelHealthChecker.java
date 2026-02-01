package com.learning.LearningLoadBalancer.server;

import io.netty.channel.Channel;
import io.netty.channel.EventLoop;
import io.netty.util.concurrent.Future;

public interface ChannelHealthChecker {

    Future<Boolean> isHealthy(Channel channel);

    ChannelHealthChecker ACTIVE =
            new ChannelHealthChecker() {
                @Override
                public Future<Boolean> isHealthy(Channel channel) {
                    EventLoop loop = channel.eventLoop();
                    return channel.isActive()
                            ? loop.newSucceededFuture(Boolean.TRUE)
                            : loop.newSucceededFuture(Boolean.FALSE);
                }
            };
}
