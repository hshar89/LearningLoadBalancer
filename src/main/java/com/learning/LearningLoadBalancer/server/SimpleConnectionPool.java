package com.learning.LearningLoadBalancer.server;

import static io.netty.util.internal.ObjectUtil.checkNotNull;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoop;
import io.netty.channel.pool.ChannelPoolHandler;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import io.netty.util.concurrent.Promise;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SimpleConnectionPool implements ConnectionPool {

    protected final Bootstrap bootstrap;
    protected final Deque<Channel> channelDeque = new ConcurrentLinkedDeque<>();
    protected final ChannelPoolHandler channelPoolHandler;
    protected static final AttributeKey<SimpleConnectionPool> POOL_KEY =
            AttributeKey.valueOf("com.learning.LearningLoadBalancer.server.SimpleConnectionPool");

    public SimpleConnectionPool(
            int minPoolSize, Bootstrap bootstrap, ChannelPoolHandler channelPoolHandler) {
        // Cloning the original bootstrap to set own handler
        this.bootstrap = bootstrap.clone();
        this.bootstrap.handler(
                new ChannelInitializer<>() {
                    @Override
                    protected void initChannel(Channel ch) throws Exception {
                        assert ch.eventLoop().inEventLoop();
                        channelPoolHandler.channelCreated(ch);
                    }
                });
        this.channelPoolHandler = channelPoolHandler;
        warmUpConnectionPool(minPoolSize);
    }

    private List<Future> warmUpConnectionPool(int minPoolSize) {
        List<Future> connectFutures = new ArrayList<>();
        for (int i = 0; i < minPoolSize; i++) {
            Promise<Channel> promise = bootstrap.group().next().newPromise();
            Future<Channel> future = acquireChannelOrCreateOne(promise);
            future.addListener(
                    result -> {
                        if (result.isSuccess()) {
                            release((Channel) result.getNow());
                        }
                    });
            connectFutures.add(future);
        }
        return connectFutures;
    }

    public Future<Channel> acquireChannelOrCreateOne(final Promise<Channel> promise) {
        final Channel channel = pollChannel();
        if (channel == null) {
            log.info("No channel available in pool, creating new channel");
            Bootstrap cloned = bootstrap.clone();
            cloned.attr(POOL_KEY, this);
            ChannelFuture connectFuture = cloned.connect();
            if (connectFuture.isDone()) {
                notifyConnect(connectFuture, promise);
            } else {
                connectFuture.addListener(
                        new ChannelFutureListener() {
                            @Override
                            public void operationComplete(ChannelFuture future) {
                                notifyConnect(future, promise);
                            }
                        });
            }
            return promise;
        } else {
            EventLoop eventLoop = channel.eventLoop();
            if (eventLoop.inEventLoop()) {
                doHealthCheck(channel, promise);
            } else {
                eventLoop.execute(
                        new Runnable() {
                            @Override
                            public void run() {
                                doHealthCheck(channel, promise);
                            }
                        });
            }
        }
        return promise;
    }

    private void notifyConnect(ChannelFuture channelFuture, Promise<Channel> channelPromise) {
        try {
            Channel channel = null;
            if (channelFuture.isSuccess()) {
                channel = channelFuture.channel();
                channelPoolHandler.channelAcquired(channel);
                if (!channelPromise.trySuccess(channel)) {
                    release(channel);
                }
            } else {
                log.error("Failed to connect to backend", channelFuture.cause());
                channelPromise.tryFailure(channelFuture.cause());
            }
        } catch (Throwable t) {
            channelPromise.tryFailure(t);
        }
    }

    @Override
    public Future<Channel> acquire() {
        return acquireChannelOrCreateOne(bootstrap.config().group().next().newPromise());
    }

    @Override
    public Future<Channel> acquire(Promise<Channel> promise) {
        return acquireChannelOrCreateOne(promise);
    }

    @Override
    public Future<Void> release(Channel channel) {
        return release(channel, channel.eventLoop().newPromise());
    }

    @Override
    public Future<Void> release(Channel channel, Promise<Void> promise) {
        try {
            checkNotNull(channel, "channel");
            checkNotNull(promise, "promise");
            boolean inEventLoop = channel.eventLoop().inEventLoop();
            if (inEventLoop) {
                doReleaseChannel(channel, promise);
            } else {
                channel.eventLoop().execute(() -> doReleaseChannel(channel, promise));
            }
        } catch (Throwable throwable) {
            closeAndFail(channel, throwable, promise);
        }
        return promise;
    }

    @Override
    public void close() {
        for (Channel channel : channelDeque) {
            closeChannel(channel);
        }
    }

    private void doReleaseChannel(Channel channel, Promise<Void> promise) {
        try {
            assert channel.eventLoop().inEventLoop();
            ConnectionPool pool = channel.attr(POOL_KEY).getAndSet(null);
            if (pool != this) {
                closeAndFail(
                        channel,
                        new IllegalStateException("Channel not owned by this pool"),
                        promise);
            } else {
                releaseAndOffer(channel, promise);
            }
        } catch (Throwable cause) {
            closeAndFail(channel, cause, promise);
        }
    }

    private void releaseAndOffer(Channel channel, Promise<Void> promise) throws Exception {
        if (!channel.isActive()) {
            closeChannel(channel);
            promise.tryFailure(new RuntimeException("Channel is not active"));
            return;
        }
        if (offerChannel(channel)) {
            channelPoolHandler.channelReleased(channel);
            promise.setSuccess(null);
        } else {
            closeAndFail(channel, new IllegalStateException("Channel pool is full"), promise);
        }
    }

    private boolean offerChannel(Channel channel) {
        return channelDeque.offer(channel);
    }

    private Channel pollChannel() {
        return channelDeque.poll();
    }

    private void doHealthCheck(Channel channel, Promise<Channel> promise) {
        try {
            assert channel.eventLoop().inEventLoop();
            Future<Boolean> healthCheck = ChannelHealthChecker.ACTIVE.isHealthy(channel);
            if (healthCheck.isDone()) {
                notifyHealthCheck(healthCheck, channel, promise);
            } else {
                healthCheck.addListener(
                        new FutureListener<>() {
                            @Override
                            public void operationComplete(Future future) {
                                notifyHealthCheck(future, channel, promise);
                            }
                        });
            }
        } catch (Throwable throwable) {
            closeAndFail(channel, throwable, promise);
        }
    }

    private void notifyHealthCheck(
            Future<Boolean> healthCheck, Channel channel, Promise<Channel> promise) {
        try {
            assert channel.eventLoop().inEventLoop();
            if (healthCheck.isSuccess() && healthCheck.getNow()) {
                channel.attr(POOL_KEY).set(this);
                channelPoolHandler.channelAcquired(channel);
                promise.setSuccess(channel);
            } else {
                log.warn("Health check failed for channel {}. Closing and retrying", channel.id());
                closeChannel(channel);
                acquireChannelOrCreateOne(promise);
            }
        } catch (Throwable cause) {
            closeAndFail(channel, cause, promise);
        }
    }

    private void closeAndFail(Channel channel, Throwable cause, Promise<?> promise) {
        if (channel != null) {
            try {
                closeChannel(channel);
            } catch (Throwable t) {
                promise.tryFailure(t);
            }
        }
        promise.tryFailure(cause);
    }

    private void closeChannel(Channel channel) {
        channel.attr(POOL_KEY).getAndSet(null);
        channel.close();
    }
}
