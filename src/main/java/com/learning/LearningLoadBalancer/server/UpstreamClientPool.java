package com.learning.LearningLoadBalancer.server;

import static com.learning.LearningLoadBalancer.server.ChannelAttributeKeyConstants.CONNECTION_POOL_KEY;
import static com.learning.LearningLoadBalancer.server.ChannelAttributeKeyConstants.DOWNSTREAM_CONTEXT_KEY;
import static com.learning.LearningLoadBalancer.server.ChannelAttributeKeyConstants.PROCESSING_KEY;
import static com.learning.LearningLoadBalancer.server.ChannelAttributeKeyConstants.REQUEST_QUEUE;
import static com.learning.LearningLoadBalancer.server.ChannelAttributeKeyConstants.UPSTREAM_CHANNEL_KEY;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.pool.ChannelPoolHandler;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UpstreamClientPool {

    private final int[] upstreamServerPorts;
    private final AtomicLong currentIndex = new AtomicLong(0);
    private final ConcurrentMap<Integer, ConnectionPool> upstreamServerConnectionPool =
            new ConcurrentHashMap<>();
    private final EventLoopGroup workerGroup;

    public UpstreamClientPool(int[] upstreamServerPorts, EventLoopGroup workerGroup) {
        this.upstreamServerPorts = upstreamServerPorts;
        this.workerGroup = workerGroup;
    }

    public void initConnections() {
        for (int port : upstreamServerPorts) {
            setupBackendPool(port);
        }
    }

    private void setupBackendPool(int port) {
        try {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap
                    .group(workerGroup)
                    .channel(NioSocketChannel.class)
                    .remoteAddress(new InetSocketAddress("localhost", port))
                    .option(ChannelOption.SO_KEEPALIVE, true);
            ConnectionPool connectionPool =
                    new EnhancedConnectionPool(
                            1,
                            4,
                            4,
                            EnhancedConnectionPool.AcquireTimeoutAction.FAIL,
                            1000,
                            bootstrap,
                            new ChannelPoolHandler() {
                                @Override
                                public void channelReleased(Channel ch) throws Exception {
                                    log.info("Channel is released..{}", ch.remoteAddress());
                                }

                                @Override
                                public void channelAcquired(Channel ch) throws Exception {
                                    log.info("Channel is acquired..{}", ch.remoteAddress());
                                }

                                @Override
                                public void channelCreated(Channel ch) throws Exception {
                                    ch.pipeline()
                                            .addLast(
                                                    "ClientResponseHandler",
                                                    new ClientResponseHandler());
                                }
                            });
            upstreamServerConnectionPool.put(port, connectionPool);
        } catch (Exception ex) {
            log.error("Failed to connect to backend on port {}", port, ex);
        }
    }

    public ChannelFuture writeToUpstreamChannel(
            ChannelHandlerContext downstreamChannelCtx, ByteBuf byteBuf) {
        Queue<RequestTask> newQueue = new LinkedList<>();
        Queue<RequestTask> existingQueue =
                downstreamChannelCtx.channel().attr(REQUEST_QUEUE).setIfAbsent(newQueue);
        ChannelPromise promise = downstreamChannelCtx.channel().newPromise();
        if (existingQueue != null) {
            synchronized (existingQueue) {
                Boolean isProcessing = downstreamChannelCtx.channel().attr(PROCESSING_KEY).get();
                if (isProcessing != null && isProcessing) {
                    existingQueue.add(new RequestTask(byteBuf, promise));
                    return promise;
                }
            }
        }
        processTask(downstreamChannelCtx, new RequestTask(byteBuf, promise));
        return promise;
    }

    private ChannelFuture processTask(
            ChannelHandlerContext downstreamChannelCtx, RequestTask requestTask) {
        downstreamChannelCtx.channel().attr(PROCESSING_KEY).set(true);
        ByteBuf byteBuf = requestTask.byteBuf;
        ChannelPromise promise = requestTask.promise;
        try {
            if (downstreamChannelCtx.channel().attr(UPSTREAM_CHANNEL_KEY).get() != null) {
                Channel upstreamChannel =
                        downstreamChannelCtx.channel().attr(UPSTREAM_CHANNEL_KEY).get();
                if (!upstreamChannel.isActive()) {
                    promise.tryFailure(new RuntimeException("Upstream channel is not active"));
                    return promise;
                }
                writeToUpstreamChannel(upstreamChannel, downstreamChannelCtx, byteBuf, promise);
            } else {
                Future<Channel> selectedChannel =
                        acquireChannel(
                                (int) (currentIndex.getAndIncrement() % upstreamServerPorts.length),
                                upstreamServerPorts.length,
                                workerGroup.next().newPromise());
                selectedChannel.addListener(
                        result -> {
                            if (result.isSuccess()) {
                                log.info(
                                        "Listener: Channel is selected successfully {}",
                                        selectedChannel.getNow().remoteAddress());
                                writeToUpstreamChannel(
                                        (Channel) result.getNow(),
                                        downstreamChannelCtx,
                                        byteBuf,
                                        promise);
                            } else {
                                log.error("Failed to select channel", result.cause());
                                promise.setFailure(result.cause());
                            }
                        });
            }
        } catch (Throwable throwable) {
            log.error("Failed to write to backend channel", throwable);
            promise.setFailure(throwable);
        }
        promise.addListener(
                result -> {
                    Queue<RequestTask> requestQueue =
                            downstreamChannelCtx.channel().attr(REQUEST_QUEUE).get();
                    synchronized (requestQueue) {
                        if (result.isSuccess() && requestQueue != null && !requestQueue.isEmpty()) {
                            RequestTask newTask = requestQueue.poll();
                            processTask(downstreamChannelCtx, newTask);
                        }
                        downstreamChannelCtx.channel().attr(PROCESSING_KEY).set(false);
                    }
                });
        return promise;
    }

    private Future<Channel> acquireChannel(
            int currentIndex, int maxTries, Promise<Channel> outerPromise) {
        if (maxTries == 0) {
            outerPromise.tryFailure(new RuntimeException("No server available to connect"));
            return outerPromise;
        }
        int selectedPort = upstreamServerPorts[currentIndex];
        Promise<Channel> promise = workerGroup.next().newPromise();
        Future<Channel> acquiredChannel =
                upstreamServerConnectionPool.get(selectedPort).acquire(promise);
        if (acquiredChannel.isDone()) {
            if (acquiredChannel.isSuccess()) {
                outerPromise.trySuccess(acquiredChannel.getNow());
                return outerPromise;
            } else {
                return acquireChannel(
                        (currentIndex + 1) % upstreamServerPorts.length,
                        maxTries - 1,
                        outerPromise);
            }
        } else {
            acquiredChannel.addListener(
                    channelFuture -> {
                        if (channelFuture.isSuccess()) {
                            outerPromise.trySuccess((Channel) channelFuture.getNow());
                        } else {
                            acquireChannel(
                                    (currentIndex + 1) % upstreamServerPorts.length,
                                    maxTries - 1,
                                    outerPromise);
                        }
                    });
        }
        return outerPromise;
    }

    private void writeToUpstreamChannel(
            Channel upstreamChannel,
            ChannelHandlerContext downstreamChannelCtx,
            ByteBuf byteBuf,
            ChannelPromise channelPromise) {
        // Set the downstream context and connection pool as channel attributes
        upstreamChannel.attr(DOWNSTREAM_CONTEXT_KEY).set(downstreamChannelCtx);
        ConnectionPool connectionPool =
                upstreamServerConnectionPool.get(
                        ((InetSocketAddress) upstreamChannel.remoteAddress()).getPort());
        upstreamChannel.attr(CONNECTION_POOL_KEY).set(connectionPool);
        downstreamChannelCtx.channel().attr(CONNECTION_POOL_KEY).set(connectionPool);
        downstreamChannelCtx.channel().attr(UPSTREAM_CHANNEL_KEY).set(upstreamChannel);

        upstreamChannel
                .writeAndFlush(byteBuf.retain())
                .addListener(
                        (ChannelFutureListener)
                                future -> {
                                    log.info(
                                            "Write to upstream is completed {}",
                                            future.isSuccess());
                                    if (future.isSuccess()) {
                                        channelPromise.setSuccess();
                                    } else {
                                        channelPromise.setFailure(future.cause());
                                    }
                                    ReferenceCountUtil.release(byteBuf);
                                });
    }

    public void shutdown() {
        for (ConnectionPool connectionPool : upstreamServerConnectionPool.values()) {
            connectionPool.close();
        }
    }

    public static class RequestTask {
        public final ByteBuf byteBuf;
        public final ChannelPromise promise;

        public RequestTask(ByteBuf byteBuf, ChannelPromise promise) {
            this.byteBuf = byteBuf;
            this.promise = promise;
        }
    }
}
