package com.learning.LearningLoadBalancer.server;

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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UpstreamClientPool {

    private final int[] upstreamServerPorts;
    private final AtomicLong currentIndex = new AtomicLong(0);
    private final ConcurrentMap<Integer, SimpleConnectionPool> upstreamServerConnectionPool =
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
            SimpleConnectionPool connectionPool =
                    new SimpleConnectionPool(
                            2,
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
        ChannelPromise promise = downstreamChannelCtx.channel().newPromise();
        try {
            Future<Channel> selectedChannel =
                    acquireChannel(
                            (int) (currentIndex.getAndIncrement() % upstreamServerPorts.length),
                            upstreamServerPorts.length,
                            workerGroup.next().newPromise());
            if (selectedChannel.isDone()) {
                if (selectedChannel.isSuccess()) {
                    log.info("Channel is selected successfully");
                    writeToUpstreamChannel(
                            selectedChannel.get(), downstreamChannelCtx, byteBuf, promise);
                } else {
                    log.error("Failed to select channel", selectedChannel.cause());
                    promise.setFailure(selectedChannel.cause());
                }
            } else {
                selectedChannel.addListener(
                        result -> {
                            if (result.isSuccess()) {
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
                upstreamServerConnectionPool.get(selectedPort).acquireChannelOrCreateOne(promise);
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
            Channel selectedChannel,
            ChannelHandlerContext downstreamChannelCtx,
            ByteBuf byteBuf,
            ChannelPromise channelPromise) {
        // Set the downstream context and connection pool as channel attributes
        selectedChannel
                .attr(ClientResponseHandler.DOWNSTREAM_CONTEXT_KEY)
                .set(downstreamChannelCtx);
        selectedChannel
                .attr(ClientResponseHandler.CONNECTION_POOL_KEY)
                .set(
                        upstreamServerConnectionPool.get(
                                ((InetSocketAddress) selectedChannel.remoteAddress()).getPort()));

        selectedChannel
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
}
