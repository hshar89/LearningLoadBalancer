package com.learning.LearningLoadBalancer.server;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.Future;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BackendConnectionPool {

    private final int[] backendPorts;
    private final AtomicLong currentIndex = new AtomicLong(0);
    ;
    private final ConcurrentMap<Integer, UpstreamChannel> backendChannels =
            new ConcurrentHashMap<>();
    private final ConcurrentMap<Integer, Semaphore> backendChannelsLocks =
            new ConcurrentHashMap<>();
    private final EventLoopGroup workerGroup;

    public BackendConnectionPool(int[] backendPorts, EventLoopGroup workerGroup) {
        this.backendPorts = backendPorts;
        this.workerGroup = workerGroup;
    }

    public List<Future> initConnections() {
        List<Future> connectFutures = new ArrayList<>();
        for (int port : backendPorts) {
            backendChannelsLocks.put(port, new Semaphore(1));
            connectFutures.add(connectToBackend(port));
        }
        workerGroup
                .next()
                .scheduleAtFixedRate(
                        () -> reestablishConnectionOnInactiveClients(), 5, 5, TimeUnit.SECONDS);
        return connectFutures;
    }

    private Future connectToBackend(int port) {
        final Semaphore lock = backendChannelsLocks.get(port);
        if (!lock.tryAcquire()) {
            return workerGroup
                    .next()
                    .newFailedFuture(
                            new IllegalStateException(
                                    "Connection already in progress for port " + port));
        }
        try {
            Bootstrap bootstrap = new Bootstrap();
            final ClientResponseHandler clientResponseHandler = new ClientResponseHandler();
            bootstrap
                    .group(workerGroup)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.SO_KEEPALIVE, true)
                    .handler(
                            new ChannelInitializer<SocketChannel>() {
                                @Override
                                protected void initChannel(SocketChannel ch) {
                                    ch.pipeline().addLast(clientResponseHandler);
                                    ch.pipeline().addLast(new ReconnectHandler(port));
                                }
                            });

            ChannelFuture connectFuture = bootstrap.connect("localhost", port);
            connectFuture.addListener(
                    future -> {
                        if (future.isSuccess()) {
                            backendChannels.put(
                                    port,
                                    new UpstreamChannel(
                                            connectFuture.channel(), clientResponseHandler, port));
                            log.info("Connected to backend on port {}", port);
                        } else {
                            log.error(
                                    "Failed to connect to backend on port {}",
                                    port,
                                    future.cause());
                        }
                        lock.release();
                    });
            return connectFuture;
        } catch (Exception ex) {
            lock.release();
            log.error("Failed to connect to backend on port {}", port, ex);
            return workerGroup.next().newFailedFuture(ex);
        }
    }

    private void reestablishConnectionOnInactiveClients() {
        log.info("Restablishing connection on inactive clients..");
        for (int port : backendPorts) {
            if (!backendChannels.containsKey(port)
                    || !backendChannels.get(port).channel.isActive()) {
                log.info("Restablishing connection to backend on port {}", port);
                connectToBackend(port);
            }
        }
    }

    public ChannelFuture writeToBackendChannel(ChannelHandlerContext ctx, ByteBuf byteBuf) {
        Optional<UpstreamChannel> selectedChannel = selectChannel();
        if (selectedChannel.isEmpty()) {
            log.error("No active backend channel found");
            return ctx.channel().newFailedFuture(new Exception("No active backend channel found"));
        }
        return writeToBackendChannelWithRetry(selectedChannel.get(), ctx, byteBuf, 3);
    }

    private Optional<UpstreamChannel> selectChannel() {
        int selectedPort;
        int startIdx = (int) (currentIndex.getAndIncrement() % backendPorts.length);
        for (int i = 0; i < backendPorts.length; i++) {
            int idx = (startIdx + i) % backendPorts.length;
            selectedPort = backendPorts[idx];
            UpstreamChannel channel = backendChannels.get(selectedPort);
            if (channel != null && channel.channel.isActive()) {
                return Optional.of(channel);
            }
        }
        return Optional.empty();
    }

    private ChannelFuture writeToBackendChannelWithRetry(
            UpstreamChannel selectedChannel,
            ChannelHandlerContext ctx,
            ByteBuf byteBuf,
            int retriesLeft) {
        ChannelPromise promise = ctx.channel().newPromise();

        if (selectedChannel.channel.isActive()) {
            selectedChannel.connectionCount.incrementAndGet();
            log.info("backend channel is active...");
            selectedChannel.clientResponseHandler.enqueueFrontEndContext(ctx);
            selectedChannel
                    .channel
                    .writeAndFlush(byteBuf.retain())
                    .addListener(
                            (ChannelFutureListener)
                                    future -> {
                                        if (future.isSuccess()) {
                                            promise.setSuccess();
                                        } else {
                                            promise.setFailure(future.cause());
                                            selectedChannel.clientResponseHandler
                                                    .dequeFrontEndContext();
                                        }
                                        selectedChannel.connectionCount.decrementAndGet();
                                        ReferenceCountUtil.release(byteBuf);
                                    });
        } else if (retriesLeft > 0) {
            log.info(
                    "backend channel is not active, reconnecting and retrying... ({} retries left)",
                    retriesLeft);
            connectToBackend(selectedChannel.port)
                    .addListener(
                            future -> {
                                if (future.isSuccess()) {
                                    writeToBackendChannelWithRetry(
                                                    backendChannels.get(selectedChannel.port),
                                                    ctx,
                                                    byteBuf,
                                                    retriesLeft - 1)
                                            .addListener(
                                                    (ChannelFutureListener)
                                                            retryFuture -> {
                                                                if (retryFuture.isSuccess()) {
                                                                    promise.setSuccess();
                                                                } else {
                                                                    promise.setFailure(
                                                                            retryFuture.cause());
                                                                }
                                                            });
                                } else {
                                    log.error("Reconnection failed", future.cause());
                                    ReferenceCountUtil.release(byteBuf);
                                    promise.setFailure(future.cause());
                                }
                            });
        } else {
            log.error("No retries left, backend channel is not active");
            ReferenceCountUtil.release(byteBuf);
            promise.setFailure(new Exception("Backend channel is not active after retries"));
        }
        return promise;
    }

    public void shutdown() {
        for (UpstreamChannel upstreamChannel : backendChannels.values()) {
            if (upstreamChannel.channel != null && upstreamChannel.channel.isActive()) {
                upstreamChannel.channel.close();
            }
        }
    }

    private static class UpstreamChannel {
        Channel channel;
        ClientResponseHandler clientResponseHandler;
        int port;
        final AtomicInteger connectionCount = new AtomicInteger(0);

        public UpstreamChannel(
                Channel channel, ClientResponseHandler clientResponseHandler, int port) {
            this.channel = channel;
            this.clientResponseHandler = clientResponseHandler;
            this.port = port;
        }
    }

    private class ReconnectHandler extends ChannelInboundHandlerAdapter {
        private final int port;

        public ReconnectHandler(int port) {
            this.port = port;
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            log.info("Backend channel inactive on port {}, scheduling reconnect...", port);
            ctx.channel().eventLoop().execute(() -> reconnect(port));
            super.channelInactive(ctx);
        }
    }

    private void reconnect(int port) {
        log.info("Reconnecting to backend on port {}...", port);
        connectToBackend(port);
    }
}
