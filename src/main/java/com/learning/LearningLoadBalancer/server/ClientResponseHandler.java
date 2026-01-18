package com.learning.LearningLoadBalancer.server;

import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ClientResponseHandler extends ChannelInboundHandlerAdapter {
    private final Queue<ChannelHandlerContext> pendingFrontendContexts =
            new ConcurrentLinkedQueue<>();

    public ClientResponseHandler() {}

    public void enqueueFrontEndContext(ChannelHandlerContext frontEndContext) {
        pendingFrontendContexts.offer(frontEndContext);
    }

    public void dequeFrontEndContext() {
        pendingFrontendContexts.poll();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ChannelHandlerContext frontEndContext = pendingFrontendContexts.poll();
        if (frontEndContext == null) {
            log.error("No pending frontend context for response, discarding");
            return;
        }
        frontEndContext
                .writeAndFlush(msg)
                .addListener(
                        (ChannelFutureListener)
                                future -> {
                                    if (future.isSuccess()) {
                                        log.info("Successfully send response to downstream client");
                                        ctx.channel().read();
                                    } else {
                                        log.error(
                                                "Failed to send response to downstreamclient",
                                                future.cause());
                                        future.channel().close();
                                    }
                                });
    }

    /**
     * @param ctx
     * @throws Exception
     */
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        this.pendingFrontendContexts.clear();
        super.channelInactive(ctx);
    }

    /**
     * @param ctx
     * @throws Exception
     */
    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        this.pendingFrontendContexts.clear();
        super.channelUnregistered(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        cause.printStackTrace();
        ctx.close();
    }
}
