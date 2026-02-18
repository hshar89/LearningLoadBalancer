package com.learning.LearningLoadBalancer.server;

import static com.learning.LearningLoadBalancer.server.ChannelAttributeKeyConstants.CONNECTION_POOL_KEY;
import static com.learning.LearningLoadBalancer.server.ChannelAttributeKeyConstants.DOWNSTREAM_CONTEXT_KEY;

import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ClientResponseHandler extends ChannelInboundHandlerAdapter {

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        log.info(
                "Channel read called on response handler {}, {}",
                ctx.channel().id(),
                ctx.channel().isActive());

        ChannelHandlerContext downstreamContext = ctx.channel().attr(DOWNSTREAM_CONTEXT_KEY).get();
        ConnectionPool connectionPool = ctx.channel().attr(CONNECTION_POOL_KEY).get();

        if (downstreamContext == null || !downstreamContext.channel().isActive()) {
            log.error("No pending downstream context for response, discarding");
            if (connectionPool != null) {
                connectionPool.release(ctx.channel());
            }
            return;
        }
        downstreamContext
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
        log.info("Upstream Channel became inactive..");
        super.channelInactive(ctx);
    }

    /**
     * @param ctx
     * @throws Exception
     */
    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        super.channelUnregistered(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        cause.printStackTrace();
        ctx.close();
    }
}
