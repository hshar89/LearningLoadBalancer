package com.learning.LearningLoadBalancer.server;

import static com.learning.LearningLoadBalancer.server.ChannelAttributeKeyConstants.CONNECTION_POOL_KEY;
import static com.learning.LearningLoadBalancer.server.ChannelAttributeKeyConstants.PROCESSING_KEY;
import static com.learning.LearningLoadBalancer.server.ChannelAttributeKeyConstants.REQUEST_QUEUE;
import static com.learning.LearningLoadBalancer.server.ChannelAttributeKeyConstants.UPSTREAM_CHANNEL_KEY;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;
import java.util.Queue;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LoadBalancerInboundHandler extends ChannelInboundHandlerAdapter {

    private final UpstreamClientPool upstreamClientPool;

    public LoadBalancerInboundHandler(UpstreamClientPool upstreamClientPool) {
        this.upstreamClientPool = upstreamClientPool;
    }

    @Override
    public void channelRead(ChannelHandlerContext downstreamHandlerCtx, Object msg) {
        ByteBuf byteBuf = (ByteBuf) msg;
        byte[] array = new byte[byteBuf.readableBytes()];
        byteBuf.getBytes(byteBuf.readerIndex(), array);
        log.info("Received request from downstream client: {}", new String(array));
        ChannelFuture channelFuture =
                upstreamClientPool.writeToUpstreamChannel(downstreamHandlerCtx, byteBuf);
        channelFuture.addListener(
                (ChannelFutureListener)
                        future -> {
                            if (future.isSuccess()) {
                                downstreamHandlerCtx.channel().read();
                            } else {
                                if (downstreamHandlerCtx.channel().isActive()) {
                                    String body = "Error when writing to upstream client";
                                    String httpResponse =
                                            "HTTP/1.1 503 Service Unavailable\r\n"
                                                    + "Content-Type: text/plain\r\n"
                                                    + "Content-Length: "
                                                    + body.length()
                                                    + "\r\n"
                                                    + "Connection: close\r\n"
                                                    + "\r\n"
                                                    + body;
                                    downstreamHandlerCtx
                                            .writeAndFlush(
                                                    downstreamHandlerCtx
                                                            .alloc()
                                                            .buffer()
                                                            .writeBytes(httpResponse.getBytes()))
                                            .addListener(ChannelFutureListener.CLOSE);
                                    ReferenceCountUtil.release(byteBuf);
                                }
                            }
                        });
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.error("Exception in Load Balancer channel", cause);
        ctx.close();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        log.info("Downstream Channel became inactive..");
        ConnectionPool connectionPool = ctx.channel().attr(CONNECTION_POOL_KEY).get();
        Channel upstreamChannel = ctx.channel().attr(UPSTREAM_CHANNEL_KEY).get();
        connectionPool
                .release(upstreamChannel)
                .addListener(
                        result -> {
                            if (!result.isSuccess()) {
                                log.error("Failed to release channel", result.cause());
                            }
                        });
        Queue<UpstreamClientPool.RequestTask> requestQueue =
                ctx.channel().attr(REQUEST_QUEUE).get();
        while (requestQueue != null && requestQueue.peek() != null) {
            UpstreamClientPool.RequestTask newTask = requestQueue.poll();
            newTask.promise.setFailure(new RuntimeException("Downstream channel became inactive"));
            ReferenceCountUtil.release(newTask.byteBuf);
        }
        // Clear processing flag when channel closes
        ctx.channel().attr(PROCESSING_KEY).set(null);
        super.channelInactive(ctx);
    }
}
