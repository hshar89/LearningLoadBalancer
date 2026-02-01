package com.learning.LearningLoadBalancer.server;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LoadBalancerInitializer extends ChannelInboundHandlerAdapter {

    private final UpstreamClientPool upstreamClientPool;

    public LoadBalancerInitializer(UpstreamClientPool upstreamClientPool) {
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
                        });
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.error("Exception in Load Balancer channel", cause);
        ctx.close();
    }
}
