package com.learning.LearningLoadBalancer.server;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LoadBalancerInitializer extends ChannelInboundHandlerAdapter {

    private final BackendConnectionPool backendConnectionPool;

    public LoadBalancerInitializer(BackendConnectionPool backendConnectionPool) {
        this.backendConnectionPool = backendConnectionPool;
    }

    /**
     * @param ctx
     * @throws Exception
     */
    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        super.channelRegistered(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf byteBuf = (ByteBuf) msg;
        byte[] array = new byte[byteBuf.readableBytes()];
        byteBuf.getBytes(byteBuf.readerIndex(), array);
        log.info("Received request from downstream client: " + new String(array));
        ChannelFuture channelFuture = backendConnectionPool.writeToBackendChannel(ctx, byteBuf);
        channelFuture.addListener(
                (ChannelFutureListener)
                        future -> {
                            if (future.isSuccess()) {
                                log.info("Successfully sent request to upstream client");
                                ctx.channel().read();
                            } else {
                                log.info("Failed to send request to upstream client");
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
                                ctx.writeAndFlush(
                                                ctx.alloc()
                                                        .buffer()
                                                        .writeBytes(httpResponse.getBytes()))
                                        .addListener(ChannelFutureListener.CLOSE);
                            }
                        });
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        cause.printStackTrace();
        ctx.close();
    }
}
