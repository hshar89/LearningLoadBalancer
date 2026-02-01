package com.learning.LearningLoadBalancer.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class L4LoadBalancer {

    // curl -X POST -H "Content-Type: application/json" -d '{"key":"value23314231"}'
    // http://localhost:8080
    public static void main(String[] args) throws InterruptedException {
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        UpstreamClientPool upstreamClientPool =
                new UpstreamClientPool(new int[] {8081, 8082, 8083}, workerGroup);
        upstreamClientPool.initConnections();
        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(
                            new ChannelInitializer<SocketChannel>() {
                                @Override
                                protected void initChannel(SocketChannel ch) {
                                    ch.pipeline()
                                            .addLast(
                                                    new LoadBalancerInitializer(
                                                            upstreamClientPool));
                                }
                            });
            b.bind(8080).sync().channel().closeFuture().sync();
        } finally {
            upstreamClientPool.shutdown();
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }
}
