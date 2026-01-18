package com.learning.LearningLoadBalancer.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import io.netty.util.concurrent.PromiseCombiner;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class L4LoadBalancer {

    // curl -X POST -H "Content-Type: application/json" -d '{"key":"value23314231"}'
    // http://localhost:8080
    public static void main(String[] args) throws InterruptedException {
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        BackendConnectionPool backendConnectionPool =
                new BackendConnectionPool(new int[] {8081, 8082, 8083}, workerGroup);
        List<Future> connectFutures = backendConnectionPool.initConnections();
        EventLoop eventLoop = workerGroup.next();
        Promise<Void> aggregatePromise = eventLoop.newPromise();
        eventLoop.execute(
                () -> {
                    PromiseCombiner combiner = new PromiseCombiner(eventLoop);
                    for (Future future : connectFutures) {
                        combiner.add(future);
                    }
                    combiner.finish(aggregatePromise);
                });
        aggregatePromise.await();
        if (!aggregatePromise.isSuccess()) {
            log.error("Failed to connect to backend", aggregatePromise.cause());
            return;
        }
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
                                                            backendConnectionPool));
                                }
                            });
            b.bind(8080).sync().channel().closeFuture().sync();
        } finally {
            backendConnectionPool.shutdown();
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }
}
