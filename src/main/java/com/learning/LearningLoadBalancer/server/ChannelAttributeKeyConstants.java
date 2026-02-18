package com.learning.LearningLoadBalancer.server;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.AttributeKey;
import java.util.Queue;

public class ChannelAttributeKeyConstants {

    public static final AttributeKey<ChannelHandlerContext> DOWNSTREAM_CONTEXT_KEY =
            AttributeKey.valueOf("downstreamContext");
    public static final AttributeKey<ConnectionPool> CONNECTION_POOL_KEY =
            AttributeKey.valueOf("connectionPool");
    public static final AttributeKey<Channel> UPSTREAM_CHANNEL_KEY =
            AttributeKey.valueOf("upstreamChannel");
    // Add to ChannelAttributeKeyConstants
    public static final AttributeKey<Boolean> PROCESSING_KEY = AttributeKey.valueOf("processing");
    public static final AttributeKey<Queue> REQUEST_QUEUE = AttributeKey.valueOf("requestQueue");
}
