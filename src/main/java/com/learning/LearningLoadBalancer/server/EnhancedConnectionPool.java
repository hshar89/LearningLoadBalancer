package com.learning.LearningLoadBalancer.server;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.pool.ChannelPoolHandler;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import io.netty.util.concurrent.Promise;
import io.netty.util.internal.ObjectUtil;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EnhancedConnectionPool extends SimpleConnectionPool {

    public enum AcquireTimeoutAction {
        NEW,
        FAIL;
    }

    private final int maxConnections;
    private final int maxPendingAcquires;
    private final long pendingAcquireTimeoutNano;
    private boolean poolClosed;
    private EventExecutor executor;
    private final Queue<AcquireTask> pendingAcquires = new ConcurrentLinkedDeque<>();
    private final AtomicInteger pendingAcquiresCount = new AtomicInteger(0);
    private final AtomicInteger acquiredChannelCount = new AtomicInteger(0);
    private TimeoutTask timeoutTask;

    public EnhancedConnectionPool(
            int minConnections,
            int maxConnections,
            int maxPendingAcquires,
            AcquireTimeoutAction acquireTimeoutAction,
            long pendingAcquireTimeoutMs,
            Bootstrap bootstrap,
            ChannelPoolHandler channelPoolHandler) {
        super(minConnections, bootstrap, channelPoolHandler);
        if (acquireTimeoutAction == null && pendingAcquireTimeoutMs == -1) {
            this.pendingAcquireTimeoutNano = -1;
        } else if (acquireTimeoutAction != null && pendingAcquireTimeoutMs == -1) {
            throw new NullPointerException("PendingAcquireTimeoutMs cannot be -1.");
        } else {
            this.pendingAcquireTimeoutNano = pendingAcquireTimeoutMs * 1000L * 1000L;
            this.executor = bootstrap.config().group().next();
            switch (acquireTimeoutAction) {
                case NEW:
                    timeoutTask =
                            new TimeoutTask() {
                                @Override
                                void onTimeout(AcquireTask acquireTask) {
                                    acquire(acquireTask.originalPromise);
                                }
                            };
                    break;
                case FAIL:
                    timeoutTask =
                            new TimeoutTask() {
                                @Override
                                void onTimeout(AcquireTask acquireTask) {
                                    acquireTask.originalPromise.setFailure(
                                            new RuntimeException(
                                                    "Failed to acquire. Pending Acquire timeout"));
                                }
                            };
                    break;
            }
        }
        this.maxConnections = maxConnections;
        this.maxPendingAcquires = maxPendingAcquires;
    }

    @Override
    public Future<Channel> acquire() {
        return acquire(bootstrap.config().group().next().newPromise());
    }

    private Future<Channel> tooManyPendingAcquires(Promise<Channel> promise) {
        return promise.setFailure(new Exception("Too many pending acquires"));
    }

    @Override
    public Future<Channel> acquire(Promise<Channel> promise) {
        try {
            if (this.acquiredChannelCount.get() < this.maxConnections) {
                assert acquiredChannelCount.get() >= 0;
                Promise<Channel> p = bootstrap.config().group().next().newPromise();
                AcquireListener acquireListener = new AcquireListener(promise);
                acquireListener.acquire();
                p.addListener(acquireListener);
                return super.acquire(p);
            } else {
                if (this.pendingAcquiresCount.get() > this.maxPendingAcquires) {
                    log.warn("Too many pending acquire");
                    return tooManyPendingAcquires(promise);
                }
                AcquireTask acquireTask = new AcquireTask(promise, pendingAcquireTimeoutNano);
                if (timeoutTask != null) {
                    ScheduledFuture<?> scheduledFuture =
                            executor.schedule(
                                    timeoutTask, pendingAcquireTimeoutNano, TimeUnit.NANOSECONDS);
                    acquireTask.setScheduledFuture(scheduledFuture);
                }
                this.pendingAcquires.add(acquireTask);
                return promise;
            }
        } catch (Throwable throwable) {
            promise.setFailure(new RuntimeException("Could not acquire from the pool"));
        }
        return promise;
    }

    @Override
    public Future<Void> release(Channel channel) {
        return release(channel, channel.eventLoop().newPromise());
    }

    @Override
    public Future<Void> release(Channel channel, Promise<Void> promise) {
        ObjectUtil.checkNotNull(promise, "promise");
        final Promise<Void> p = executor.newPromise();
        super.release(
                channel,
                p.addListener(
                        new FutureListener<Void>() {
                            @Override
                            public void operationComplete(Future<Void> future) {
                                try {
                                    assert executor.inEventLoop();
                                    if (poolClosed) {
                                        channel.close();
                                        promise.setFailure(
                                                new RuntimeException(
                                                        "DynamicChannelPool is closed"));
                                        return;
                                    }
                                    // The count is decremented after release because the
                                    // maxConnections only limits the number
                                    // of acquired connections. connections can sit in pool idle but
                                    // they cannot be acquired over a limit.
                                    if (future.isSuccess()) {
                                        promise.setSuccess(null);
                                        doDecrementAndTryPending();
                                    } else {
                                        Throwable cause = future.cause();
                                        if (!(cause instanceof IllegalArgumentException)) {
                                            doDecrementAndTryPending();
                                        }
                                        promise.setFailure(cause);
                                    }
                                } catch (Exception e) {

                                }
                            }
                        }));
        return promise;
    }

    @Override
    public void close() {
        this.poolClosed = true;
        this.pendingAcquires.clear();
    }

    private void doDecrementAndTryPending() {
        int count = this.acquiredChannelCount.decrementAndGet();
        assert count >= 0;
        tryAcquirePending();
    }

    private void tryAcquirePending() {
        while (this.acquiredChannelCount.get() < this.maxConnections) {
            AcquireTask acquireTask = pendingAcquires.poll();
            if (acquireTask == null) {
                break;
            }
            // The scheduled future is cancelled here. The false here means the scheduled future
            // will not be interrupted if it is already in progress
            ScheduledFuture<?> scheduledFuture = acquireTask.scheduledFuture;
            if (scheduledFuture != null) {
                scheduledFuture.cancel(false);
            }
            pendingAcquiresCount.decrementAndGet();
            acquireTask.acquire();
            super.acquire(acquireTask.originalPromise);
        }
    }

    private class AcquireTask extends AcquireListener {
        private final long timeoutInstant;
        @Setter ScheduledFuture<?> scheduledFuture;

        public AcquireTask(Promise<Channel> promise, long pendingAcquireTimeoutNano) {
            super(promise);
            this.timeoutInstant = System.nanoTime() + pendingAcquireTimeoutNano;
        }
    }

    private class AcquireListener implements FutureListener<Channel> {

        protected final Promise<Channel> originalPromise;
        private boolean acquired;

        public AcquireListener(Promise<Channel> originalPromise) {
            this.originalPromise = originalPromise;
        }

        @Override
        public void operationComplete(Future<Channel> channelFuture) throws Exception {
            if (poolClosed) {
                originalPromise.setFailure(new RuntimeException("Pool is closed"));
                return;
            }
            if (channelFuture.isSuccess()) {
                originalPromise.setSuccess(channelFuture.getNow());
            } else {
                if (acquired) {
                    doDecrementAndTryPending();
                } else {
                    tryAcquirePending();
                }
                originalPromise.setFailure(channelFuture.cause());
            }
        }

        public void acquire() {
            if (acquired) {
                return;
            }
            acquiredChannelCount.incrementAndGet();
            acquired = true;
        }
    }

    private abstract class TimeoutTask implements Runnable {

        @Override
        public void run() {
            while (!pendingAcquires.isEmpty()) {
                AcquireTask acquireTask = pendingAcquires.peek();
                if (acquireTask == null) {
                    break;
                }
                long currentTimeNano = System.nanoTime();
                if (currentTimeNano - acquireTask.timeoutInstant < 0) {
                    break;
                }
                pendingAcquires.poll();
                pendingAcquiresCount.decrementAndGet();
                onTimeout(acquireTask);
            }
        }

        abstract void onTimeout(AcquireTask acquireTask);
    }
}
