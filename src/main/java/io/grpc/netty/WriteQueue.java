/*
 * Copyright 2015 The gRPC Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.grpc.netty;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPromise;
import io.perfmark.Link;
import io.perfmark.PerfMark;
import io.prometheus.client.Histogram;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.lang3.tuple.Pair;

/** A queue of pending writes to a {@link Channel} that is flushed as a single unit. */
class WriteQueue {

  // Dequeue in chunks, so we don't have to acquire the queue's log too often.
  @VisibleForTesting static final int DEQUE_CHUNK_SIZE = 128;

  /** {@link Runnable} used to schedule work onto the tail of the event loop. */
  private final Runnable later =
      new Runnable() {
        @Override
        public void run() {
          flush();
        }
      };

  private final Channel channel;
  private final Queue<Pair<QueuedCommand, Long>> queue;
  private final AtomicBoolean scheduled = new AtomicBoolean();
  private static final Logger logger = Logger.getLogger(WriteQueue.class.getName());

  public static final Histogram writeQueuePendingDuration =
      Histogram.build()
          .name("grpc_netty_write_queue_pending_duration_ms")
          .help("Pending duration of a task in the write queue.")
          .register();

  public static final Histogram writeQueueWaitBatchDuration =
      Histogram.build()
          .name("grpc_netty_write_queue_wait_batch_duration_seconds")
          .help("Duration of waiting a batch filled in the write queue.")
          .register();

  public static final Histogram writeQueueBatchSize =
      Histogram.build()
          .name("grpc_netty_write_queue_batch_size")
          .help("Number of tasks in a batch in the write queue.")
          .register();

  public static final Histogram writeQueueCmdRunDuration =
      Histogram.build()
          .name("grpc_netty_write_queue_cmd_run_duration_seconds")
          .help("Duration of a task execution in the write queue.")
          .labelNames("type")
          .register();

  public static final Histogram writeQueueChannelFlushDuration =
      Histogram.build()
          .name("grpc_netty_write_queue_channel_flush_duration_seconds")
          .help("Duration of a channel flush in the write queue.")
          .labelNames("phase")
          .register();

  public static final Histogram writeQueueFlushDuration =
      Histogram.build()
          .name("grpc_netty_write_queue_flush_duration_seconds")
          .help("Duration of a flush of the write queue.")
          .register();

  public static final Histogram perfmarkWriteQueueDuration =
      Histogram.build()
          .name("perfmark_write_queue_duration_seconds")
          .help("perfmark_write_queue_duration_seconds")
          .labelNames("type")
          .register();

  public WriteQueue(Channel channel) {
    this.channel = Preconditions.checkNotNull(channel, "channel");
    queue = new ConcurrentLinkedQueue<>();
  }

  /** Schedule a flush on the channel. */
  void scheduleFlush() {
    if (scheduled.compareAndSet(false, true)) {
      // Add the queue to the tail of the event loop so writes will be executed immediately
      // inside the event loop. Note DO NOT do channel.write outside the event loop as
      // it will not wake up immediately without a flush.
      channel.eventLoop().execute(later);
    }
  }

  /**
   * Enqueue a write command on the channel.
   *
   * @param command a write to be executed on the channel.
   * @param flush true if a flush of the write should be schedule, false if a later call to enqueue
   *     will schedule the flush.
   */
  @CanIgnoreReturnValue
  ChannelFuture enqueue(QueuedCommand command, boolean flush) {
    // Detect erroneous code that tries to reuse command objects.
    Preconditions.checkArgument(command.promise() == null, "promise must not be set on command");

    ChannelPromise promise = channel.newPromise();
    command.promise(promise);
    queue.add(Pair.of(command, System.nanoTime()));
    if (flush) {
      scheduleFlush();
    }
    return promise;
  }

  /**
   * Enqueue the runnable. It is not safe for another thread to queue an Runnable directly to the
   * event loop, because it will be out-of-order with writes. This method allows the Runnable to be
   * processed in-order with writes.
   */
  void enqueue(Runnable runnable, boolean flush) {
    Long now = System.nanoTime();
    queue.add(Pair.<QueuedCommand, Long>of(new RunnableCommand(runnable), now));
    if (flush) {
      scheduleFlush();
    }
  }

  /**
   * Executes enqueued work directly on the current thread. This can be used to trigger writes
   * before performing additional reads. Must be called from the event loop. This method makes no
   * guarantee that the work queue is empty when it returns.
   */
  void drainNow() {
    Preconditions.checkState(channel.eventLoop().inEventLoop(), "must be on the event loop");
    if (queue.peek() == null) {
      return;
    }
    flush();
  }

  /**
   * Process the queue of commands and dispatch them to the stream. This method is only called in
   * the event loop
   */
  private void flush() {
    Histogram.Timer flushTimer = writeQueueFlushDuration.startTimer();
    List<Record> batch = new ArrayList<>();

    PerfMark.startTask("WriteQueue.periodicFlush");
    Histogram.Timer periodicFlush =
        perfmarkWriteQueueDuration.labels("WriteQueue.periodicFlush").startTimer();

    long start = System.nanoTime();
    try {
      Pair<QueuedCommand, Long> item;
      int i = 0;
      boolean flushedOnce = false;
      Histogram.Timer waitBatchTimer = writeQueueWaitBatchDuration.startTimer();
      while ((item = queue.poll()) != null) {
        QueuedCommand cmd = item.getLeft();
        writeQueuePendingDuration.observe((System.nanoTime() - item.getRight()) / 1_000_000.0);

        Record cmdRecord = new Record(cmd.getClass().getSimpleName());
        Histogram.Timer cmdTimer =
            writeQueueCmdRunDuration.labels(cmd.getClass().getSimpleName()).startTimer();

        // Run the command
        cmd.run(channel);

        cmdTimer.observeDuration();
        cmdRecord.end();

        batch.add(cmdRecord);
        if (++i == DEQUE_CHUNK_SIZE) {
          waitBatchTimer.observeDuration();
          waitBatchTimer = writeQueueWaitBatchDuration.startTimer();
          i = 0;
          // Flush each chunk so we are releasing buffers periodically. In theory this loop
          // might never end as new events are continuously added to the queue, if we never
          // flushed in that case we would be guaranteed to OOM.
          PerfMark.startTask("WriteQueue.flush0");
          Histogram.Timer flush0 =
              perfmarkWriteQueueDuration.labels("WriteQueue.flush0").startTimer();
          Histogram.Timer channelFlushTimer =
              writeQueueChannelFlushDuration.labels("flush0").startTimer();
          Record flushRecord = new Record("flush0");
          try {
            channel.flush();
          } finally {
            writeQueueBatchSize.observe(DEQUE_CHUNK_SIZE);
            channelFlushTimer.observeDuration();
            flushRecord.end();
            batch.add(flushRecord);
            PerfMark.stopTask("WriteQueue.flush0");
            flush0.observeDuration();
          }
          flushedOnce = true;
        }
      }
      // Must flush at least once, even if there were no writes.
      if (i != 0 || !flushedOnce) {
        PerfMark.startTask("WriteQueue.flush1");
        Histogram.Timer flush1 =
            perfmarkWriteQueueDuration.labels("WriteQueue.flush1").startTimer();
        Histogram.Timer channelFlushTimer =
            writeQueueChannelFlushDuration.labels("flush1").startTimer();
        Record flushRecord = new Record("flush1");
        try {
          channel.flush();
        } finally {
          waitBatchTimer.observeDuration();
          writeQueueBatchSize.observe(i);
          channelFlushTimer.observeDuration();
          flushRecord.end();
          batch.add(flushRecord);
          PerfMark.stopTask("WriteQueue.flush1");
          flush1.observeDuration();
        }
      }
    } finally {
      PerfMark.stopTask("WriteQueue.periodicFlush");
      periodicFlush.observeDuration();
      flushTimer.observeDuration();
      if (System.nanoTime() - start > 50_000_000) {
        String msg = "Found slow batch. WriteQueue.flush: " + batch;
        logger.log(Level.WARNING, msg);
        System.out.println(msg);
      }
      // Mark the write as done, if the queue is non-empty after marking trigger a new write.
      scheduled.set(false);
      if (!queue.isEmpty()) {
        scheduleFlush();
      }
    }
  }

  private static class RunnableCommand implements QueuedCommand {

    private final Runnable runnable;
    private final Link link;

    public RunnableCommand(Runnable runnable) {
      this.link = PerfMark.linkOut();
      this.runnable = runnable;
    }

    @Override
    public final void promise(ChannelPromise promise) {
      throw new UnsupportedOperationException();
    }

    @Override
    public final ChannelPromise promise() {
      throw new UnsupportedOperationException();
    }

    @Override
    public final void run(Channel channel) {
      runnable.run();
    }

    @Override
    public Link getLink() {
      return link;
    }
  }

  abstract static class AbstractQueuedCommand implements QueuedCommand {

    private ChannelPromise promise;
    private final Link link;

    AbstractQueuedCommand() {
      this.link = PerfMark.linkOut();
    }

    @Override
    public final void promise(ChannelPromise promise) {
      this.promise = promise;
    }

    @Override
    public final ChannelPromise promise() {
      return promise;
    }

    @Override
    public final void run(Channel channel) {
      channel.write(this, promise);
    }

    @Override
    public Link getLink() {
      return link;
    }
  }

  /** Simple wrapper type around a command and its optional completion listener. */
  interface QueuedCommand {

    /** Returns the promise beeing notified of the success/failure of the write. */
    ChannelPromise promise();

    /** Sets the promise. */
    void promise(ChannelPromise promise);

    void run(Channel channel);

    Link getLink();
  }

  private static class Record {

    long start;
    long end;
    long durationMS;
    String name;
    static ThreadLocal<StringBuilder> builder = ThreadLocal.withInitial(() -> new StringBuilder());

    public Record(String name) {
      this.name = name;
      this.start = System.nanoTime();
    }

    public void end() {
      this.end = System.nanoTime();
      this.durationMS = (end - start) / 1_000_000;
    }

    public String toString() {
      StringBuilder sb = builder.get();
      sb.setLength(0);
      sb.append("Record{start=");
      sb.append(start);
      sb.append(", end=");
      sb.append(end);
      sb.append(", durationMS=");
      sb.append(durationMS);
      sb.append(", name='");
      sb.append(name);
      sb.append("'}");
      return sb.toString();
    }
  }
}
