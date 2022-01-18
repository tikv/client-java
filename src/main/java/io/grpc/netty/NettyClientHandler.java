/*
 * Copyright 2014 The gRPC Authors
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

import static io.netty.handler.codec.http2.DefaultHttp2LocalFlowController.DEFAULT_WINDOW_UPDATE_RATIO;
import static io.netty.util.CharsetUtil.UTF_8;
import static io.netty.util.internal.ObjectUtil.checkNotNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Supplier;
import io.grpc.Attributes;
import io.grpc.ChannelLogger;
import io.grpc.InternalChannelz;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.internal.ClientStreamListener.RpcProgress;
import io.grpc.internal.ClientTransport.PingCallback;
import io.grpc.internal.GrpcAttributes;
import io.grpc.internal.GrpcUtil;
import io.grpc.internal.Http2Ping;
import io.grpc.internal.InUseStateAggregator;
import io.grpc.internal.KeepAliveManager;
import io.grpc.internal.TransportTracer;
import io.grpc.netty.GrpcHttp2HeadersUtils.GrpcHttp2ClientHeadersDecoder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http2.DecoratingHttp2FrameWriter;
import io.netty.handler.codec.http2.DefaultHttp2Connection;
import io.netty.handler.codec.http2.DefaultHttp2ConnectionDecoder;
import io.netty.handler.codec.http2.DefaultHttp2ConnectionEncoder;
import io.netty.handler.codec.http2.DefaultHttp2FrameReader;
import io.netty.handler.codec.http2.DefaultHttp2FrameWriter;
import io.netty.handler.codec.http2.DefaultHttp2LocalFlowController;
import io.netty.handler.codec.http2.DefaultHttp2RemoteFlowController;
import io.netty.handler.codec.http2.Http2CodecUtil;
import io.netty.handler.codec.http2.Http2Connection;
import io.netty.handler.codec.http2.Http2ConnectionAdapter;
import io.netty.handler.codec.http2.Http2ConnectionDecoder;
import io.netty.handler.codec.http2.Http2ConnectionEncoder;
import io.netty.handler.codec.http2.Http2Error;
import io.netty.handler.codec.http2.Http2Exception;
import io.netty.handler.codec.http2.Http2FlowController;
import io.netty.handler.codec.http2.Http2FrameAdapter;
import io.netty.handler.codec.http2.Http2FrameLogger;
import io.netty.handler.codec.http2.Http2FrameReader;
import io.netty.handler.codec.http2.Http2FrameWriter;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.http2.Http2HeadersDecoder;
import io.netty.handler.codec.http2.Http2InboundFrameLogger;
import io.netty.handler.codec.http2.Http2OutboundFrameLogger;
import io.netty.handler.codec.http2.Http2Settings;
import io.netty.handler.codec.http2.Http2Stream;
import io.netty.handler.codec.http2.Http2StreamVisitor;
import io.netty.handler.codec.http2.StreamBufferingEncoder;
import io.netty.handler.codec.http2.WeightedFairQueueByteDistributor;
import io.netty.handler.logging.LogLevel;
import io.perfmark.PerfMark;
import io.perfmark.Tag;
import io.prometheus.client.Histogram;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.Executor;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;
import org.tikv.common.util.HistogramUtils;

/**
 * Client-side Netty handler for GRPC processing. All event handlers are executed entirely within
 * the context of the Netty Channel thread.
 */
class NettyClientHandler extends AbstractNettyHandler {
  private static final Logger logger = Logger.getLogger(NettyClientHandler.class.getName());

  /**
   * A message that simply passes through the channel without any real processing. It is useful to
   * check if buffers have been drained and test the health of the channel in a single operation.
   */
  static final Object NOOP_MESSAGE = new Object();

  /** Status used when the transport has exhausted the number of streams. */
  private static final Status EXHAUSTED_STREAMS_STATUS =
      Status.UNAVAILABLE.withDescription("Stream IDs have been exhausted");

  private static final long USER_PING_PAYLOAD = 1111;

  private final Http2Connection.PropertyKey streamKey;
  private final ClientTransportLifecycleManager lifecycleManager;
  private final KeepAliveManager keepAliveManager;
  // Returns new unstarted stopwatches
  private final Supplier<Stopwatch> stopwatchFactory;
  private final TransportTracer transportTracer;
  private final Attributes eagAttributes;
  private final String authority;
  private final InUseStateAggregator<Http2Stream> inUseState =
      new InUseStateAggregator<Http2Stream>() {
        @Override
        protected void handleInUse() {
          lifecycleManager.notifyInUse(true);
        }

        @Override
        protected void handleNotInUse() {
          lifecycleManager.notifyInUse(false);
        }
      };

  private WriteQueue clientWriteQueue;
  private Http2Ping ping;
  private Attributes attributes;
  private InternalChannelz.Security securityInfo;
  private Status abruptGoAwayStatus;
  private Status channelInactiveReason;

  public static final Histogram createStreamWriteHeaderDuration =
      HistogramUtils.buildDuration()
          .name("grpc_netty_client_stream_write_header_duration_seconds")
          .help("Time taken to write headers for a stream in seconds.")
          .register();

  public static final Histogram createStreamAddListenerDuration =
      HistogramUtils.buildDuration()
          .name("grpc_netty_client_stream_add_listener_duration_seconds")
          .help("Time taken to add listener for a stream future in seconds.")
          .register();

  public static final Histogram createStreamCreateNewFuture =
      HistogramUtils.buildDuration()
          .name("grpc_netty_client_stream_create_future_duration_seconds")
          .help("Time taken to create new stream future in seconds.")
          .register();

  public static final Histogram perfmarkNettyClientHandlerDuration =
      HistogramUtils.buildDuration()
          .name("perfmark_netty_client_handler_duration_seconds")
          .help("Perfmark netty client handler duration seconds")
          .labelNames("type")
          .register();

  static NettyClientHandler newHandler(
      ClientTransportLifecycleManager lifecycleManager,
      @Nullable KeepAliveManager keepAliveManager,
      boolean autoFlowControl,
      int flowControlWindow,
      int maxHeaderListSize,
      Supplier<Stopwatch> stopwatchFactory,
      Runnable tooManyPingsRunnable,
      TransportTracer transportTracer,
      Attributes eagAttributes,
      String authority,
      ChannelLogger negotiationLogger) {
    Preconditions.checkArgument(maxHeaderListSize > 0, "maxHeaderListSize must be positive");
    Http2HeadersDecoder headersDecoder = new GrpcHttp2ClientHeadersDecoder(maxHeaderListSize);
    Http2FrameReader frameReader = new DefaultHttp2FrameReader(headersDecoder);
    Http2FrameWriter frameWriter = new DefaultHttp2FrameWriter();
    Http2Connection connection = new DefaultHttp2Connection(false);
    WeightedFairQueueByteDistributor dist = new WeightedFairQueueByteDistributor(connection);
    dist.allocationQuantum(16 * 1024); // Make benchmarks fast again.
    DefaultHttp2RemoteFlowController controller =
        new DefaultHttp2RemoteFlowController(connection, dist);
    connection.remote().flowController(controller);

    return newHandler(
        connection,
        frameReader,
        frameWriter,
        lifecycleManager,
        keepAliveManager,
        autoFlowControl,
        flowControlWindow,
        maxHeaderListSize,
        stopwatchFactory,
        tooManyPingsRunnable,
        transportTracer,
        eagAttributes,
        authority,
        negotiationLogger);
  }

  @VisibleForTesting
  static NettyClientHandler newHandler(
      final Http2Connection connection,
      Http2FrameReader frameReader,
      Http2FrameWriter frameWriter,
      ClientTransportLifecycleManager lifecycleManager,
      KeepAliveManager keepAliveManager,
      boolean autoFlowControl,
      int flowControlWindow,
      int maxHeaderListSize,
      Supplier<Stopwatch> stopwatchFactory,
      Runnable tooManyPingsRunnable,
      TransportTracer transportTracer,
      Attributes eagAttributes,
      String authority,
      ChannelLogger negotiationLogger) {
    Preconditions.checkNotNull(connection, "connection");
    Preconditions.checkNotNull(frameReader, "frameReader");
    Preconditions.checkNotNull(lifecycleManager, "lifecycleManager");
    Preconditions.checkArgument(flowControlWindow > 0, "flowControlWindow must be positive");
    Preconditions.checkArgument(maxHeaderListSize > 0, "maxHeaderListSize must be positive");
    Preconditions.checkNotNull(stopwatchFactory, "stopwatchFactory");
    Preconditions.checkNotNull(tooManyPingsRunnable, "tooManyPingsRunnable");
    Preconditions.checkNotNull(eagAttributes, "eagAttributes");
    Preconditions.checkNotNull(authority, "authority");

    Http2FrameLogger frameLogger = new Http2FrameLogger(LogLevel.DEBUG, NettyClientHandler.class);
    frameReader = new Http2InboundFrameLogger(frameReader, frameLogger);
    frameWriter = new Http2OutboundFrameLogger(frameWriter, frameLogger);

    PingCountingFrameWriter pingCounter;
    frameWriter = pingCounter = new PingCountingFrameWriter(frameWriter);

    StreamBufferingEncoder encoder =
        new StreamBufferingEncoder(new DefaultHttp2ConnectionEncoder(connection, frameWriter));

    // Create the local flow controller configured to auto-refill the connection window.
    connection
        .local()
        .flowController(
            new DefaultHttp2LocalFlowController(connection, DEFAULT_WINDOW_UPDATE_RATIO, true));

    Http2ConnectionDecoder decoder =
        new DefaultHttp2ConnectionDecoder(connection, encoder, frameReader);

    transportTracer.setFlowControlWindowReader(
        new TransportTracer.FlowControlReader() {
          final Http2FlowController local = connection.local().flowController();
          final Http2FlowController remote = connection.remote().flowController();

          @Override
          public TransportTracer.FlowControlWindows read() {
            return new TransportTracer.FlowControlWindows(
                local.windowSize(connection.connectionStream()),
                remote.windowSize(connection.connectionStream()));
          }
        });

    Http2Settings settings = new Http2Settings();
    settings.pushEnabled(false);
    settings.initialWindowSize(flowControlWindow);
    settings.maxConcurrentStreams(0);
    settings.maxHeaderListSize(maxHeaderListSize);

    return new NettyClientHandler(
        decoder,
        encoder,
        settings,
        negotiationLogger,
        lifecycleManager,
        keepAliveManager,
        stopwatchFactory,
        tooManyPingsRunnable,
        transportTracer,
        eagAttributes,
        authority,
        autoFlowControl,
        pingCounter);
  }

  private NettyClientHandler(
      Http2ConnectionDecoder decoder,
      Http2ConnectionEncoder encoder,
      Http2Settings settings,
      ChannelLogger negotiationLogger,
      ClientTransportLifecycleManager lifecycleManager,
      KeepAliveManager keepAliveManager,
      Supplier<Stopwatch> stopwatchFactory,
      final Runnable tooManyPingsRunnable,
      TransportTracer transportTracer,
      Attributes eagAttributes,
      String authority,
      boolean autoFlowControl,
      PingLimiter pingLimiter) {
    super(
        /* channelUnused= */ null,
        decoder,
        encoder,
        settings,
        negotiationLogger,
        autoFlowControl,
        pingLimiter);
    this.lifecycleManager = lifecycleManager;
    this.keepAliveManager = keepAliveManager;
    this.stopwatchFactory = stopwatchFactory;
    this.transportTracer = Preconditions.checkNotNull(transportTracer);
    this.eagAttributes = eagAttributes;
    this.authority = authority;
    this.attributes =
        Attributes.newBuilder().set(GrpcAttributes.ATTR_CLIENT_EAG_ATTRS, eagAttributes).build();

    // Set the frame listener on the decoder.
    decoder().frameListener(new FrameListener());

    Http2Connection connection = encoder.connection();
    streamKey = connection.newKey();

    connection.addListener(
        new Http2ConnectionAdapter() {
          @Override
          public void onGoAwayReceived(int lastStreamId, long errorCode, ByteBuf debugData) {
            byte[] debugDataBytes = ByteBufUtil.getBytes(debugData);
            goingAway(errorCode, debugDataBytes);
            if (errorCode == Http2Error.ENHANCE_YOUR_CALM.code()) {
              String data = new String(debugDataBytes, UTF_8);
              logger.log(
                  Level.WARNING, "Received GOAWAY with ENHANCE_YOUR_CALM. Debug data: {0}", data);
              if ("too_many_pings".equals(data)) {
                tooManyPingsRunnable.run();
              }
            }
          }

          @Override
          public void onStreamActive(Http2Stream stream) {
            if (connection().numActiveStreams() == 1
                && NettyClientHandler.this.keepAliveManager != null) {
              NettyClientHandler.this.keepAliveManager.onTransportActive();
            }
          }

          @Override
          public void onStreamClosed(Http2Stream stream) {
            // Although streams with CALL_OPTIONS_RPC_OWNED_BY_BALANCER are not marked as "in-use"
            // in
            // the first place, we don't propagate that option here, and it's safe to reset the
            // in-use
            // state for them, which will be a cheap no-op.
            inUseState.updateObjectInUse(stream, false);
            if (connection().numActiveStreams() == 0
                && NettyClientHandler.this.keepAliveManager != null) {
              NettyClientHandler.this.keepAliveManager.onTransportIdle();
            }
          }
        });
  }

  /**
   * The protocol negotiation attributes, available once the protocol negotiation completes;
   * otherwise returns {@code Attributes.EMPTY}.
   */
  Attributes getAttributes() {
    return attributes;
  }

  /** Handler for commands sent from the stream. */
  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
      throws Exception {
    if (msg instanceof CreateStreamCommand) {
      createStream((CreateStreamCommand) msg, promise);
    } else if (msg instanceof SendGrpcFrameCommand) {
      sendGrpcFrame(ctx, (SendGrpcFrameCommand) msg, promise);
    } else if (msg instanceof CancelClientStreamCommand) {
      cancelStream(ctx, (CancelClientStreamCommand) msg, promise);
    } else if (msg instanceof SendPingCommand) {
      sendPingFrame(ctx, (SendPingCommand) msg, promise);
    } else if (msg instanceof GracefulCloseCommand) {
      gracefulClose(ctx, (GracefulCloseCommand) msg, promise);
    } else if (msg instanceof ForcefulCloseCommand) {
      forcefulClose(ctx, (ForcefulCloseCommand) msg, promise);
    } else if (msg == NOOP_MESSAGE) {
      ctx.write(Unpooled.EMPTY_BUFFER, promise);
    } else {
      throw new AssertionError("Write called for unexpected type: " + msg.getClass().getName());
    }
  }

  void startWriteQueue(Channel channel) {
    clientWriteQueue = new WriteQueue(channel);
  }

  WriteQueue getWriteQueue() {
    return clientWriteQueue;
  }

  ClientTransportLifecycleManager getLifecycleManager() {
    return lifecycleManager;
  }

  /** Returns the given processed bytes back to inbound flow control. */
  void returnProcessedBytes(Http2Stream stream, int bytes) {
    try {
      decoder().flowController().consumeBytes(stream, bytes);
    } catch (Http2Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void onHeadersRead(int streamId, Http2Headers headers, boolean endStream) {
    // Stream 1 is reserved for the Upgrade response, so we should ignore its headers here:
    if (streamId != Http2CodecUtil.HTTP_UPGRADE_STREAM_ID) {
      NettyClientStream.TransportState stream = clientStream(requireHttp2Stream(streamId));
      PerfMark.event("NettyClientHandler.onHeadersRead", stream.tag());
      stream.transportHeadersReceived(headers, endStream);
    }

    if (keepAliveManager != null) {
      keepAliveManager.onDataReceived();
    }
  }

  /** Handler for an inbound HTTP/2 DATA frame. */
  private void onDataRead(int streamId, ByteBuf data, int padding, boolean endOfStream) {
    flowControlPing().onDataRead(data.readableBytes(), padding);
    NettyClientStream.TransportState stream = clientStream(requireHttp2Stream(streamId));
    PerfMark.event("NettyClientHandler.onDataRead", stream.tag());
    stream.transportDataReceived(data, endOfStream);
    if (keepAliveManager != null) {
      keepAliveManager.onDataReceived();
    }
  }

  /** Handler for an inbound HTTP/2 RST_STREAM frame, terminating a stream. */
  private void onRstStreamRead(int streamId, long errorCode) {
    NettyClientStream.TransportState stream = clientStream(connection().stream(streamId));
    if (stream != null) {
      PerfMark.event("NettyClientHandler.onRstStreamRead", stream.tag());
      Status status = statusFromH2Error(null, "RST_STREAM closed stream", errorCode, null);
      stream.transportReportStatus(
          status,
          errorCode == Http2Error.REFUSED_STREAM.code()
              ? RpcProgress.REFUSED
              : RpcProgress.PROCESSED,
          false /*stop delivery*/,
          new Metadata());
      if (keepAliveManager != null) {
        keepAliveManager.onDataReceived();
      }
    }
  }

  @Override
  public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
    logger.fine("Network channel being closed by the application.");
    if (ctx.channel().isActive()) { // Ignore notification that the socket was closed
      lifecycleManager.notifyShutdown(
          Status.UNAVAILABLE.withDescription("Transport closed for unknown reason"));
    }
    super.close(ctx, promise);
  }

  /** Handler for the Channel shutting down. */
  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    try {
      logger.fine("Network channel is closed");
      Status status = Status.UNAVAILABLE.withDescription("Network closed for unknown reason");
      lifecycleManager.notifyShutdown(status);
      final Status streamStatus;
      if (channelInactiveReason != null) {
        streamStatus = channelInactiveReason;
      } else {
        streamStatus = lifecycleManager.getShutdownStatus();
      }
      try {
        cancelPing(lifecycleManager.getShutdownThrowable());
        // Report status to the application layer for any open streams
        connection()
            .forEachActiveStream(
                new Http2StreamVisitor() {
                  @Override
                  public boolean visit(Http2Stream stream) throws Http2Exception {
                    NettyClientStream.TransportState clientStream = clientStream(stream);
                    if (clientStream != null) {
                      clientStream.transportReportStatus(streamStatus, false, new Metadata());
                    }
                    return true;
                  }
                });
      } finally {
        lifecycleManager.notifyTerminated(status);
      }
    } finally {
      // Close any open streams
      super.channelInactive(ctx);
      if (keepAliveManager != null) {
        keepAliveManager.onTransportTermination();
      }
    }
  }

  @Override
  public void handleProtocolNegotiationCompleted(
      Attributes attributes, InternalChannelz.Security securityInfo) {
    this.attributes = this.attributes.toBuilder().setAll(attributes).build();
    this.securityInfo = securityInfo;
    super.handleProtocolNegotiationCompleted(attributes, securityInfo);
    writeBufferingAndRemove(ctx().channel());
  }

  static void writeBufferingAndRemove(Channel channel) {
    checkNotNull(channel, "channel");
    ChannelHandlerContext handlerCtx =
        channel.pipeline().context(WriteBufferingAndExceptionHandler.class);
    if (handlerCtx == null) {
      return;
    }
    ((WriteBufferingAndExceptionHandler) handlerCtx.handler()).writeBufferedAndRemove(handlerCtx);
  }

  @Override
  public Attributes getEagAttributes() {
    return eagAttributes;
  }

  @Override
  public String getAuthority() {
    return authority;
  }

  InternalChannelz.Security getSecurityInfo() {
    return securityInfo;
  }

  @Override
  protected void onConnectionError(
      ChannelHandlerContext ctx, boolean outbound, Throwable cause, Http2Exception http2Ex) {
    logger.log(Level.FINE, "Caught a connection error", cause);
    lifecycleManager.notifyShutdown(Utils.statusFromThrowable(cause));
    // Parent class will shut down the Channel
    super.onConnectionError(ctx, outbound, cause, http2Ex);
  }

  @Override
  protected void onStreamError(
      ChannelHandlerContext ctx,
      boolean outbound,
      Throwable cause,
      Http2Exception.StreamException http2Ex) {
    // Close the stream with a status that contains the cause.
    NettyClientStream.TransportState stream = clientStream(connection().stream(http2Ex.streamId()));
    if (stream != null) {
      stream.transportReportStatus(Utils.statusFromThrowable(cause), false, new Metadata());
    } else {
      logger.log(Level.FINE, "Stream error for unknown stream " + http2Ex.streamId(), cause);
    }

    // Delegate to the base class to send a RST_STREAM.
    super.onStreamError(ctx, outbound, cause, http2Ex);
  }

  @Override
  protected boolean isGracefulShutdownComplete() {
    // Only allow graceful shutdown to complete after all pending streams have completed.
    return super.isGracefulShutdownComplete()
        && ((StreamBufferingEncoder) encoder()).numBufferedStreams() == 0;
  }

  /**
   * Attempts to create a new stream from the given command. If there are too many active streams,
   * the creation request is queued.
   */
  private void createStream(CreateStreamCommand command, ChannelPromise promise) throws Exception {
    if (lifecycleManager.getShutdownThrowable() != null) {
      command.stream().setNonExistent();
      // The connection is going away (it is really the GOAWAY case),
      // just terminate the stream now.
      command
          .stream()
          .transportReportStatus(
              lifecycleManager.getShutdownStatus(), RpcProgress.REFUSED, true, new Metadata());
      promise.setFailure(lifecycleManager.getShutdownThrowable());
      return;
    }

    // Get the stream ID for the new stream.
    int streamId;
    try {
      streamId = incrementAndGetNextStreamId();
    } catch (StatusException e) {
      command.stream().setNonExistent();
      // Stream IDs have been exhausted for this connection. Fail the promise immediately.
      promise.setFailure(e);

      // Initiate a graceful shutdown if we haven't already.
      if (!connection().goAwaySent()) {
        logger.fine(
            "Stream IDs have been exhausted for this connection. "
                + "Initiating graceful shutdown of the connection.");
        lifecycleManager.notifyShutdown(e.getStatus());
        close(ctx(), ctx().newPromise());
      }
      return;
    }
    if (connection().goAwayReceived()) {
      Status s = abruptGoAwayStatus;
      int maxActiveStreams = connection().local().maxActiveStreams();
      int lastStreamId = connection().local().lastStreamKnownByPeer();
      if (s == null) {
        // Should be impossible, but handle pseudo-gracefully
        s =
            Status.INTERNAL.withDescription(
                "Failed due to abrupt GOAWAY, but can't find GOAWAY details");
      } else if (streamId > lastStreamId) {
        s =
            s.augmentDescription(
                "stream id: " + streamId + ", GOAWAY Last-Stream-ID:" + lastStreamId);
      } else if (connection().local().numActiveStreams() == maxActiveStreams) {
        s = s.augmentDescription("At MAX_CONCURRENT_STREAMS limit. limit: " + maxActiveStreams);
      }
      if (streamId > lastStreamId || connection().local().numActiveStreams() == maxActiveStreams) {
        // This should only be reachable during onGoAwayReceived, as otherwise
        // getShutdownThrowable() != null
        command.stream().setNonExistent();
        command.stream().transportReportStatus(s, RpcProgress.REFUSED, true, new Metadata());
        promise.setFailure(s.asRuntimeException());
        return;
      }
    }

    NettyClientStream.TransportState stream = command.stream();
    Http2Headers headers = command.headers();
    stream.setId(streamId);

    PerfMark.startTask("NettyClientHandler.createStream", stream.tag());
    Histogram.Timer createStream =
        perfmarkNettyClientHandlerDuration.labels("NettyClientHandler.createStream").startTimer();
    PerfMark.linkIn(command.getLink());
    try {
      createStreamTraced(
          streamId, stream, headers, command.isGet(), command.shouldBeCountedForInUse(), promise);
    } finally {
      PerfMark.stopTask("NettyClientHandler.createStream", stream.tag());
      createStream.observeDuration();
    }
  }

  private void createStreamTraced(
      final int streamId,
      final NettyClientStream.TransportState stream,
      final Http2Headers headers,
      boolean isGet,
      final boolean shouldBeCountedForInUse,
      final ChannelPromise promise) {
    // Create an intermediate promise so that we can intercept the failure reported back to the
    // application.
    Histogram.Timer createFutureTimer = createStreamCreateNewFuture.startTimer();
    ChannelPromise tempPromise = ctx().newPromise();
    createFutureTimer.observeDuration();

    Histogram.Timer writeHeaderTimer = createStreamWriteHeaderDuration.startTimer();
    ChannelFuture future = encoder().writeHeaders(ctx(), streamId, headers, 0, isGet, tempPromise);
    writeHeaderTimer.observeDuration();

    Histogram.Timer addListenerTimer = createStreamAddListenerDuration.startTimer();
    future.addListener(
        new ChannelFutureListener() {
          @Override
          public void operationComplete(ChannelFuture future) throws Exception {
            if (future.isSuccess()) {
              // The http2Stream will be null in case a stream buffered in the encoder
              // was canceled via RST_STREAM.
              Http2Stream http2Stream = connection().stream(streamId);
              if (http2Stream != null) {
                stream.getStatsTraceContext().clientOutboundHeaders();
                http2Stream.setProperty(streamKey, stream);

                // This delays the in-use state until the I/O completes, which technically may
                // be later than we would like.
                if (shouldBeCountedForInUse) {
                  inUseState.updateObjectInUse(http2Stream, true);
                }

                // Attach the client stream to the HTTP/2 stream object as user data.
                stream.setHttp2Stream(http2Stream);
              }
              // Otherwise, the stream has been cancelled and Netty is sending a
              // RST_STREAM frame which causes it to purge pending writes from the
              // flow-controller and delete the http2Stream. The stream listener has already
              // been notified of cancellation so there is nothing to do.

              // Just forward on the success status to the original promise.
              promise.setSuccess();
            } else {
              final Throwable cause = future.cause();
              if (cause instanceof StreamBufferingEncoder.Http2GoAwayException) {
                StreamBufferingEncoder.Http2GoAwayException e =
                    (StreamBufferingEncoder.Http2GoAwayException) cause;
                Status status =
                    statusFromH2Error(
                        Status.Code.UNAVAILABLE,
                        "GOAWAY closed buffered stream",
                        e.errorCode(),
                        e.debugData());
                stream.transportReportStatus(status, RpcProgress.REFUSED, true, new Metadata());
                promise.setFailure(status.asRuntimeException());
              } else {
                promise.setFailure(cause);
              }
            }
          }
        });
    addListenerTimer.observeDuration();
  }

  /** Cancels this stream. */
  private void cancelStream(
      ChannelHandlerContext ctx, CancelClientStreamCommand cmd, ChannelPromise promise) {
    NettyClientStream.TransportState stream = cmd.stream();
    PerfMark.startTask("NettyClientHandler.cancelStream", stream.tag());
    Histogram.Timer cancelStream =
        perfmarkNettyClientHandlerDuration.labels("NettyClientHandler.cancelStream").startTimer();
    PerfMark.linkIn(cmd.getLink());
    try {
      Status reason = cmd.reason();
      if (reason != null) {
        stream.transportReportStatus(reason, true, new Metadata());
      }
      if (!cmd.stream().isNonExistent()) {
        encoder().writeRstStream(ctx, stream.id(), Http2Error.CANCEL.code(), promise);
      } else {
        promise.setSuccess();
      }
    } finally {
      PerfMark.stopTask("NettyClientHandler.cancelStream", stream.tag());
      cancelStream.observeDuration();
    }
  }

  /** Sends the given GRPC frame for the stream. */
  private void sendGrpcFrame(
      ChannelHandlerContext ctx, SendGrpcFrameCommand cmd, ChannelPromise promise) {
    PerfMark.startTask("NettyClientHandler.sendGrpcFrame", cmd.stream().tag());
    Histogram.Timer sendGrpcFrame =
        perfmarkNettyClientHandlerDuration.labels("NettyClientHandler.sendGrpcFrame").startTimer();
    PerfMark.linkIn(cmd.getLink());
    try {
      // Call the base class to write the HTTP/2 DATA frame.
      // Note: no need to flush since this is handled by the outbound flow controller.
      encoder().writeData(ctx, cmd.stream().id(), cmd.content(), 0, cmd.endStream(), promise);
    } finally {
      PerfMark.stopTask("NettyClientHandler.sendGrpcFrame", cmd.stream().tag());
      sendGrpcFrame.observeDuration();
    }
  }

  private void sendPingFrame(
      ChannelHandlerContext ctx, SendPingCommand msg, ChannelPromise promise) {
    PerfMark.startTask("NettyClientHandler.sendPingFrame");
    Histogram.Timer sendPingFrame =
        perfmarkNettyClientHandlerDuration.labels("NettyClientHandler.sendPingFrame").startTimer();
    PerfMark.linkIn(msg.getLink());
    try {
      sendPingFrameTraced(ctx, msg, promise);
    } finally {
      PerfMark.stopTask("NettyClientHandler.sendPingFrame");
      sendPingFrame.observeDuration();
    }
  }

  /**
   * Sends a PING frame. If a ping operation is already outstanding, the callback in the message is
   * registered to be called when the existing operation completes, and no new frame is sent.
   */
  private void sendPingFrameTraced(
      ChannelHandlerContext ctx, SendPingCommand msg, ChannelPromise promise) {
    // Don't check lifecycleManager.getShutdownStatus() since we want to allow pings after shutdown
    // but before termination. After termination, messages will no longer arrive because the
    // pipeline clears all handlers on channel close.

    PingCallback callback = msg.callback();
    Executor executor = msg.executor();
    // we only allow one outstanding ping at a time, so just add the callback to
    // any outstanding operation
    if (ping != null) {
      promise.setSuccess();
      ping.addCallback(callback, executor);
      return;
    }

    // Use a new promise to prevent calling the callback twice on write failure: here and in
    // NettyClientTransport.ping(). It may appear strange, but it will behave the same as if
    // ping != null above.
    promise.setSuccess();
    promise = ctx().newPromise();
    // set outstanding operation
    long data = USER_PING_PAYLOAD;
    Stopwatch stopwatch = stopwatchFactory.get();
    stopwatch.start();
    ping = new Http2Ping(data, stopwatch);
    ping.addCallback(callback, executor);
    // and then write the ping
    encoder().writePing(ctx, false, USER_PING_PAYLOAD, promise);
    ctx.flush();
    final Http2Ping finalPing = ping;
    promise.addListener(
        new ChannelFutureListener() {
          @Override
          public void operationComplete(ChannelFuture future) throws Exception {
            if (future.isSuccess()) {
              transportTracer.reportKeepAliveSent();
            } else {
              Throwable cause = future.cause();
              if (cause instanceof ClosedChannelException) {
                cause = lifecycleManager.getShutdownThrowable();
                if (cause == null) {
                  cause =
                      Status.UNKNOWN
                          .withDescription("Ping failed but for unknown reason.")
                          .withCause(future.cause())
                          .asException();
                }
              }
              finalPing.failed(cause);
              if (ping == finalPing) {
                ping = null;
              }
            }
          }
        });
  }

  private void gracefulClose(
      ChannelHandlerContext ctx, GracefulCloseCommand msg, ChannelPromise promise)
      throws Exception {
    lifecycleManager.notifyShutdown(msg.getStatus());
    // Explicitly flush to create any buffered streams before sending GOAWAY.
    // TODO(ejona): determine if the need to flush is a bug in Netty
    flush(ctx);
    close(ctx, promise);
  }

  private void forcefulClose(
      final ChannelHandlerContext ctx, final ForcefulCloseCommand msg, ChannelPromise promise)
      throws Exception {
    connection()
        .forEachActiveStream(
            new Http2StreamVisitor() {
              @Override
              public boolean visit(Http2Stream stream) throws Http2Exception {
                NettyClientStream.TransportState clientStream = clientStream(stream);
                Tag tag = clientStream != null ? clientStream.tag() : PerfMark.createTag();
                PerfMark.startTask("NettyClientHandler.forcefulClose", tag);
                Histogram.Timer forcefulClose =
                    perfmarkNettyClientHandlerDuration
                        .labels("NettyClientHandler.forcefulClose")
                        .startTimer();
                PerfMark.linkIn(msg.getLink());
                try {
                  if (clientStream != null) {
                    clientStream.transportReportStatus(msg.getStatus(), true, new Metadata());
                    resetStream(ctx, stream.id(), Http2Error.CANCEL.code(), ctx.newPromise());
                  }
                  stream.close();
                  return true;
                } finally {
                  PerfMark.stopTask("NettyClientHandler.forcefulClose", tag);
                  forcefulClose.observeDuration();
                }
              }
            });
    close(ctx, promise);
  }

  /**
   * Handler for a GOAWAY being received. Fails any streams created after the last known stream. May
   * only be called during a read.
   */
  private void goingAway(long errorCode, byte[] debugData) {
    Status finalStatus =
        statusFromH2Error(
            Status.Code.UNAVAILABLE, "GOAWAY shut down transport", errorCode, debugData);
    lifecycleManager.notifyGracefulShutdown(finalStatus);
    abruptGoAwayStatus =
        statusFromH2Error(
            Status.Code.UNAVAILABLE, "Abrupt GOAWAY closed unsent stream", errorCode, debugData);
    // While this _should_ be UNAVAILABLE, Netty uses the wrong stream id in the GOAWAY when it
    // fails streams due to HPACK failures (e.g., header list too large). To be more conservative,
    // we assume any sent streams may be related to the GOAWAY. This should rarely impact users
    // since the main time servers should use abrupt GOAWAYs is if there is a protocol error, and if
    // there wasn't a protocol error the error code was probably NO_ERROR which is mapped to
    // UNAVAILABLE. https://github.com/netty/netty/issues/10670
    final Status abruptGoAwayStatusConservative =
        statusFromH2Error(null, "Abrupt GOAWAY closed sent stream", errorCode, debugData);
    // Try to allocate as many in-flight streams as possible, to reduce race window of
    // https://github.com/grpc/grpc-java/issues/2562 . To be of any help, the server has to
    // gracefully shut down the connection with two GOAWAYs. gRPC servers generally send a PING
    // after the first GOAWAY, so they can very precisely detect when the GOAWAY has been
    // processed and thus this processing must be in-line before processing additional reads.

    // This can cause reentrancy, but should be minor since it is normal to handle writes in
    // response to a read. Also, the call stack is rather shallow at this point
    clientWriteQueue.drainNow();
    if (lifecycleManager.notifyShutdown(finalStatus)) {
      // This is for the only RPCs that are actually covered by the GOAWAY error code. All other
      // RPCs were not observed by the remote and so should be UNAVAILABLE.
      channelInactiveReason =
          statusFromH2Error(null, "Connection closed after GOAWAY", errorCode, debugData);
    }

    final int lastKnownStream = connection().local().lastStreamKnownByPeer();
    try {
      connection()
          .forEachActiveStream(
              new Http2StreamVisitor() {
                @Override
                public boolean visit(Http2Stream stream) throws Http2Exception {
                  if (stream.id() > lastKnownStream) {
                    NettyClientStream.TransportState clientStream = clientStream(stream);
                    if (clientStream != null) {
                      // RpcProgress _should_ be REFUSED, but are being conservative. See comment
                      // for
                      // abruptGoAwayStatusConservative. This does reduce our ability to perform
                      // transparent
                      // retries, but our main goal of transporent retries is to resolve the local
                      // race. We
                      // still hope/expect servers to use the graceful double-GOAWAY when closing
                      // connections.
                      clientStream.transportReportStatus(
                          abruptGoAwayStatusConservative,
                          RpcProgress.PROCESSED,
                          false,
                          new Metadata());
                    }
                    stream.close();
                  }
                  return true;
                }
              });
    } catch (Http2Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void cancelPing(Throwable t) {
    if (ping != null) {
      ping.failed(t);
      ping = null;
    }
  }

  /** If {@code statusCode} is non-null, it will be used instead of the http2 error code mapping. */
  private Status statusFromH2Error(
      Status.Code statusCode, String context, long errorCode, byte[] debugData) {
    Status status = GrpcUtil.Http2Error.statusForCode((int) errorCode);
    if (statusCode == null) {
      statusCode = status.getCode();
    }
    String debugString = "";
    if (debugData != null && debugData.length > 0) {
      // If a debug message was provided, use it.
      debugString = ", debug data: " + new String(debugData, UTF_8);
    }
    return statusCode
        .toStatus()
        .withDescription(context + ". " + status.getDescription() + debugString);
  }

  /** Gets the client stream associated to the given HTTP/2 stream object. */
  private NettyClientStream.TransportState clientStream(Http2Stream stream) {
    return stream == null ? null : (NettyClientStream.TransportState) stream.getProperty(streamKey);
  }

  private int incrementAndGetNextStreamId() throws StatusException {
    int nextStreamId = connection().local().incrementAndGetNextStreamId();
    if (nextStreamId < 0) {
      logger.fine(
          "Stream IDs have been exhausted for this connection. "
              + "Initiating graceful shutdown of the connection.");
      throw EXHAUSTED_STREAMS_STATUS.asException();
    }
    return nextStreamId;
  }

  private Http2Stream requireHttp2Stream(int streamId) {
    Http2Stream stream = connection().stream(streamId);
    if (stream == null) {
      // This should never happen.
      throw new AssertionError("Stream does not exist: " + streamId);
    }
    return stream;
  }

  private class FrameListener extends Http2FrameAdapter {
    private boolean firstSettings = true;

    @Override
    public void onSettingsRead(ChannelHandlerContext ctx, Http2Settings settings) {
      if (firstSettings) {
        firstSettings = false;
        lifecycleManager.notifyReady();
      }
    }

    @Override
    public int onDataRead(
        ChannelHandlerContext ctx, int streamId, ByteBuf data, int padding, boolean endOfStream)
        throws Http2Exception {
      NettyClientHandler.this.onDataRead(streamId, data, padding, endOfStream);
      return padding;
    }

    @Override
    public void onHeadersRead(
        ChannelHandlerContext ctx,
        int streamId,
        Http2Headers headers,
        int streamDependency,
        short weight,
        boolean exclusive,
        int padding,
        boolean endStream)
        throws Http2Exception {
      NettyClientHandler.this.onHeadersRead(streamId, headers, endStream);
    }

    @Override
    public void onRstStreamRead(ChannelHandlerContext ctx, int streamId, long errorCode)
        throws Http2Exception {
      NettyClientHandler.this.onRstStreamRead(streamId, errorCode);
    }

    @Override
    public void onPingAckRead(ChannelHandlerContext ctx, long ackPayload) throws Http2Exception {
      Http2Ping p = ping;
      if (ackPayload == flowControlPing().payload()) {
        flowControlPing().updateWindow();
        if (logger.isLoggable(Level.FINE)) {
          logger.log(
              Level.FINE,
              String.format(
                  "Window: %d",
                  decoder().flowController().initialWindowSize(connection().connectionStream())));
        }
      } else if (p != null) {
        if (p.payload() == ackPayload) {
          p.complete();
          ping = null;
        } else {
          logger.log(
              Level.WARNING,
              String.format(
                  "Received unexpected ping ack. Expecting %d, got %d", p.payload(), ackPayload));
        }
      } else {
        logger.warning("Received unexpected ping ack. No ping outstanding");
      }
      if (keepAliveManager != null) {
        keepAliveManager.onDataReceived();
      }
    }

    @Override
    public void onPingRead(ChannelHandlerContext ctx, long data) throws Http2Exception {
      if (keepAliveManager != null) {
        keepAliveManager.onDataReceived();
      }
    }
  }

  private static class PingCountingFrameWriter extends DecoratingHttp2FrameWriter
      implements AbstractNettyHandler.PingLimiter {
    private int pingCount;

    public PingCountingFrameWriter(Http2FrameWriter delegate) {
      super(delegate);
    }

    @Override
    public boolean isPingAllowed() {
      // "3 strikes" may cause the server to complain, so we limit ourselves to 2 or below.
      return pingCount < 2;
    }

    @Override
    public ChannelFuture writeHeaders(
        ChannelHandlerContext ctx,
        int streamId,
        Http2Headers headers,
        int padding,
        boolean endStream,
        ChannelPromise promise) {
      pingCount = 0;
      return super.writeHeaders(ctx, streamId, headers, padding, endStream, promise);
    }

    @Override
    public ChannelFuture writeHeaders(
        ChannelHandlerContext ctx,
        int streamId,
        Http2Headers headers,
        int streamDependency,
        short weight,
        boolean exclusive,
        int padding,
        boolean endStream,
        ChannelPromise promise) {
      pingCount = 0;
      return super.writeHeaders(
          ctx, streamId, headers, streamDependency, weight, exclusive, padding, endStream, promise);
    }

    @Override
    public ChannelFuture writeWindowUpdate(
        ChannelHandlerContext ctx, int streamId, int windowSizeIncrement, ChannelPromise promise) {
      pingCount = 0;
      return super.writeWindowUpdate(ctx, streamId, windowSizeIncrement, promise);
    }

    @Override
    public ChannelFuture writePing(
        ChannelHandlerContext ctx, boolean ack, long data, ChannelPromise promise) {
      if (!ack) {
        pingCount++;
      }
      return super.writePing(ctx, ack, data, promise);
    }

    @Override
    public ChannelFuture writeData(
        ChannelHandlerContext ctx,
        int streamId,
        ByteBuf data,
        int padding,
        boolean endStream,
        ChannelPromise promise) {
      if (data.isReadable()) {
        pingCount = 0;
      }
      return super.writeData(ctx, streamId, data, padding, endStream, promise);
    }
  }
}
