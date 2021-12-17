package org.tikv.cdc;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.region.TiRegion;
import org.tikv.common.util.FastByteComparisons;
import org.tikv.common.util.KeyRangeUtils;
import org.tikv.kvproto.Cdcpb.ChangeDataEvent;
import org.tikv.kvproto.Cdcpb.ChangeDataRequest;
import org.tikv.kvproto.Cdcpb.Event.LogType;
import org.tikv.kvproto.Cdcpb.Event.Row;
import org.tikv.kvproto.Cdcpb.Header;
import org.tikv.kvproto.Cdcpb.ResolvedTs;
import org.tikv.kvproto.ChangeDataGrpc;
import org.tikv.kvproto.ChangeDataGrpc.ChangeDataStub;
import org.tikv.kvproto.Coprocessor.KeyRange;

class RegionCDCClient implements AutoCloseable, StreamObserver<ChangeDataEvent> {

  private static final Logger LOGGER = LoggerFactory.getLogger(RegionCDCClient.class);
  private static final AtomicLong REQ_ID_COUNTER = new AtomicLong(0);
  private static final Set<LogType> ALLOWED_LOGTYPE =
      ImmutableSet.of(LogType.PREWRITE, LogType.COMMIT, LogType.COMMITTED, LogType.ROLLBACK);

  private final TiRegion region;
  private final KeyRange keyRange;
  private final KeyRange regionKeyRange;
  private final ManagedChannel channel;
  private final ChangeDataStub asyncStub;
  private final Consumer<CDCEvent> eventConsumer;
  private final CDCConfig config;
  private final Predicate<Row> rowFilter;

  private final AtomicBoolean running = new AtomicBoolean(false);

  private final boolean started = false;

  public RegionCDCClient(
      final TiRegion region,
      final KeyRange keyRange,
      final ManagedChannel channel,
      final Consumer<CDCEvent> eventConsumer,
      final CDCConfig config) {
    this.region = region;
    this.keyRange = keyRange;
    this.channel = channel;
    this.asyncStub = ChangeDataGrpc.newStub(channel);
    this.eventConsumer = eventConsumer;
    this.config = config;

    this.regionKeyRange =
        KeyRange.newBuilder().setStart(region.getStartKey()).setEnd(region.getEndKey()).build();

    this.rowFilter =
        regionEnclosed()
            ? ((row) -> true)
            : new Predicate<Row>() {
              final byte[] buffer = new byte[config.getMaxRowKeySize()];

              final byte[] start = keyRange.getStart().toByteArray();
              final byte[] end = keyRange.getEnd().toByteArray();

              @Override
              public boolean test(final Row row) {
                final int len = row.getKey().size();
                row.getKey().copyTo(buffer, 0);
                return (FastByteComparisons.compareTo(buffer, 0, len, start, 0, start.length) >= 0)
                    && (FastByteComparisons.compareTo(buffer, 0, len, end, 0, end.length) < 0);
              }
            };
  }

  public synchronized void start(final long startTs) {
    Preconditions.checkState(!started, "RegionCDCClient has already started");
    running.set(true);
    LOGGER.info("start streaming region: {}, running: {}", region.getId(), running.get());
    final ChangeDataRequest request =
        ChangeDataRequest.newBuilder()
            .setRequestId(REQ_ID_COUNTER.incrementAndGet())
            .setHeader(Header.newBuilder().setTicdcVersion("5.0.0").build())
            .setRegionId(region.getId())
            .setCheckpointTs(startTs)
            .setStartKey(keyRange.getStart())
            .setEndKey(keyRange.getEnd())
            .setRegionEpoch(region.getRegionEpoch())
            .setExtraOp(config.getExtraOp())
            .build();
    final StreamObserver<ChangeDataRequest> requestObserver = asyncStub.eventFeed(this);
    requestObserver.onNext(request);
  }

  public TiRegion getRegion() {
    return region;
  }

  public KeyRange getKeyRange() {
    return keyRange;
  }

  public KeyRange getRegionKeyRange() {
    return regionKeyRange;
  }

  public boolean regionEnclosed() {
    return KeyRangeUtils.makeRange(keyRange.getStart(), keyRange.getEnd())
        .encloses(KeyRangeUtils.makeRange(regionKeyRange.getStart(), regionKeyRange.getEnd()));
  }

  public boolean isRunning() {
    return running.get();
  }

  @Override
  public void close() throws Exception {
    LOGGER.info("close (region: {})", region.getId());
    running.set(false);
    synchronized (this) {
      channel.shutdown();
    }
    try {
      LOGGER.atDebug().log("awaitTermination (region: {})", region.getId());
      channel.awaitTermination(60, TimeUnit.SECONDS);
    } catch (final InterruptedException e) {
      LOGGER.error("Failed to shutdown channel(regionId: {})", region.getId());
      Thread.currentThread().interrupt();
      synchronized (this) {
        channel.shutdownNow();
      }
    }
    LOGGER.info("terminated (region: {})", region.getId());
  }

  @Override
  public void onCompleted() {
    // should never been called
    onError(new IllegalStateException("RegionCDCClient should never complete"));
  }

  @Override
  public void onError(final Throwable error) {
    LOGGER.error("region CDC error: region: {}, error: {}", region.getId(), error);
    running.set(false);
    eventConsumer.accept(CDCEvent.error(region.getId(), error));
  }

  @Override
  public void onNext(final ChangeDataEvent event) {
    try {
      if (running.get()) {
        event
            .getEventsList()
            .stream()
            .flatMap(ev -> ev.getEntries().getEntriesList().stream())
            .filter(row -> ALLOWED_LOGTYPE.contains(row.getType()))
            .filter(this.rowFilter)
            .map(row -> CDCEvent.rowEvent(region.getId(), row))
            .forEach(this::submitEvent);

        if (event.hasResolvedTs()) {
          final ResolvedTs resolvedTs = event.getResolvedTs();
          if (resolvedTs.getRegionsList().indexOf(region.getId()) >= 0) {
            submitEvent(CDCEvent.resolvedTsEvent(region.getId(), resolvedTs.getTs()));
          }
        }
      }
    } catch (final Exception e) {
      onError(e);
    }
  }

  private void submitEvent(final CDCEvent event) {
    LOGGER.atDebug().log("submit event: {}", event);
    eventConsumer.accept(event);
  }
}
