package org.tikv.cdc;

import com.google.common.base.Preconditions;
import com.google.common.collect.Range;
import com.google.common.collect.TreeMultiset;
import io.grpc.ManagedChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.TiSession;
import org.tikv.common.key.Key;
import org.tikv.common.region.TiRegion;
import org.tikv.common.util.RangeSplitter;
import org.tikv.common.util.RangeSplitter.RegionTask;
import org.tikv.kvproto.Cdcpb.Event.Row;
import org.tikv.kvproto.Coprocessor.KeyRange;
import org.tikv.kvproto.Kvrpcpb;

public class CDCClient implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(CDCClient.class);

  private final TiSession session;
  private final KeyRange keyRange;
  private final CDCConfig config;

  private final BlockingQueue<CDCEvent> eventsBuffer;
  private final TreeMap<Long, RegionCDCClient> regionClients = new TreeMap<>();
  private final Map<Long, Long> regionToResolvedTs = new HashMap<>();
  private final TreeMultiset<Long> resolvedTsSet = TreeMultiset.create();

  private boolean started = false;

  public CDCClient(final TiSession session, final KeyRange keyRange) {
    this(session, keyRange, new CDCConfig());
  }

  public CDCClient(final TiSession session, final KeyRange keyRange, final CDCConfig config) {
    Preconditions.checkState(
        session.getConf().getIsolationLevel().equals(Kvrpcpb.IsolationLevel.SI),
        "Unsupported Isolation Level"); // only support SI for now
    this.session = session;
    this.keyRange = keyRange;
    this.config = config;
    eventsBuffer = new ArrayBlockingQueue<>(config.getEventBufferSize());
  }

  public synchronized void start(final long startTs) {
    Preconditions.checkState(!started, "Client is already started");
    try {
      applyKeyRange(keyRange, startTs);
    } catch (final Throwable e) {
      LOGGER.error("failed to start:", e);
    }
    started = true;
  }

  public synchronized Row get() throws InterruptedException {
    final CDCEvent event = eventsBuffer.poll(100, TimeUnit.MILLISECONDS);
    if (event != null) {
      switch (event.eventType) {
        case ROW:
          return event.row;
        case RESOLVED_TS:
          handleResolvedTs(event.regionId, event.resolvedTs);
          break;
        case ERROR:
          handleErrorEvent(event.regionId, event.error);
          break;
      }
    }
    return null;
  }

  public synchronized long getMinResolvedTs() {
    return resolvedTsSet.firstEntry().getElement();
  }

  public synchronized long getMaxResolvedTs() {
    return resolvedTsSet.lastEntry().getElement();
  }

  public synchronized void close() {
    removeRegions(regionClients.keySet());
  }

  private synchronized void applyKeyRange(final KeyRange keyRange, final long timestamp) {
    final RangeSplitter splitter = RangeSplitter.newSplitter(session.getRegionManager());

    final Iterator<TiRegion> newRegionsIterator =
        splitter
            .splitRangeByRegion(Arrays.asList(keyRange))
            .stream()
            .map(RegionTask::getRegion)
            .sorted((a, b) -> Long.compare(a.getId(), b.getId()))
            .iterator();
    final Iterator<RegionCDCClient> oldRegionsIterator = regionClients.values().iterator();

    final ArrayList<TiRegion> regionsToAdd = new ArrayList<>();
    final ArrayList<Long> regionsToRemove = new ArrayList<>();

    TiRegion newRegion = newRegionsIterator.hasNext() ? newRegionsIterator.next() : null;
    RegionCDCClient oldRegionClient =
        oldRegionsIterator.hasNext() ? oldRegionsIterator.next() : null;

    while (newRegion != null && oldRegionClient != null) {
      if (newRegion.getId() == oldRegionClient.getRegion().getId()) {
        // check if should refresh region
        if (!oldRegionClient.isRunning()) {
          regionsToRemove.add(newRegion.getId());
          regionsToAdd.add(newRegion);
        }

        newRegion = newRegionsIterator.hasNext() ? newRegionsIterator.next() : null;
        oldRegionClient = oldRegionsIterator.hasNext() ? oldRegionsIterator.next() : null;
      } else if (newRegion.getId() < oldRegionClient.getRegion().getId()) {
        regionsToAdd.add(newRegion);
        newRegion = newRegionsIterator.hasNext() ? newRegionsIterator.next() : null;
      } else {
        regionsToRemove.add(oldRegionClient.getRegion().getId());
        oldRegionClient = oldRegionsIterator.hasNext() ? oldRegionsIterator.next() : null;
      }
    }

    while (newRegion != null) {
      regionsToAdd.add(newRegion);
      newRegion = newRegionsIterator.hasNext() ? newRegionsIterator.next() : null;
    }

    while (oldRegionClient != null) {
      regionsToRemove.add(oldRegionClient.getRegion().getId());
      oldRegionClient = oldRegionsIterator.hasNext() ? oldRegionsIterator.next() : null;
    }

    removeRegions(regionsToRemove);
    addRegions(regionsToAdd, timestamp);
    LOGGER.info("keyRange applied");
  }

  private synchronized void addRegions(final Iterable<TiRegion> regions, final long timestamp) {
    LOGGER.info("add regions: {}, timestamp: {}", regions, timestamp);
    for (final TiRegion region : regions) {
      if (overlapWithRegion(region)) {
        final String address =
            session
                .getRegionManager()
                .getStoreById(region.getLeader().getStoreId())
                .getStore()
                .getAddress();
        final ManagedChannel channel =
            session.getChannelFactory().getChannel(address, session.getPDClient().getHostMapping());
        try {
          final RegionCDCClient client =
              new RegionCDCClient(region, keyRange, channel, eventsBuffer::offer, config);
          regionClients.put(region.getId(), client);
          regionToResolvedTs.put(region.getId(), timestamp);
          resolvedTsSet.add(timestamp);

          client.start(timestamp);
        } catch (final Exception e) {
          LOGGER.error("failed to add region(regionId: {}, reason: {})", region.getId(), e);
          throw new RuntimeException(e);
        }
      }
    }
  }

  private synchronized void removeRegions(final Iterable<Long> regionIds) {
    LOGGER.info("remove regions: {}", regionIds);
    for (final long regionId : regionIds) {
      final RegionCDCClient regionClient = regionClients.remove(regionId);
      if (regionClient != null) {
        try {
          regionClient.close();
        } catch (final Exception e) {
          LOGGER.error("failed to close region client, region id: {}, error: {}", regionId, e);
        } finally {
          resolvedTsSet.remove(regionToResolvedTs.remove(regionId));
          regionToResolvedTs.remove(regionId);
        }
      }
    }
  }

  private boolean overlapWithRegion(final TiRegion region) {
    final Range<Key> regionRange =
        Range.closedOpen(Key.toRawKey(region.getStartKey()), Key.toRawKey(region.getEndKey()));
    final Range<Key> clientRange =
        Range.closedOpen(Key.toRawKey(keyRange.getStart()), Key.toRawKey(keyRange.getEnd()));
    final Range<Key> intersection = regionRange.intersection(clientRange);
    return !intersection.isEmpty();
  }

  private void handleResolvedTs(final long regionId, final long resolvedTs) {
    LOGGER.info("handle resolvedTs: {}, regionId: {}", resolvedTs, regionId);
    resolvedTsSet.remove(regionToResolvedTs.replace(regionId, resolvedTs));
    resolvedTsSet.add(resolvedTs);
  }

  private void handleErrorEvent(final long regionId, final Throwable error) {
    LOGGER.info("handle error: {}, regionId: {}", error, regionId);
    final TiRegion region = regionClients.get(regionId).getRegion();
    session.getRegionManager().onRequestFail(region); // invalidate cache for corresponding region

    removeRegions(Arrays.asList(regionId));
    applyKeyRange(keyRange, getMinResolvedTs()); // reapply the whole keyRange
  }
}
