package org.tikv.common.region;

import static org.tikv.common.codec.KeyUtils.formatBytesUTF8;
import static org.tikv.common.util.KeyRangeUtils.makeRange;

import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.key.Key;
import org.tikv.common.util.BackOffer;

public class RegionCache {
  private static final Logger logger = LoggerFactory.getLogger(RegionCache.class);

  private final Map<Long, TiRegion> regionCache;
  private final Map<Long, TiStore> storeCache;
  private final RangeMap<Key, Long> keyToRegionIdCache;

  public RegionCache() {
    regionCache = new HashMap<>();
    storeCache = new HashMap<>();

    keyToRegionIdCache = TreeRangeMap.create();
  }

  public synchronized void invalidateAll() {
    regionCache.clear();
    storeCache.clear();
    keyToRegionIdCache.clear();
  }

  public synchronized TiRegion getRegionByKey(ByteString key, BackOffer backOffer) {
    Long regionId;
    if (key.isEmpty()) {
      // if key is empty, it must be the start key.
      regionId = keyToRegionIdCache.get(Key.toRawKey(key, true));
    } else {
      regionId = keyToRegionIdCache.get(Key.toRawKey(key));
    }
    logger.debug(String.format("getRegionByKey key[%s] -> ID[%s]", formatBytesUTF8(key), regionId));

    if (regionId == null) {
      return null;
    }
    TiRegion region;
    region = regionCache.get(regionId);
    logger.debug(String.format("getRegionByKey ID[%s] -> Region[%s]", regionId, region));
    return region;
  }

  public synchronized TiRegion putRegion(TiRegion region) {
    logger.debug("putRegion: " + region);
    TiRegion oldRegion = regionCache.get(region.getId());
    if (oldRegion != null) {
      if (oldRegion.getMeta().equals(region.getMeta())) {
        return oldRegion;
      } else {
        invalidateRegion(oldRegion);
      }
    }
    regionCache.put(region.getId(), region);
    keyToRegionIdCache.put(makeRange(region.getStartKey(), region.getEndKey()), region.getId());
    return region;
  }

  @Deprecated
  public synchronized TiRegion getRegionById(long regionId) {
    TiRegion region = regionCache.get(regionId);
    logger.debug(String.format("getRegionByKey ID[%s] -> Region[%s]", regionId, region));
    return region;
  }

  private synchronized TiRegion getRegionFromCache(long regionId) {
    return regionCache.get(regionId);
  }

  /** Removes region associated with regionId from regionCache. */
  public synchronized void invalidateRegion(TiRegion region) {
    try {
      logger.debug(String.format("invalidateRegion ID[%s]", region.getId()));
      TiRegion oldRegion = regionCache.get(region.getId());
      if (oldRegion != null && oldRegion == region) {
        keyToRegionIdCache.remove(makeRange(region.getStartKey(), region.getEndKey()));
        regionCache.remove(region.getId());
      }
    } catch (Exception ignore) {
    }
  }

  public synchronized boolean updateRegion(TiRegion expected, TiRegion region) {
    try {
      logger.debug(String.format("invalidateRegion ID[%s]", region.getId()));
      TiRegion oldRegion = regionCache.get(region.getId());
      if (!expected.getMeta().equals(oldRegion.getMeta())) {
        return false;
      } else {
        if (oldRegion != null) {
          keyToRegionIdCache.remove(makeRange(oldRegion.getStartKey(), oldRegion.getEndKey()));
        }
        regionCache.put(region.getId(), region);
        keyToRegionIdCache.put(makeRange(region.getStartKey(), region.getEndKey()), region.getId());
        return true;
      }
    } catch (Exception ignore) {
      return false;
    }
  }

  public synchronized boolean updateStore(TiStore oldStore, TiStore newStore) {
    if (!newStore.isValid()) {
      return false;
    }
    TiStore originStore = storeCache.get(oldStore.getId());
    if (originStore == oldStore) {
      storeCache.put(newStore.getId(), newStore);
      oldStore.markInvalid();
      return true;
    }
    return false;
  }

  public synchronized void invalidateAllRegionForStore(TiStore store) {
    TiStore oldStore = storeCache.get(store.getId());
    if (oldStore != store) {
      return;
    }
    List<TiRegion> regionToRemove = new ArrayList<>();
    for (TiRegion r : regionCache.values()) {
      if (r.getLeader().getStoreId() == store.getId()) {
        logger.debug(String.format("invalidateAllRegionForStore Region[%s]", r));
        regionToRemove.add(r);
      }
    }

    logger.warn(String.format("invalid store [%d]", store.getId()));
    // remove region
    for (TiRegion r : regionToRemove) {
      keyToRegionIdCache.remove(makeRange(r.getStartKey(), r.getEndKey()));
      regionCache.remove(r.getId());
    }
  }

  public synchronized void invalidateStore(long storeId) {
    TiStore store = storeCache.remove(storeId);
    if (store != null) {
      store.markInvalid();
    }
  }

  public synchronized TiStore getStoreById(long id) {
    return storeCache.get(id);
  }

  public synchronized boolean putStore(long id, TiStore store) {
    TiStore oldStore = storeCache.get(id);
    if (oldStore != null) {
      if (oldStore.equals(store)) {
        return false;
      } else {
        oldStore.markInvalid();
      }
    }
    storeCache.put(id, store);
    return true;
  }

  public synchronized void clearAll() {
    keyToRegionIdCache.clear();
    regionCache.clear();
  }
}
