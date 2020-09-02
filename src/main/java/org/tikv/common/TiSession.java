/*
 * Copyright 2017 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.tikv.common;

import com.google.common.annotations.VisibleForTesting;
import org.tikv.common.region.RegionManager;
import org.tikv.common.region.RegionStoreClient.RegionStoreClientBuilder;
import org.tikv.common.util.ChannelFactory;
import org.tikv.raw.RawKVClient;
import org.tikv.txn.KVClient;

/**
 * TiSession is the holder for PD Client, Store pdClient and PD Cache All sessions share common
 * region store connection pool but separated PD conn and cache for better concurrency TiSession is
 * thread-safe but it's also recommended to have multiple session avoiding lock contention
 */
public class TiSession implements AutoCloseable {
  private final TiConfiguration conf;
  private final PDClient pdClient;
  private final ChannelFactory channelFactory;

  public TiSession(TiConfiguration conf) {
    this.conf = conf;
    this.channelFactory = new ChannelFactory(conf.getMaxFrameSize());
    this.pdClient = PDClient.createRaw(conf, channelFactory);
  }

  public TiConfiguration getConf() {
    return conf;
  }

  public static TiSession create(TiConfiguration conf) {
    return new TiSession(conf);
  }

  public RawKVClient createRawClient() {
    // Create new Region Manager avoiding thread contentions
    RegionManager regionMgr = new RegionManager(pdClient);
    RegionStoreClientBuilder builder =
        new RegionStoreClientBuilder(conf, channelFactory, regionMgr, pdClient);
    return new RawKVClient(conf, builder);
  }

  public KVClient createTxnKVClient() {
    // Create new Region Manager avoiding thread contentions
    RegionManager regionMgr = new RegionManager(pdClient);
    RegionStoreClientBuilder builder =
        new RegionStoreClientBuilder(conf, channelFactory, regionMgr, pdClient);
    return new KVClient(conf, builder);
  }

  @VisibleForTesting
  public RegionManager getRegionManager() {
    return new RegionManager(pdClient);
  }

  @VisibleForTesting
  public PDClient getPDClient() {
    return pdClient;
  }

  @VisibleForTesting
  public ChannelFactory getChannelFactory() {
    return channelFactory;
  }

  @Override
  public void close() {
    pdClient.close();
    channelFactory.close();
  }
}
