/*
 *
 * Copyright 2021 PingCAP, Inc.
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
 *
 */

package org.tikv.common.importer;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.tikv.common.PDClient;
import org.tikv.common.region.TiStore;
import org.tikv.common.util.BackOffer;
import org.tikv.common.util.ConcreteBackOffer;
import org.tikv.kvproto.ImportSstpb;
import org.tikv.kvproto.Metapb;

public class SwitchTiKVModeClient {
  private static final int IMPORT_MODE_TIMEOUT = 600;
  private static final int KEEP_TIKV_TO_IMPORT_MODE_PERIOD = IMPORT_MODE_TIMEOUT / 5;

  private final PDClient pdClient;
  private final ImporterStoreClient.ImporterStoreClientBuilder builder;

  private final ScheduledExecutorService ingestScheduledExecutorService;

  public SwitchTiKVModeClient(
      PDClient pdClient, ImporterStoreClient.ImporterStoreClientBuilder builder) {
    this.pdClient = pdClient;
    this.builder = builder;

    this.ingestScheduledExecutorService =
        Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder()
                .setNameFormat("switch-tikv-mode-pool-%d")
                .setDaemon(true)
                .build());
  }

  public void switchTiKVToNormalMode() {
    doSwitchTiKVMode(ImportSstpb.SwitchMode.Normal);
  }

  public void keepTiKVToImportMode() {
    ingestScheduledExecutorService.scheduleAtFixedRate(
        this::switchTiKVToImportMode, 0, KEEP_TIKV_TO_IMPORT_MODE_PERIOD, TimeUnit.SECONDS);
  }

  public void stopKeepTiKVToImportMode() {
    ingestScheduledExecutorService.shutdown();
  }

  private void switchTiKVToImportMode() {
    doSwitchTiKVMode(ImportSstpb.SwitchMode.Import);
  }

  private void doSwitchTiKVMode(ImportSstpb.SwitchMode mode) {
    BackOffer bo = ConcreteBackOffer.newCustomBackOff(BackOffer.PD_INFO_BACKOFF);
    List<Metapb.Store> allStores = pdClient.getAllStores(bo);
    for (Metapb.Store store : allStores) {
      ImporterStoreClient client = builder.build(new TiStore(store));
      client.switchMode(mode);
    }
  }
}
