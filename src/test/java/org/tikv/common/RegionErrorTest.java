/*
 * Copyright 2022 TiKV Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.tikv.common;

import static org.junit.Assert.fail;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.tikv.common.KVMockServer.State;
import org.tikv.common.apiversion.RequestKeyCodec;
import org.tikv.common.region.TiRegion;
import org.tikv.common.region.TiStore;
import org.tikv.common.util.Pair;
import org.tikv.kvproto.Errorpb;
import org.tikv.kvproto.Errorpb.ServerIsBusy;
import org.tikv.kvproto.Errorpb.StaleCommand;
import org.tikv.kvproto.Metapb;
import org.tikv.kvproto.Metapb.Peer;
import org.tikv.kvproto.Pdpb;
import org.tikv.raw.RawKVClient;

public class RegionErrorTest extends MockThreeStoresTest {

  @Before
  public void init() throws Exception {
    upgradeToV2Cluster();
  }

  private RawKVClient createClient() {
    return session.createRawClient();
  }

  @Test
  public void testOnEpochNotMatch() {
    try (RawKVClient client = createClient()) {
      // Construct a key that is less than the prefix of RAW API v2;
      ByteString key = ByteString.copyFromUtf8("key-test-epoch-not-match");
      ByteString value = ByteString.copyFromUtf8("value");

      ByteString requestKey = client.getSession().getPDClient().getCodec().encodeKey(key);
      put(requestKey, value);

      Assert.assertEquals(Optional.of(value), client.get(key));

      Metapb.Region newMeta =
          Metapb.Region.newBuilder()
              .mergeFrom(this.region.getMeta())
              .setRegionEpoch(Metapb.RegionEpoch.newBuilder().setConfVer(2).setVersion(3))
              .setStartKey(PDClientV2MockTest.encode(requestKey))
              .setEndKey(PDClientV2MockTest.encode(requestKey.concat(ByteString.copyFromUtf8("0"))))
              .build();

      // Increase the region epoch for the cluster,
      // this will cause the cluster return an EpochNotMatch region error.
      TiRegion newRegion =
          new TiRegion(
              this.region.getConf(),
              newMeta,
              this.region.getLeader(),
              this.region.getPeersList(),
              stores.stream().map(TiStore::new).collect(Collectors.toList()));

      // Update the region of each server
      for (KVMockServer server : servers) {
        server.setRegion(newRegion);
        server.setSubregions(ImmutableList.of(newRegion.getMeta()));
      }

      // Forbid the client get region from PD leader.
      leader.addGetRegionListener(request -> null);

      // The get should success since the region cache
      // will be updated the currentRegions of `EpochNotMatch` error.
      Assert.assertEquals(Optional.of(value), client.get(key));
    }
  }

  @Test
  public void testDecodeEpochNotMatch() {
    initCluster();
    try (RawKVClient client = createClient()) {
      // Construct a key that is less than the prefix of RAW API v2;
      ByteString key = ByteString.copyFromUtf8("key-test-decode-not-match");
      ByteString value = ByteString.copyFromUtf8("value");

      RequestKeyCodec c = client.getSession().getPDClient().getCodec();
      ByteString requestKey = c.encodeKey(key);
      put(requestKey, value);

      Assert.assertEquals(Optional.of(value), client.get(key));

      Metapb.Region newMeta =
          Metapb.Region.newBuilder()
              .mergeFrom(this.region.getMeta())
              .setRegionEpoch(Metapb.RegionEpoch.newBuilder().setConfVer(2).setVersion(3))
              .setStartKey(PDClientV2MockTest.encode(requestKey))
              .setEndKey(PDClientV2MockTest.encode(requestKey.concat(ByteString.copyFromUtf8("0"))))
              .build();

      // Increase the region epoch for the cluster,
      // this will cause the cluster return an EpochNotMatch region error.
      TiRegion newRegion =
          new TiRegion(
              this.region.getConf(),
              newMeta,
              this.region.getLeader(),
              this.region.getPeersList(),
              stores.stream().map(TiStore::new).collect(Collectors.toList()));

      // ["", minKeyspaceKey)
      Metapb.Region newRegionA =
          Metapb.Region.newBuilder()
              .mergeFrom(this.region.getMeta())
              .setId(10)
              .setRegionEpoch(Metapb.RegionEpoch.newBuilder().setConfVer(2).setVersion(3))
              .setStartKey(ByteString.EMPTY)
              .setEndKey(PDClientV2MockTest.encode(c.encodeKey(ByteString.EMPTY)))
              .build();
      // [minKeyspaceKey, maxKeyspaceKey)
      Pair<ByteString, ByteString> range = c.encodeRange(ByteString.EMPTY, ByteString.EMPTY);
      Metapb.Region newRegionB =
          Metapb.Region.newBuilder()
              .mergeFrom(this.region.getMeta())
              .setRegionEpoch(Metapb.RegionEpoch.newBuilder().setConfVer(2).setVersion(3))
              .setStartKey(PDClientV2MockTest.encode(range.first))
              .setEndKey(PDClientV2MockTest.encode(range.second))
              .build();
      // [maxKeyspaceKey, "")
      Metapb.Region newRegionC =
          Metapb.Region.newBuilder()
              .mergeFrom(this.region.getMeta())
              .setId(14)
              .setRegionEpoch(Metapb.RegionEpoch.newBuilder().setConfVer(2).setVersion(3))
              .setStartKey(PDClientV2MockTest.encode(range.second))
              .setEndKey(ByteString.EMPTY)
              .build();
      List<Metapb.Region> subregions =
          new ArrayList<Metapb.Region>() {
            {
              add(newRegionA);
              add(newRegionB);
              add(newRegionC);
            }
          };

      // Update the region of each server
      for (KVMockServer server : servers) {
        server.setRegion(newRegion);
        server.setSubregions(subregions);
      }

      // Forbid the client get region from PD leader.
      leader.addGetRegionListener(
          request -> {
            return Pdpb.GetRegionResponse.newBuilder()
                .setRegion(newRegionA)
                .setLeader(newRegionA.getPeers(0))
                .build();
          });

      // The get should success since the region cache
      // will be updated the currentRegions of `EpochNotMatch` error.
      Assert.assertEquals(Optional.of(value), client.get(key));
    }
  }

  @Test
  public void testEmptyEpochNotMatch() {
    initCluster();
    try (RawKVClient client = createClient()) {
      // Construct a key that is less than the prefix of RAW API v2;
      ByteString key = ByteString.copyFromUtf8("key-test-empty-epoch-not-match");
      ByteString value = ByteString.copyFromUtf8("value");

      RequestKeyCodec c = client.getSession().getPDClient().getCodec();
      ByteString requestKey = c.encodeKey(key);
      put(requestKey, value);

      Assert.assertEquals(Optional.of(value), client.get(key));

      servers
          .get(0)
          .putError(
              requestKey.toStringUtf8(),
              () ->
                  Errorpb.Error.newBuilder()
                      .setEpochNotMatch(Errorpb.EpochNotMatch.getDefaultInstance()));

      TiRegion newRegion =
          new TiRegion(
              this.region.getConf(),
              region.getMeta(),
              Metapb.Peer.newBuilder().setId(2).setStoreId(2).build(),
              this.region.getPeersList(),
              stores.stream().map(TiStore::new).collect(Collectors.toList()));

      leader.addGetRegionListener(
          request -> {
            return Pdpb.GetRegionResponse.newBuilder()
                .setRegion(newRegion.getMeta())
                .setLeader(newRegion.getLeader())
                .build();
          });

      for (int i = 1; i < servers.size(); i++) {
        servers.get(i).setRegion(newRegion);
      }

      Assert.assertEquals(Optional.of(value), client.get(key));
    }
  }

  @Test
  public void testTiKVFailed() {
    initCluster();
    try (RawKVClient client = createClient()) {
      // Construct a key that is less than the prefix of RAW API v2;
      ByteString key = ByteString.copyFromUtf8("key-test-tikv-failed");
      ByteString value = ByteString.copyFromUtf8("value");

      RequestKeyCodec c = client.getSession().getPDClient().getCodec();
      ByteString requestKey = c.encodeKey(key);
      put(requestKey, value);

      Assert.assertEquals(Optional.of(value), client.get(key));
      for (KVMockServer server : servers) {
        server.setState(State.Fail);
      }
      try {
        Assert.assertEquals(Optional.of(value), client.get(key));
        fail();
      } catch (Exception ignored) {
      }
    }
  }

  @Test
  public void testNotLeader() {
    initCluster();
    try (RawKVClient client = createClient()) {
      // Construct a key that is less than the prefix of RAW API v2;
      ByteString key = ByteString.copyFromUtf8("key-test-not-leader");
      ByteString value = ByteString.copyFromUtf8("value");

      RequestKeyCodec c = client.getSession().getPDClient().getCodec();
      ByteString requestKey = c.encodeKey(key);
      put(requestKey, value);

      Assert.assertEquals(Optional.of(value), client.get(key));

      // Forbid the client get region from PD leader.
      leader.addGetRegionListener(request -> null);

      // Increase the region epoch for the cluster,
      // this will cause the cluster return an EpochNotMatch region error.
      TiRegion newRegion =
          new TiRegion(
              this.region.getConf(),
              region.getMeta(),
              Metapb.Peer.newBuilder().setId(2).setStoreId(2).build(),
              this.region.getPeersList(),
              stores.stream().map(TiStore::new).collect(Collectors.toList()));

      for (KVMockServer server : servers) {
        server.setRegion(newRegion);
        server.setNewLeader(newRegion.getLeader());
      }

      // The get should success since the leader is in the region error resp
      Assert.assertEquals(Optional.of(value), client.get(key));

      newRegion =
          new TiRegion(
              this.region.getConf(),
              region.getMeta(),
              Metapb.Peer.newBuilder().setId(3).setStoreId(3).build(),
              this.region.getPeersList(),
              stores.stream().map(TiStore::new).collect(Collectors.toList()));

      for (KVMockServer server : servers) {
        server.setRegion(newRegion);
        server.setNewLeader(Metapb.Peer.getDefaultInstance());
      }

      TiRegion finalNewRegion = newRegion;
      leader.addGetRegionListener(
          request ->
              Pdpb.GetRegionResponse.newBuilder()
                  .setRegion(finalNewRegion.getMeta())
                  .setLeader(Metapb.Peer.newBuilder().setId(3).setStoreId(3).build())
                  .build());

      Assert.assertEquals(Optional.of(value), client.get(key));
    }
  }

  @Test
  public void testStoreNotMatch() {
    initCluster();
    try (RawKVClient client = createClient()) {
      // Construct a key that is less than the prefix of RAW API v2;
      ByteString key = ByteString.copyFromUtf8("key-test-store-not-match");
      ByteString value = ByteString.copyFromUtf8("value");

      RequestKeyCodec c = client.getSession().getPDClient().getCodec();
      ByteString requestKey = c.encodeKey(key);
      put(requestKey, value);

      Assert.assertEquals(Optional.of(value), client.get(key));

      TiRegion newRegion =
          new TiRegion(
              this.region.getConf(),
              region.getMeta(),
              Metapb.Peer.newBuilder().setId(3).setStoreId(3).build(),
              this.region.getPeersList(),
              stores.stream().map(TiStore::new).collect(Collectors.toList()));
      servers.get(0).setStoreNotMatch(1, 2);
      for (KVMockServer server : servers) {
        server.setRegion(newRegion);
      }
      leader.addGetRegionListener(
          request ->
              Pdpb.GetRegionResponse.newBuilder()
                  .setRegion(region.getMeta())
                  .setLeader(Peer.newBuilder().setStoreId(2).setId(2).build())
                  .build());

      Assert.assertEquals(Optional.of(value), client.get(key));
    }
  }

  public void testRetryableRegionError(String caseName, Errorpb.Error.Builder eb) {
    initCluster();
    // Set the first server to be temporarily unavailable in the first try.
    servers.get(0).setTempError(true);
    try (RawKVClient client = createClient()) {
      // Construct a key that is less than the prefix of RAW API v2;
      ByteString key = ByteString.copyFromUtf8("key-test-" + caseName);
      ByteString value = ByteString.copyFromUtf8("value");

      RequestKeyCodec c = client.getSession().getPDClient().getCodec();
      ByteString requestKey = c.encodeKey(key);
      put(requestKey, value);

      // First try should success, and refresh region cache.
      Assert.assertEquals(Optional.of(value), client.get(key));
      servers.get(0).putError(requestKey.toStringUtf8(), () -> eb);
      Assert.assertEquals(Optional.of(value), client.get(key));
    }
  }

  public void testUnretryableRegionError(String caseName, Errorpb.Error.Builder eb) {
    initCluster();
    try (RawKVClient client = createClient()) {
      // Construct a key that is less than the prefix of RAW API v2;
      ByteString key = ByteString.copyFromUtf8("key-test-" + caseName);
      ByteString value = ByteString.copyFromUtf8("value");

      RequestKeyCodec c = client.getSession().getPDClient().getCodec();
      ByteString requestKey = c.encodeKey(key);
      put(requestKey, value);

      // First try should success, and refresh region cache.
      Assert.assertEquals(Optional.of(value), client.get(key));

      servers.get(0).putError(requestKey.toStringUtf8(), () -> eb);
      // The request should fail since the region cache is empty and pd is not available.
      try {
        client.get(key);
        fail();
      } catch (Exception ignore) {
      }

      leader.addGetRegionListener(
          request ->
              Pdpb.GetRegionResponse.newBuilder()
                  .setRegion(region.getMeta())
                  .setLeader(Peer.newBuilder().setStoreId(2).setId(2).build())
                  .build());

      TiRegion newRegion =
          new TiRegion(
              this.region.getConf(),
              region.getMeta(),
              Metapb.Peer.newBuilder().setId(2).setStoreId(2).build(),
              this.region.getPeersList(),
              stores.stream().map(TiStore::new).collect(Collectors.toList()));
      for (KVMockServer server : servers) {
        server.setRegion(newRegion);
      }

      Assert.assertEquals(Optional.of(value), client.get(key));
    }
  }

  @Test
  public void testKeyNotInRegion() {
    testUnretryableRegionError(
        "key-not-in-region",
        Errorpb.Error.newBuilder().setKeyNotInRegion(Errorpb.KeyNotInRegion.getDefaultInstance()));
  }

  @Test
  public void testRaftEntryTooLarge() {
    testUnretryableRegionError(
        "raft-entry-too-large",
        Errorpb.Error.newBuilder()
            .setRaftEntryTooLarge(Errorpb.RaftEntryTooLarge.getDefaultInstance()));
  }

  @Test
  public void testServerIsBusy() {
    testRetryableRegionError(
        "server-is-busy",
        Errorpb.Error.newBuilder().setServerIsBusy(ServerIsBusy.getDefaultInstance()));
  }

  @Test
  public void testStaleCommand() {
    testRetryableRegionError(
        "stale-command",
        Errorpb.Error.newBuilder().setStaleCommand(StaleCommand.getDefaultInstance()));
  }

  @Test
  public void testTsoBatchUsedUp() {
    testRetryableRegionError(
        "tso-batch-used-up", Errorpb.Error.newBuilder().setMessage("TsoBatchUsedUp"));
  }

  @Test
  public void testRaftProposalDropped() {
    testRetryableRegionError(
        "raft-proposal-dropped", Errorpb.Error.newBuilder().setMessage("Raft ProposalDropped"));
  }

  @Test
  public void testUnknownRegionError() {
    testRetryableRegionError(
        "unknown-region-error", Errorpb.Error.newBuilder().setMessage("Unknown Region Error"));
  }

  private void initCluster() {
    for (KVMockServer server : servers) {
      server.reset();
    }
    leader.addGetRegionListener(
        request ->
            Pdpb.GetRegionResponse.newBuilder()
                .setRegion(firstRegion.getMeta())
                .setLeader(firstRegion.getLeader())
                .build());
    // Reset cluster regions;
    session.getRegionManager().invalidateAll();
  }
}
