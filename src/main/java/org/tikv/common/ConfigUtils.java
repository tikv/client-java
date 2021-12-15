/*
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
 */

package org.tikv.common;

import org.tikv.common.util.BackOffer;
import org.tikv.kvproto.Kvrpcpb;

public class ConfigUtils {
  public static final String TIKV_CONFIGURATION_FILENAME = "tikv.properties";

  public static final String TIKV_PD_ADDRESSES = "tikv.pd.addresses";
  public static final String TIKV_GRPC_TIMEOUT = "tikv.grpc.timeout_in_ms";
  public static final String TIKV_GRPC_INGEST_TIMEOUT = "tikv.grpc.ingest_timeout_in_ms";
  public static final String TIKV_GRPC_FORWARD_TIMEOUT = "tikv.grpc.forward_timeout_in_ms";
  public static final String TIKV_GRPC_WARM_UP_TIMEOUT = "tikv.grpc.warm_up_timeout_in_ms";
  public static final String TIKV_PD_FIRST_GET_MEMBER_TIMEOUT =
      "tikv.grpc.pd_first_get_member_timeout_in_ms";
  public static final String TIKV_GRPC_SCAN_TIMEOUT = "tikv.grpc.scan_timeout_in_ms";
  public static final String TIKV_GRPC_SCAN_BATCH_SIZE = "tikv.grpc.scan_batch_size";
  public static final String TIKV_GRPC_MAX_FRAME_SIZE = "tikv.grpc.max_frame_size";
  public static final String TIKV_GRPC_KEEPALIVE_TIME = "tikv.grpc.keepalive_time";
  public static final String TIKV_GRPC_KEEPALIVE_TIMEOUT = "tikv.grpc.keepalive_timeout";
  public static final String TIKV_GRPC_IDLE_TIMEOUT = "tikv.grpc.idle_timeout";

  public static final String TIKV_INDEX_SCAN_BATCH_SIZE = "tikv.index.scan_batch_size";
  public static final String TIKV_INDEX_SCAN_CONCURRENCY = "tikv.index.scan_concurrency";
  public static final String TIKV_TABLE_SCAN_CONCURRENCY = "tikv.table.scan_concurrency";

  public static final String TIKV_BATCH_GET_CONCURRENCY = "tikv.batch_get_concurrency";
  public static final String TIKV_BATCH_PUT_CONCURRENCY = "tikv.batch_put_concurrency";
  public static final String TIKV_BATCH_DELETE_CONCURRENCY = "tikv.batch_delete_concurrency";
  public static final String TIKV_BATCH_SCAN_CONCURRENCY = "tikv.batch_scan_concurrency";
  public static final String TIKV_DELETE_RANGE_CONCURRENCY = "tikv.delete_range_concurrency";

  public static final String TIKV_REQUEST_COMMAND_PRIORITY = "tikv.request.command.priority";
  public static final String TIKV_REQUEST_ISOLATION_LEVEL = "tikv.request.isolation.level";

  public static final String TIKV_SHOW_ROWID = "tikv.show_rowid";
  public static final String TIKV_DB_PREFIX = "tikv.db_prefix";
  public static final String TIKV_KV_CLIENT_CONCURRENCY = "tikv.kv_client_concurrency";

  public static final String TIKV_KV_MODE = "tikv.kv_mode";
  public static final String TIKV_REPLICA_READ = "tikv.replica_read";

  public static final String TIKV_METRICS_ENABLE = "tikv.metrics.enable";
  public static final String TIKV_METRICS_PORT = "tikv.metrics.port";

  public static final String TIKV_NETWORK_MAPPING_NAME = "tikv.network.mapping";
  public static final String TIKV_ENABLE_GRPC_FORWARD = "tikv.enable_grpc_forward";
  public static final String TIKV_GRPC_HEALTH_CHECK_TIMEOUT = "tikv.grpc.health_check_timeout";
  public static final String TIKV_HEALTH_CHECK_PERIOD_DURATION =
      "tikv.health_check_period_duration";

  public static final String TIKV_ENABLE_ATOMIC_FOR_CAS = "tikv.enable_atomic_for_cas";

  public static final String TIKV_IMPORTER_MAX_KV_BATCH_BYTES = "tikv.importer.max_kv_batch_bytes";
  public static final String TIKV_IMPORTER_MAX_KV_BATCH_SIZE = "tikv.importer.max_kv_batch_size";

  public static final String TIKV_SCATTER_WAIT_SECONDS = "tikv.scatter_wait_seconds";

  public static final String TIKV_RAWKV_DEFAULT_BACKOFF_IN_MS = "tikv.rawkv.default_backoff_in_ms";
  public static final String TIKV_RAWKV_READ_TIMEOUT_IN_MS = "tikv.rawkv.read_timeout_in_ms";
  public static final String TIKV_RAWKV_WRITE_TIMEOUT_IN_MS = "tikv.rawkv.write_timeout_in_ms";
  public static final String TIKV_RAWKV_BATCH_READ_TIMEOUT_IN_MS =
      "tikv.rawkv.batch_read_timeout_in_ms";
  public static final String TIKV_RAWKV_BATCH_WRITE_TIMEOUT_IN_MS =
      "tikv.rawkv.batch_write_timeout_in_ms";
  public static final String TIKV_RAWKV_SCAN_TIMEOUT_IN_MS = "tikv.rawkv.scan_timeout_in_ms";
  public static final String TIKV_RAWKV_CLEAN_TIMEOUT_IN_MS = "tikv.rawkv.clean_timeout_in_ms";
  public static final String TIKV_BO_REGION_MISS_BASE_IN_MS = "tikv.bo_region_miss_base_in_ms";
  public static final String TIKV_RAWKV_READ_SLOWLOG_IN_MS = "tikv.rawkv.read_slowlog_in_ms";
  public static final String TIKV_RAWKV_WRITE_SLOWLOG_IN_MS = "tikv.rawkv.write_slowlog_in_ms";
  public static final String TIKV_RAWKV_BATCH_READ_SLOWLOG_IN_MS =
      "tikv.rawkv.batch_read_slowlog_in_ms";
  public static final String TIKV_RAWKV_BATCH_WRITE_SLOWLOG_IN_MS =
      "tikv.rawkv.batch_write_slowlog_in_ms";
  public static final String TIKV_RAWKV_SCAN_SLOWLOG_IN_MS = "tikv.rawkv.scan_slowlog_in_ms";

  public static final String TIKV_TLS_ENABLE = "tikv.tls_enable";
  public static final String TIKV_TRUST_CERT_COLLECTION = "tikv.trust_cert_collection";
  public static final String TIKV_KEY_CERT_CHAIN = "tikv.key_cert_chain";
  public static final String TIKV_KEY_FILE = "tikv.key_file";

  public static final String TiKV_CIRCUIT_BREAK_ENABLE = "tikv.circuit_break.enable";
  public static final String TiKV_CIRCUIT_BREAK_AVAILABILITY_WINDOW_IN_SECONDS =
      "tikv.circuit_break.trigger.availability.window_in_seconds";
  public static final String TiKV_CIRCUIT_BREAK_AVAILABILITY_ERROR_THRESHOLD_PERCENTAGE =
      "tikv.circuit_break.trigger.availability.error_threshold_percentage";
  public static final String TiKV_CIRCUIT_BREAK_AVAILABILITY_REQUEST_VOLUMN_THRESHOLD =
      "tikv.circuit_break.trigger.availability.request_volumn_threshold";
  public static final String TiKV_CIRCUIT_BREAK_SLEEP_WINDOW_IN_SECONDS =
      "tikv.circuit_break.trigger.sleep_window_in_seconds";
  public static final String TiKV_CIRCUIT_BREAK_ATTEMPT_REQUEST_COUNT =
      "tikv.circuit_break.trigger.attempt_request_count";

  public static final String TIKV_SCAN_REGIONS_LIMIT = "tikv.scan_regions_limit";

  public static final String TIFLASH_ENABLE = "tiflash.enable";
  public static final String TIKV_WARM_UP_ENABLE = "tikv.warm_up.enable";
  public static final String DEF_PD_ADDRESSES = "127.0.0.1:2379";
  public static final String DEF_TIMEOUT = "200ms";
  public static final String DEF_TIKV_GRPC_INGEST_TIMEOUT = "200s";
  public static final String DEF_FORWARD_TIMEOUT = "300ms";
  public static final String DEF_TIKV_GRPC_WARM_UP_TIMEOUT = "5000ms";
  public static final String DEF_TIKV_PD_FIRST_GET_MEMBER_TIMEOUT = "10000ms";
  public static final String DEF_SCAN_TIMEOUT = "20s";
  public static final int DEF_CHECK_HEALTH_TIMEOUT = 100;
  public static final int DEF_HEALTH_CHECK_PERIOD_DURATION = 300;
  public static final int DEF_SCAN_BATCH_SIZE = 10240;
  public static final int DEF_MAX_FRAME_SIZE = 268435456 * 2; // 256 * 2 MB
  public static final int DEF_INDEX_SCAN_BATCH_SIZE = 20000;
  public static final int DEF_REGION_SCAN_DOWNGRADE_THRESHOLD = 10000000;
  // if keyRange size per request exceeds this limit, the request might be too large to be accepted
  // by TiKV(maximum request size accepted by TiKV is around 1MB)
  public static final int MAX_REQUEST_KEY_RANGE_SIZE = 20000;
  public static final int DEF_INDEX_SCAN_CONCURRENCY = 5;
  public static final int DEF_TABLE_SCAN_CONCURRENCY = 512;
  public static final int DEF_BATCH_GET_CONCURRENCY = 20;
  public static final int DEF_BATCH_PUT_CONCURRENCY = 20;
  public static final int DEF_BATCH_DELETE_CONCURRENCY = 20;
  public static final int DEF_BATCH_SCAN_CONCURRENCY = 5;
  public static final int DEF_DELETE_RANGE_CONCURRENCY = 20;
  public static final Kvrpcpb.CommandPri DEF_COMMAND_PRIORITY = Kvrpcpb.CommandPri.Low;
  public static final Kvrpcpb.IsolationLevel DEF_ISOLATION_LEVEL = Kvrpcpb.IsolationLevel.SI;
  public static final boolean DEF_SHOW_ROWID = false;
  public static final String DEF_DB_PREFIX = "";
  public static final int DEF_KV_CLIENT_CONCURRENCY = 10;
  public static final TiConfiguration.KVMode DEF_KV_MODE = TiConfiguration.KVMode.TXN;
  public static final String DEF_REPLICA_READ = "LEADER";
  public static final boolean DEF_METRICS_ENABLE = false;
  public static final int DEF_METRICS_PORT = 3140;
  public static final String DEF_TIKV_NETWORK_MAPPING_NAME = "";
  public static final boolean DEF_GRPC_FORWARD_ENABLE = true;
  public static final boolean DEF_TIKV_ENABLE_ATOMIC_FOR_CAS = false;

  public static final int DEF_TIKV_IMPORTER_MAX_KV_BATCH_BYTES = 1024 * 1024;
  public static final int DEF_TIKV_IMPORTER_MAX_KV_BATCH_SIZE = 1024 * 32;
  public static final int DEF_TIKV_SCATTER_WAIT_SECONDS = 300;
  public static final int DEF_TIKV_RAWKV_DEFAULT_BACKOFF_IN_MS = BackOffer.RAWKV_MAX_BACKOFF;

  public static final int DEF_TIKV_RAWKV_READ_TIMEOUT_IN_MS = 2000;
  public static final int DEF_TIKV_RAWKV_WRITE_TIMEOUT_IN_MS = 2000;
  public static final int DEF_TIKV_RAWKV_BATCH_READ_TIMEOUT_IN_MS = 2000;
  public static final int DEF_TIKV_RAWKV_BATCH_WRITE_TIMEOUT_IN_MS = 2000;
  public static final int DEF_TIKV_RAWKV_SCAN_TIMEOUT_IN_MS = 10000;
  public static final int DEF_TIKV_RAWKV_CLEAN_TIMEOUT_IN_MS = 600000;

  public static final int DEF_TIKV_BO_REGION_MISS_BASE_IN_MS = 20;
  public static final String DEF_TIKV_RAWKV_SCAN_SLOWLOG_IN_MS = "5000";

  public static final String NORMAL_COMMAND_PRIORITY = "NORMAL";
  public static final String LOW_COMMAND_PRIORITY = "LOW";
  public static final String HIGH_COMMAND_PRIORITY = "HIGH";

  public static final String SNAPSHOT_ISOLATION_LEVEL = "SI";
  public static final String READ_COMMITTED_ISOLATION_LEVEL = "RC";

  public static final String RAW_KV_MODE = "RAW";
  public static final String TXN_KV_MODE = "TXN";

  public static final String LEADER = "LEADER";
  public static final String FOLLOWER = "FOLLOWER";
  public static final String LEADER_AND_FOLLOWER = "LEADER_AND_FOLLOWER";

  public static final int DEF_TIKV_GRPC_KEEPALIVE_TIME = 10;
  public static final int DEF_TIKV_GRPC_KEEPALIVE_TIMEOUT = 3;
  public static final int DEF_TIKV_GRPC_IDLE_TIMEOUT = 60;
  public static final boolean DEF_TIKV_TLS_ENABLE = false;
  public static final boolean DEF_TIFLASH_ENABLE = false;
  public static final boolean DEF_TIKV_WARM_UP_ENABLE = true;

  public static final boolean DEF_TiKV_CIRCUIT_BREAK_ENABLE = false;
  public static final int DEF_TiKV_CIRCUIT_BREAK_AVAILABILITY_WINDOW_IN_SECONDS = 60;
  public static final int DEF_TiKV_CIRCUIT_BREAK_AVAILABILITY_ERROR_THRESHOLD_PERCENTAGE = 100;
  public static final int DEF_TiKV_CIRCUIT_BREAK_AVAILABILITY_REQUST_VOLUMN_THRESHOLD = 10;
  public static final int DEF_TiKV_CIRCUIT_BREAK_SLEEP_WINDOW_IN_SECONDS = 20;
  public static final int DEF_TiKV_CIRCUIT_BREAK_ATTEMPT_REQUEST_COUNT = 10;

  public static final int DEF_TIKV_SCAN_REGIONS_LIMIT = 1000;
}
