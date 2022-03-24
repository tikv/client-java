/*
 * Copyright 2021 TiKV Project Authors.
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

import static org.tikv.common.ConfigUtils.DEF_BATCH_DELETE_CONCURRENCY;
import static org.tikv.common.ConfigUtils.DEF_BATCH_GET_CONCURRENCY;
import static org.tikv.common.ConfigUtils.DEF_BATCH_PUT_CONCURRENCY;
import static org.tikv.common.ConfigUtils.DEF_BATCH_SCAN_CONCURRENCY;
import static org.tikv.common.ConfigUtils.DEF_CHECK_HEALTH_TIMEOUT;
import static org.tikv.common.ConfigUtils.DEF_DB_PREFIX;
import static org.tikv.common.ConfigUtils.DEF_DELETE_RANGE_CONCURRENCY;
import static org.tikv.common.ConfigUtils.DEF_FORWARD_TIMEOUT;
import static org.tikv.common.ConfigUtils.DEF_GRPC_FORWARD_ENABLE;
import static org.tikv.common.ConfigUtils.DEF_HEALTH_CHECK_PERIOD_DURATION;
import static org.tikv.common.ConfigUtils.DEF_INDEX_SCAN_BATCH_SIZE;
import static org.tikv.common.ConfigUtils.DEF_INDEX_SCAN_CONCURRENCY;
import static org.tikv.common.ConfigUtils.DEF_KV_CLIENT_CONCURRENCY;
import static org.tikv.common.ConfigUtils.DEF_MAX_FRAME_SIZE;
import static org.tikv.common.ConfigUtils.DEF_METRICS_ENABLE;
import static org.tikv.common.ConfigUtils.DEF_METRICS_PORT;
import static org.tikv.common.ConfigUtils.DEF_PD_ADDRESSES;
import static org.tikv.common.ConfigUtils.DEF_REPLICA_READ;
import static org.tikv.common.ConfigUtils.DEF_SCAN_BATCH_SIZE;
import static org.tikv.common.ConfigUtils.DEF_SCAN_TIMEOUT;
import static org.tikv.common.ConfigUtils.DEF_SHOW_ROWID;
import static org.tikv.common.ConfigUtils.DEF_TABLE_SCAN_CONCURRENCY;
import static org.tikv.common.ConfigUtils.DEF_TIFLASH_ENABLE;
import static org.tikv.common.ConfigUtils.DEF_TIKV_API_VERSION;
import static org.tikv.common.ConfigUtils.DEF_TIKV_BO_REGION_MISS_BASE_IN_MS;
import static org.tikv.common.ConfigUtils.DEF_TIKV_ENABLE_ATOMIC_FOR_CAS;
import static org.tikv.common.ConfigUtils.DEF_TIKV_GRPC_IDLE_TIMEOUT;
import static org.tikv.common.ConfigUtils.DEF_TIKV_GRPC_INGEST_TIMEOUT;
import static org.tikv.common.ConfigUtils.DEF_TIKV_GRPC_KEEPALIVE_TIME;
import static org.tikv.common.ConfigUtils.DEF_TIKV_GRPC_KEEPALIVE_TIMEOUT;
import static org.tikv.common.ConfigUtils.DEF_TIKV_GRPC_WARM_UP_TIMEOUT;
import static org.tikv.common.ConfigUtils.DEF_TIKV_IMPORTER_MAX_KV_BATCH_BYTES;
import static org.tikv.common.ConfigUtils.DEF_TIKV_IMPORTER_MAX_KV_BATCH_SIZE;
import static org.tikv.common.ConfigUtils.DEF_TIKV_NETWORK_MAPPING_NAME;
import static org.tikv.common.ConfigUtils.DEF_TIKV_PD_FIRST_GET_MEMBER_TIMEOUT;
import static org.tikv.common.ConfigUtils.DEF_TIKV_RAWKV_BATCH_READ_TIMEOUT_IN_MS;
import static org.tikv.common.ConfigUtils.DEF_TIKV_RAWKV_BATCH_WRITE_TIMEOUT_IN_MS;
import static org.tikv.common.ConfigUtils.DEF_TIKV_RAWKV_CLEAN_TIMEOUT_IN_MS;
import static org.tikv.common.ConfigUtils.DEF_TIKV_RAWKV_DEFAULT_BACKOFF_IN_MS;
import static org.tikv.common.ConfigUtils.DEF_TIKV_RAWKV_READ_TIMEOUT_IN_MS;
import static org.tikv.common.ConfigUtils.DEF_TIKV_RAWKV_SCAN_SLOWLOG_IN_MS;
import static org.tikv.common.ConfigUtils.DEF_TIKV_RAWKV_SCAN_TIMEOUT_IN_MS;
import static org.tikv.common.ConfigUtils.DEF_TIKV_RAWKV_WRITE_TIMEOUT_IN_MS;
import static org.tikv.common.ConfigUtils.DEF_TIKV_SCAN_REGIONS_LIMIT;
import static org.tikv.common.ConfigUtils.DEF_TIKV_SCATTER_WAIT_SECONDS;
import static org.tikv.common.ConfigUtils.DEF_TIKV_TLS_ENABLE;
import static org.tikv.common.ConfigUtils.DEF_TIKV_USE_JKS;
import static org.tikv.common.ConfigUtils.DEF_TIKV_WARM_UP_ENABLE;
import static org.tikv.common.ConfigUtils.DEF_TIMEOUT;
import static org.tikv.common.ConfigUtils.DEF_TiKV_CIRCUIT_BREAK_ATTEMPT_REQUEST_COUNT;
import static org.tikv.common.ConfigUtils.DEF_TiKV_CIRCUIT_BREAK_AVAILABILITY_ERROR_THRESHOLD_PERCENTAGE;
import static org.tikv.common.ConfigUtils.DEF_TiKV_CIRCUIT_BREAK_AVAILABILITY_REQUST_VOLUMN_THRESHOLD;
import static org.tikv.common.ConfigUtils.DEF_TiKV_CIRCUIT_BREAK_AVAILABILITY_WINDOW_IN_SECONDS;
import static org.tikv.common.ConfigUtils.DEF_TiKV_CIRCUIT_BREAK_ENABLE;
import static org.tikv.common.ConfigUtils.DEF_TiKV_CIRCUIT_BREAK_SLEEP_WINDOW_IN_SECONDS;
import static org.tikv.common.ConfigUtils.FOLLOWER;
import static org.tikv.common.ConfigUtils.HIGH_COMMAND_PRIORITY;
import static org.tikv.common.ConfigUtils.LEADER_AND_FOLLOWER;
import static org.tikv.common.ConfigUtils.LOW_COMMAND_PRIORITY;
import static org.tikv.common.ConfigUtils.NORMAL_COMMAND_PRIORITY;
import static org.tikv.common.ConfigUtils.RAW_KV_MODE;
import static org.tikv.common.ConfigUtils.READ_COMMITTED_ISOLATION_LEVEL;
import static org.tikv.common.ConfigUtils.SNAPSHOT_ISOLATION_LEVEL;
import static org.tikv.common.ConfigUtils.TIFLASH_ENABLE;
import static org.tikv.common.ConfigUtils.TIKV_API_VERSION;
import static org.tikv.common.ConfigUtils.TIKV_BATCH_DELETE_CONCURRENCY;
import static org.tikv.common.ConfigUtils.TIKV_BATCH_GET_CONCURRENCY;
import static org.tikv.common.ConfigUtils.TIKV_BATCH_PUT_CONCURRENCY;
import static org.tikv.common.ConfigUtils.TIKV_BATCH_SCAN_CONCURRENCY;
import static org.tikv.common.ConfigUtils.TIKV_BO_REGION_MISS_BASE_IN_MS;
import static org.tikv.common.ConfigUtils.TIKV_DB_PREFIX;
import static org.tikv.common.ConfigUtils.TIKV_DELETE_RANGE_CONCURRENCY;
import static org.tikv.common.ConfigUtils.TIKV_ENABLE_ATOMIC_FOR_CAS;
import static org.tikv.common.ConfigUtils.TIKV_ENABLE_GRPC_FORWARD;
import static org.tikv.common.ConfigUtils.TIKV_GRPC_FORWARD_TIMEOUT;
import static org.tikv.common.ConfigUtils.TIKV_GRPC_HEALTH_CHECK_TIMEOUT;
import static org.tikv.common.ConfigUtils.TIKV_GRPC_IDLE_TIMEOUT;
import static org.tikv.common.ConfigUtils.TIKV_GRPC_INGEST_TIMEOUT;
import static org.tikv.common.ConfigUtils.TIKV_GRPC_KEEPALIVE_TIME;
import static org.tikv.common.ConfigUtils.TIKV_GRPC_KEEPALIVE_TIMEOUT;
import static org.tikv.common.ConfigUtils.TIKV_GRPC_MAX_FRAME_SIZE;
import static org.tikv.common.ConfigUtils.TIKV_GRPC_SCAN_BATCH_SIZE;
import static org.tikv.common.ConfigUtils.TIKV_GRPC_SCAN_TIMEOUT;
import static org.tikv.common.ConfigUtils.TIKV_GRPC_TIMEOUT;
import static org.tikv.common.ConfigUtils.TIKV_GRPC_WARM_UP_TIMEOUT;
import static org.tikv.common.ConfigUtils.TIKV_HEALTH_CHECK_PERIOD_DURATION;
import static org.tikv.common.ConfigUtils.TIKV_IMPORTER_MAX_KV_BATCH_BYTES;
import static org.tikv.common.ConfigUtils.TIKV_IMPORTER_MAX_KV_BATCH_SIZE;
import static org.tikv.common.ConfigUtils.TIKV_INDEX_SCAN_BATCH_SIZE;
import static org.tikv.common.ConfigUtils.TIKV_INDEX_SCAN_CONCURRENCY;
import static org.tikv.common.ConfigUtils.TIKV_JKS_KEY_PASSWORD;
import static org.tikv.common.ConfigUtils.TIKV_JKS_KEY_PATH;
import static org.tikv.common.ConfigUtils.TIKV_JKS_TRUST_PASSWORD;
import static org.tikv.common.ConfigUtils.TIKV_JKS_TRUST_PATH;
import static org.tikv.common.ConfigUtils.TIKV_KEY_CERT_CHAIN;
import static org.tikv.common.ConfigUtils.TIKV_KEY_FILE;
import static org.tikv.common.ConfigUtils.TIKV_KV_CLIENT_CONCURRENCY;
import static org.tikv.common.ConfigUtils.TIKV_KV_MODE;
import static org.tikv.common.ConfigUtils.TIKV_METRICS_ENABLE;
import static org.tikv.common.ConfigUtils.TIKV_METRICS_PORT;
import static org.tikv.common.ConfigUtils.TIKV_NETWORK_MAPPING_NAME;
import static org.tikv.common.ConfigUtils.TIKV_PD_ADDRESSES;
import static org.tikv.common.ConfigUtils.TIKV_PD_FIRST_GET_MEMBER_TIMEOUT;
import static org.tikv.common.ConfigUtils.TIKV_RAWKV_BATCH_READ_SLOWLOG_IN_MS;
import static org.tikv.common.ConfigUtils.TIKV_RAWKV_BATCH_READ_TIMEOUT_IN_MS;
import static org.tikv.common.ConfigUtils.TIKV_RAWKV_BATCH_WRITE_SLOWLOG_IN_MS;
import static org.tikv.common.ConfigUtils.TIKV_RAWKV_BATCH_WRITE_TIMEOUT_IN_MS;
import static org.tikv.common.ConfigUtils.TIKV_RAWKV_CLEAN_TIMEOUT_IN_MS;
import static org.tikv.common.ConfigUtils.TIKV_RAWKV_DEFAULT_BACKOFF_IN_MS;
import static org.tikv.common.ConfigUtils.TIKV_RAWKV_READ_SLOWLOG_IN_MS;
import static org.tikv.common.ConfigUtils.TIKV_RAWKV_READ_TIMEOUT_IN_MS;
import static org.tikv.common.ConfigUtils.TIKV_RAWKV_SCAN_SLOWLOG_IN_MS;
import static org.tikv.common.ConfigUtils.TIKV_RAWKV_SCAN_TIMEOUT_IN_MS;
import static org.tikv.common.ConfigUtils.TIKV_RAWKV_SERVER_SLOWLOG_FACTOR;
import static org.tikv.common.ConfigUtils.TIKV_RAWKV_WRITE_SLOWLOG_IN_MS;
import static org.tikv.common.ConfigUtils.TIKV_RAWKV_WRITE_TIMEOUT_IN_MS;
import static org.tikv.common.ConfigUtils.TIKV_REPLICA_READ;
import static org.tikv.common.ConfigUtils.TIKV_REQUEST_COMMAND_PRIORITY;
import static org.tikv.common.ConfigUtils.TIKV_REQUEST_ISOLATION_LEVEL;
import static org.tikv.common.ConfigUtils.TIKV_SCAN_REGIONS_LIMIT;
import static org.tikv.common.ConfigUtils.TIKV_SCATTER_WAIT_SECONDS;
import static org.tikv.common.ConfigUtils.TIKV_SHOW_ROWID;
import static org.tikv.common.ConfigUtils.TIKV_TABLE_SCAN_CONCURRENCY;
import static org.tikv.common.ConfigUtils.TIKV_TLS_ENABLE;
import static org.tikv.common.ConfigUtils.TIKV_TRUST_CERT_COLLECTION;
import static org.tikv.common.ConfigUtils.TIKV_USE_JKS;
import static org.tikv.common.ConfigUtils.TIKV_WARM_UP_ENABLE;
import static org.tikv.common.ConfigUtils.TXN_KV_MODE;
import static org.tikv.common.ConfigUtils.TiKV_CIRCUIT_BREAK_ATTEMPT_REQUEST_COUNT;
import static org.tikv.common.ConfigUtils.TiKV_CIRCUIT_BREAK_AVAILABILITY_ERROR_THRESHOLD_PERCENTAGE;
import static org.tikv.common.ConfigUtils.TiKV_CIRCUIT_BREAK_AVAILABILITY_REQUEST_VOLUMN_THRESHOLD;
import static org.tikv.common.ConfigUtils.TiKV_CIRCUIT_BREAK_AVAILABILITY_WINDOW_IN_SECONDS;
import static org.tikv.common.ConfigUtils.TiKV_CIRCUIT_BREAK_ENABLE;
import static org.tikv.common.ConfigUtils.TiKV_CIRCUIT_BREAK_SLEEP_WINDOW_IN_SECONDS;

import io.grpc.Metadata;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.pd.PDUtils;
import org.tikv.common.replica.ReplicaSelector;
import org.tikv.kvproto.Kvrpcpb.CommandPri;
import org.tikv.kvproto.Kvrpcpb.IsolationLevel;

public class TiConfiguration implements Serializable {
  public static enum ApiVersion {
    V1,
    V2;

    public static ApiVersion fromInt(int version) {
      switch (version) {
        case 1:
          return V1;
        case 2:
          return V2;
        default:
          throw new IllegalArgumentException("unknown api version " + version);
      }
    }

    public boolean isV1() {
      return this == V1;
    }

    public boolean isV2() {
      return this == V2;
    }
  }

  private static final Logger logger = LoggerFactory.getLogger(TiConfiguration.class);
  private static final ConcurrentHashMap<String, String> settings = new ConcurrentHashMap<>();
  public static final Metadata.Key<String> FORWARD_META_DATA_KEY =
      Metadata.Key.of("tikv-forwarded-host", Metadata.ASCII_STRING_MARSHALLER);
  public static final Metadata.Key<String> PD_FORWARD_META_DATA_KEY =
      Metadata.Key.of("pd-forwarded-host", Metadata.ASCII_STRING_MARSHALLER);
  public static final String API_V2_RAW_PREFIX = "r";
  public static final String API_V2_TXN_PREFIX = "x";

  static {
    // priority: system environment > config file > default
    loadFromSystemProperties();
    loadFromConfigurationFile();
    loadFromDefaultProperties();
    listAll();
  }

  private static void loadFromSystemProperties() {
    for (Map.Entry<String, String> prop : Utils.getSystemProperties().entrySet()) {
      if (prop.getKey().startsWith("tikv.")) {
        set(prop.getKey(), prop.getValue());
      }
    }
  }

  private static void loadFromConfigurationFile() {
    try (InputStream input =
        TiConfiguration.class
            .getClassLoader()
            .getResourceAsStream(ConfigUtils.TIKV_CONFIGURATION_FILENAME)) {
      Properties properties = new Properties();

      if (input == null) {
        logger.warn("Unable to find " + ConfigUtils.TIKV_CONFIGURATION_FILENAME);
        return;
      }

      logger.info("loading " + ConfigUtils.TIKV_CONFIGURATION_FILENAME);
      properties.load(input);
      for (String key : properties.stringPropertyNames()) {
        if (key.startsWith("tikv.")) {
          String value = properties.getProperty(key);
          setIfMissing(key, value);
        }
      }
    } catch (IOException e) {
      logger.error("load config file error", e);
    }
  }

  private static void loadFromDefaultProperties() {
    setIfMissing(TIKV_PD_ADDRESSES, DEF_PD_ADDRESSES);
    setIfMissing(TIKV_GRPC_TIMEOUT, DEF_TIMEOUT);
    setIfMissing(TIKV_GRPC_INGEST_TIMEOUT, DEF_TIKV_GRPC_INGEST_TIMEOUT);
    setIfMissing(TIKV_GRPC_FORWARD_TIMEOUT, DEF_FORWARD_TIMEOUT);
    setIfMissing(TIKV_GRPC_WARM_UP_TIMEOUT, DEF_TIKV_GRPC_WARM_UP_TIMEOUT);
    setIfMissing(TIKV_PD_FIRST_GET_MEMBER_TIMEOUT, DEF_TIKV_PD_FIRST_GET_MEMBER_TIMEOUT);
    setIfMissing(TIKV_GRPC_SCAN_TIMEOUT, DEF_SCAN_TIMEOUT);
    setIfMissing(TIKV_GRPC_SCAN_BATCH_SIZE, DEF_SCAN_BATCH_SIZE);
    setIfMissing(TIKV_GRPC_MAX_FRAME_SIZE, DEF_MAX_FRAME_SIZE);
    setIfMissing(TIKV_INDEX_SCAN_BATCH_SIZE, DEF_INDEX_SCAN_BATCH_SIZE);
    setIfMissing(TIKV_INDEX_SCAN_CONCURRENCY, DEF_INDEX_SCAN_CONCURRENCY);
    setIfMissing(TIKV_TABLE_SCAN_CONCURRENCY, DEF_TABLE_SCAN_CONCURRENCY);
    setIfMissing(TIKV_BATCH_GET_CONCURRENCY, DEF_BATCH_GET_CONCURRENCY);
    setIfMissing(TIKV_BATCH_PUT_CONCURRENCY, DEF_BATCH_PUT_CONCURRENCY);
    setIfMissing(TIKV_BATCH_DELETE_CONCURRENCY, DEF_BATCH_DELETE_CONCURRENCY);
    setIfMissing(TIKV_BATCH_SCAN_CONCURRENCY, DEF_BATCH_SCAN_CONCURRENCY);
    setIfMissing(TIKV_DELETE_RANGE_CONCURRENCY, DEF_DELETE_RANGE_CONCURRENCY);
    setIfMissing(TIKV_REQUEST_COMMAND_PRIORITY, LOW_COMMAND_PRIORITY);
    setIfMissing(TIKV_REQUEST_ISOLATION_LEVEL, SNAPSHOT_ISOLATION_LEVEL);
    setIfMissing(TIKV_REQUEST_ISOLATION_LEVEL, SNAPSHOT_ISOLATION_LEVEL);
    setIfMissing(TIKV_SHOW_ROWID, DEF_SHOW_ROWID);
    setIfMissing(TIKV_DB_PREFIX, DEF_DB_PREFIX);
    setIfMissing(TIKV_DB_PREFIX, DEF_DB_PREFIX);
    setIfMissing(TIKV_KV_CLIENT_CONCURRENCY, DEF_KV_CLIENT_CONCURRENCY);
    setIfMissing(TIKV_KV_MODE, TXN_KV_MODE);
    setIfMissing(TIKV_REPLICA_READ, DEF_REPLICA_READ);
    setIfMissing(TIKV_METRICS_ENABLE, DEF_METRICS_ENABLE);
    setIfMissing(TIKV_METRICS_PORT, DEF_METRICS_PORT);
    setIfMissing(TIKV_NETWORK_MAPPING_NAME, DEF_TIKV_NETWORK_MAPPING_NAME);
    setIfMissing(TIKV_ENABLE_GRPC_FORWARD, DEF_GRPC_FORWARD_ENABLE);
    setIfMissing(TIKV_GRPC_HEALTH_CHECK_TIMEOUT, DEF_CHECK_HEALTH_TIMEOUT);
    setIfMissing(TIKV_HEALTH_CHECK_PERIOD_DURATION, DEF_HEALTH_CHECK_PERIOD_DURATION);
    setIfMissing(TIKV_ENABLE_ATOMIC_FOR_CAS, DEF_TIKV_ENABLE_ATOMIC_FOR_CAS);
    setIfMissing(TIKV_IMPORTER_MAX_KV_BATCH_BYTES, DEF_TIKV_IMPORTER_MAX_KV_BATCH_BYTES);
    setIfMissing(TIKV_IMPORTER_MAX_KV_BATCH_SIZE, DEF_TIKV_IMPORTER_MAX_KV_BATCH_SIZE);
    setIfMissing(TIKV_SCATTER_WAIT_SECONDS, DEF_TIKV_SCATTER_WAIT_SECONDS);
    setIfMissing(TIKV_RAWKV_DEFAULT_BACKOFF_IN_MS, DEF_TIKV_RAWKV_DEFAULT_BACKOFF_IN_MS);
    setIfMissing(TIKV_GRPC_KEEPALIVE_TIME, DEF_TIKV_GRPC_KEEPALIVE_TIME);
    setIfMissing(TIKV_GRPC_KEEPALIVE_TIMEOUT, DEF_TIKV_GRPC_KEEPALIVE_TIMEOUT);
    setIfMissing(TIKV_GRPC_IDLE_TIMEOUT, DEF_TIKV_GRPC_IDLE_TIMEOUT);
    setIfMissing(TIKV_TLS_ENABLE, DEF_TIKV_TLS_ENABLE);
    setIfMissing(TIKV_USE_JKS, DEF_TIKV_USE_JKS);
    setIfMissing(TIFLASH_ENABLE, DEF_TIFLASH_ENABLE);
    setIfMissing(TIKV_WARM_UP_ENABLE, DEF_TIKV_WARM_UP_ENABLE);
    setIfMissing(TIKV_RAWKV_READ_TIMEOUT_IN_MS, DEF_TIKV_RAWKV_READ_TIMEOUT_IN_MS);
    setIfMissing(TIKV_RAWKV_WRITE_TIMEOUT_IN_MS, DEF_TIKV_RAWKV_WRITE_TIMEOUT_IN_MS);
    setIfMissing(TIKV_RAWKV_BATCH_READ_TIMEOUT_IN_MS, DEF_TIKV_RAWKV_BATCH_READ_TIMEOUT_IN_MS);
    setIfMissing(TIKV_RAWKV_BATCH_WRITE_TIMEOUT_IN_MS, DEF_TIKV_RAWKV_BATCH_WRITE_TIMEOUT_IN_MS);
    setIfMissing(TIKV_RAWKV_SCAN_TIMEOUT_IN_MS, DEF_TIKV_RAWKV_SCAN_TIMEOUT_IN_MS);
    setIfMissing(TIKV_RAWKV_CLEAN_TIMEOUT_IN_MS, DEF_TIKV_RAWKV_CLEAN_TIMEOUT_IN_MS);
    setIfMissing(TIKV_BO_REGION_MISS_BASE_IN_MS, DEF_TIKV_BO_REGION_MISS_BASE_IN_MS);
    setIfMissing(TIKV_RAWKV_SCAN_SLOWLOG_IN_MS, DEF_TIKV_RAWKV_SCAN_SLOWLOG_IN_MS);
    setIfMissing(TiKV_CIRCUIT_BREAK_ENABLE, DEF_TiKV_CIRCUIT_BREAK_ENABLE);
    setIfMissing(
        TiKV_CIRCUIT_BREAK_AVAILABILITY_WINDOW_IN_SECONDS,
        DEF_TiKV_CIRCUIT_BREAK_AVAILABILITY_WINDOW_IN_SECONDS);
    setIfMissing(
        TiKV_CIRCUIT_BREAK_AVAILABILITY_ERROR_THRESHOLD_PERCENTAGE,
        DEF_TiKV_CIRCUIT_BREAK_AVAILABILITY_ERROR_THRESHOLD_PERCENTAGE);
    setIfMissing(
        TiKV_CIRCUIT_BREAK_AVAILABILITY_REQUEST_VOLUMN_THRESHOLD,
        DEF_TiKV_CIRCUIT_BREAK_AVAILABILITY_REQUST_VOLUMN_THRESHOLD);
    setIfMissing(
        TiKV_CIRCUIT_BREAK_SLEEP_WINDOW_IN_SECONDS, DEF_TiKV_CIRCUIT_BREAK_SLEEP_WINDOW_IN_SECONDS);
    setIfMissing(
        TiKV_CIRCUIT_BREAK_ATTEMPT_REQUEST_COUNT, DEF_TiKV_CIRCUIT_BREAK_ATTEMPT_REQUEST_COUNT);
    setIfMissing(TIKV_SCAN_REGIONS_LIMIT, DEF_TIKV_SCAN_REGIONS_LIMIT);

    setIfMissing(TIKV_API_VERSION, DEF_TIKV_API_VERSION);
  }

  public static void listAll() {
    logger.info("static configurations are:" + new ArrayList<>(settings.entrySet()).toString());
  }

  private static void set(String key, String value) {
    if (key == null) {
      throw new NullPointerException("null key");
    }
    if (value == null) {
      throw new NullPointerException("null value for " + key);
    }
    settings.put(key, value);
  }

  private static void setIfMissing(String key, int value) {
    setIfMissing(key, String.valueOf(value));
  }

  private static void setIfMissing(String key, boolean value) {
    setIfMissing(key, String.valueOf(value));
  }

  private static void setIfMissing(String key, String value) {
    if (key == null) {
      throw new NullPointerException("null key");
    }
    if (value == null) {
      throw new NullPointerException("null value for " + key);
    }
    settings.putIfAbsent(key, value);
  }

  private static Optional<String> getOption(String key) {
    return Optional.ofNullable(settings.get(key));
  }

  private static String get(String key) {
    Optional<String> option = getOption(key);
    if (!option.isPresent()) {
      throw new NoSuchElementException(key);
    }
    return option.get();
  }

  public static int getInt(String key) {
    return Integer.parseInt(get(key));
  }

  public static Optional<Integer> getIntOption(String key) {
    return getOption(key).map(Integer::parseInt);
  }

  private static int getInt(String key, int defaultValue) {
    try {
      return getOption(key).map(Integer::parseInt).orElse(defaultValue);
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }

  private static long getLong(String key) {
    return Long.parseLong(get(key));
  }

  private static long getLong(String key, long defaultValue) {
    try {
      return getOption(key).map(Long::parseLong).orElse(defaultValue);
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }

  private static double getDouble(String key) {
    return Double.parseDouble(get(key));
  }

  private static double getDouble(String key, double defaultValue) {
    try {
      return getOption(key).map(Double::parseDouble).orElse(defaultValue);
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }

  private static boolean getBoolean(String key) {
    return Boolean.parseBoolean(get(key));
  }

  private static boolean getBoolean(String key, boolean defaultValue) {
    try {
      return getOption(key).map(Boolean::parseBoolean).orElse(defaultValue);
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }

  private static Long getTimeAsMs(String key) {
    return Utils.timeStringAsMs(get(key));
  }

  private static Long getTimeAsSeconds(String key) {
    return Utils.timeStringAsSec(get(key));
  }

  private static List<URI> getPdAddrs(String key) {
    Optional<String> pdAddrs = getOption(key);
    if (pdAddrs.isPresent()) {
      return strToURI(pdAddrs.get());
    } else {
      return new ArrayList<>();
    }
  }

  private static CommandPri getCommandPri(String key) {
    String priority = get(key).toUpperCase(Locale.ROOT);
    switch (priority) {
      case NORMAL_COMMAND_PRIORITY:
        return CommandPri.Normal;
      case LOW_COMMAND_PRIORITY:
        return CommandPri.Low;
      case HIGH_COMMAND_PRIORITY:
        return CommandPri.High;
      default:
        return CommandPri.UNRECOGNIZED;
    }
  }

  private static IsolationLevel getIsolationLevel(String key) {
    String isolationLevel = get(key).toUpperCase(Locale.ROOT);
    switch (isolationLevel) {
      case READ_COMMITTED_ISOLATION_LEVEL:
        return IsolationLevel.RC;
      case SNAPSHOT_ISOLATION_LEVEL:
        return IsolationLevel.SI;
      default:
        return IsolationLevel.UNRECOGNIZED;
    }
  }

  private static KVMode getKvMode(String key) {
    if (get(key).toUpperCase(Locale.ROOT).equals(RAW_KV_MODE)) {
      return KVMode.RAW;
    } else {
      return KVMode.TXN;
    }
  }

  private static ReplicaRead getReplicaRead(String key) {
    String value = get(key).toUpperCase(Locale.ROOT);
    if (FOLLOWER.equals(value)) {
      return ReplicaRead.FOLLOWER;
    } else if (LEADER_AND_FOLLOWER.equals(value)) {
      return ReplicaRead.LEADER_AND_FOLLOWER;
    } else {
      return ReplicaRead.LEADER;
    }
  }

  private long timeout = getTimeAsMs(TIKV_GRPC_TIMEOUT);
  private long ingestTimeout = getTimeAsMs(TIKV_GRPC_INGEST_TIMEOUT);
  private long forwardTimeout = getTimeAsMs(TIKV_GRPC_FORWARD_TIMEOUT);
  private long warmUpTimeout = getTimeAsMs(TIKV_GRPC_WARM_UP_TIMEOUT);
  private long pdFirstGetMemberTimeout = getTimeAsMs(TIKV_PD_FIRST_GET_MEMBER_TIMEOUT);
  private long scanTimeout = getTimeAsMs(TIKV_GRPC_SCAN_TIMEOUT);
  private int maxFrameSize = getInt(TIKV_GRPC_MAX_FRAME_SIZE);
  private List<URI> pdAddrs = getPdAddrs(TIKV_PD_ADDRESSES);
  private int indexScanBatchSize = getInt(TIKV_INDEX_SCAN_BATCH_SIZE);
  private int indexScanConcurrency = getInt(TIKV_INDEX_SCAN_CONCURRENCY);
  private int tableScanConcurrency = getInt(TIKV_TABLE_SCAN_CONCURRENCY);
  private int batchGetConcurrency = getInt(TIKV_BATCH_GET_CONCURRENCY);
  private int batchPutConcurrency = getInt(TIKV_BATCH_PUT_CONCURRENCY);
  private int batchDeleteConcurrency = getInt(TIKV_BATCH_DELETE_CONCURRENCY);
  private int batchScanConcurrency = getInt(TIKV_BATCH_SCAN_CONCURRENCY);
  private int deleteRangeConcurrency = getInt(TIKV_DELETE_RANGE_CONCURRENCY);
  private CommandPri commandPriority = getCommandPri(TIKV_REQUEST_COMMAND_PRIORITY);
  private IsolationLevel isolationLevel = getIsolationLevel(TIKV_REQUEST_ISOLATION_LEVEL);
  private boolean showRowId = getBoolean(TIKV_SHOW_ROWID);
  private String dbPrefix = get(TIKV_DB_PREFIX);
  private KVMode kvMode = getKvMode(TIKV_KV_MODE);
  private boolean enableGrpcForward = getBoolean(TIKV_ENABLE_GRPC_FORWARD);

  private int kvClientConcurrency = getInt(TIKV_KV_CLIENT_CONCURRENCY);
  private ReplicaRead replicaRead = getReplicaRead(TIKV_REPLICA_READ);
  private ReplicaSelector internalReplicaSelector = getReplicaSelector(replicaRead);
  private ReplicaSelector replicaSelector;

  private boolean metricsEnable = getBoolean(TIKV_METRICS_ENABLE);
  private int metricsPort = getInt(TIKV_METRICS_PORT);
  private int grpcHealthCheckTimeout = getInt(TIKV_GRPC_HEALTH_CHECK_TIMEOUT);
  private int healthCheckPeriodDuration = getInt(TIKV_HEALTH_CHECK_PERIOD_DURATION);

  private final String networkMappingName = get(TIKV_NETWORK_MAPPING_NAME);
  private HostMapping hostMapping = null;

  private boolean enableAtomicForCAS = getBoolean(TIKV_ENABLE_ATOMIC_FOR_CAS);

  private int importerMaxKVBatchBytes = getInt(TIKV_IMPORTER_MAX_KV_BATCH_BYTES);

  private int importerMaxKVBatchSize = getInt(TIKV_IMPORTER_MAX_KV_BATCH_SIZE);

  private int scatterWaitSeconds = getInt(TIKV_SCATTER_WAIT_SECONDS);

  private int rawKVDefaultBackoffInMS = getInt(TIKV_RAWKV_DEFAULT_BACKOFF_IN_MS);
  private int rawKVReadTimeoutInMS = getInt(TIKV_RAWKV_READ_TIMEOUT_IN_MS);
  private int rawKVWriteTimeoutInMS = getInt(TIKV_RAWKV_WRITE_TIMEOUT_IN_MS);
  private int rawKVBatchReadTimeoutInMS = getInt(TIKV_RAWKV_BATCH_READ_TIMEOUT_IN_MS);
  private int rawKVBatchWriteTimeoutInMS = getInt(TIKV_RAWKV_BATCH_WRITE_TIMEOUT_IN_MS);
  private int rawKVScanTimeoutInMS = getInt(TIKV_RAWKV_SCAN_TIMEOUT_IN_MS);
  private int rawKVCleanTimeoutInMS = getInt(TIKV_RAWKV_CLEAN_TIMEOUT_IN_MS);
  private Integer rawKVReadSlowLogInMS = getIntOption(TIKV_RAWKV_READ_SLOWLOG_IN_MS).orElse(null);
  private Integer rawKVWriteSlowLogInMS = getIntOption(TIKV_RAWKV_WRITE_SLOWLOG_IN_MS).orElse(null);
  private Integer rawKVBatchReadSlowLogInMS =
      getIntOption(TIKV_RAWKV_BATCH_READ_SLOWLOG_IN_MS).orElse(null);
  private Integer rawKVBatchWriteSlowLogInMS =
      getIntOption(TIKV_RAWKV_BATCH_WRITE_SLOWLOG_IN_MS).orElse(null);
  private int rawKVScanSlowLogInMS = getInt(TIKV_RAWKV_SCAN_SLOWLOG_IN_MS);
  private double rawKVServerSlowLogFactor = getDouble(TIKV_RAWKV_SERVER_SLOWLOG_FACTOR, 0.5);

  private boolean tlsEnable = getBoolean(TIKV_TLS_ENABLE);
  private String trustCertCollectionFile = getOption(TIKV_TRUST_CERT_COLLECTION).orElse(null);
  private String keyCertChainFile = getOption(TIKV_KEY_CERT_CHAIN).orElse(null);
  private String keyFile = getOption(TIKV_KEY_FILE).orElse(null);

  private boolean useJks = getBoolean(TIKV_USE_JKS);
  private String jksKeyPath = getOption(TIKV_JKS_KEY_PATH).orElse(null);
  private String jksKeyPassword = getOption(TIKV_JKS_KEY_PASSWORD).orElse(null);
  private String jksTrustPath = getOption(TIKV_JKS_TRUST_PATH).orElse(null);
  private String jksTrustPassword = getOption(TIKV_JKS_TRUST_PASSWORD).orElse(null);

  private boolean tiFlashEnable = getBoolean(TIFLASH_ENABLE);
  private boolean warmUpEnable = getBoolean(TIKV_WARM_UP_ENABLE);

  private boolean isTest = false;

  private int keepaliveTime = getInt(TIKV_GRPC_KEEPALIVE_TIME);
  private int keepaliveTimeout = getInt(TIKV_GRPC_KEEPALIVE_TIMEOUT);
  private int idleTimeout = getInt(TIKV_GRPC_IDLE_TIMEOUT);

  private boolean circuitBreakEnable = getBoolean(TiKV_CIRCUIT_BREAK_ENABLE);
  private int circuitBreakAvailabilityWindowInSeconds =
      getInt(TiKV_CIRCUIT_BREAK_AVAILABILITY_WINDOW_IN_SECONDS);
  private int circuitBreakAvailabilityErrorThresholdPercentage =
      getInt(TiKV_CIRCUIT_BREAK_AVAILABILITY_ERROR_THRESHOLD_PERCENTAGE);
  private int circuitBreakAvailabilityRequestVolumnThreshold =
      getInt(TiKV_CIRCUIT_BREAK_AVAILABILITY_REQUEST_VOLUMN_THRESHOLD);
  private int circuitBreakSleepWindowInSeconds = getInt(TiKV_CIRCUIT_BREAK_SLEEP_WINDOW_IN_SECONDS);
  private int circuitBreakAttemptRequestCount = getInt(TiKV_CIRCUIT_BREAK_ATTEMPT_REQUEST_COUNT);

  private int scanRegionsLimit = getInt(TIKV_SCAN_REGIONS_LIMIT);

  private ApiVersion apiVersion = ApiVersion.fromInt(getInt(TIKV_API_VERSION));

  public enum KVMode {
    TXN,
    RAW
  }

  public enum ReplicaRead {
    LEADER,
    FOLLOWER,
    LEADER_AND_FOLLOWER
  }

  public TiConfiguration() {
    if (rawKVServerSlowLogFactor < 0 || rawKVServerSlowLogFactor > 1) {
      throw new IllegalArgumentException("rawkv_server_slowlog_factor must be in range [0, 1]");
    }
  }

  public static TiConfiguration createDefault() {
    return new TiConfiguration();
  }

  public static TiConfiguration createDefault(String pdAddrsStr) {
    Objects.requireNonNull(pdAddrsStr, "pdAddrsStr is null");
    TiConfiguration conf = new TiConfiguration();
    conf.pdAddrs = strToURI(pdAddrsStr);
    return conf;
  }

  public static TiConfiguration createRawDefault() {
    TiConfiguration conf = new TiConfiguration();
    conf.kvMode = KVMode.RAW;
    return conf;
  }

  public static TiConfiguration createRawDefault(String pdAddrsStr) {
    Objects.requireNonNull(pdAddrsStr, "pdAddrsStr is null");
    TiConfiguration conf = new TiConfiguration();
    conf.pdAddrs = strToURI(pdAddrsStr);
    conf.kvMode = KVMode.RAW;
    return conf;
  }

  private static List<URI> strToURI(String addressStr) {
    Objects.requireNonNull(addressStr);
    String[] addrs = addressStr.split(",");
    Arrays.sort(addrs);
    return PDUtils.addrsToUrls(addrs);
  }

  public static <E> String listToString(List<E> list) {
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    for (int i = 0; i < list.size(); i++) {
      sb.append(list.get(i).toString());
      if (i != list.size() - 1) {
        sb.append(",");
      }
    }
    sb.append("]");
    return sb.toString();
  }

  public long getTimeout() {
    return timeout;
  }

  public TiConfiguration setTimeout(long timeout) {
    this.timeout = timeout;
    return this;
  }

  public long getIngestTimeout() {
    return ingestTimeout;
  }

  public void setIngestTimeout(long ingestTimeout) {
    this.ingestTimeout = ingestTimeout;
  }

  public long getForwardTimeout() {
    return forwardTimeout;
  }

  public TiConfiguration setForwardTimeout(long timeout) {
    this.forwardTimeout = timeout;
    return this;
  }

  public long getWarmUpTimeout() {
    return warmUpTimeout;
  }

  public TiConfiguration setWarmUpTimeout(long timeout) {
    this.warmUpTimeout = timeout;
    return this;
  }

  public long getPdFirstGetMemberTimeout() {
    return pdFirstGetMemberTimeout;
  }

  public void setPdFirstGetMemberTimeout(long pdFirstGetMemberTimeout) {
    this.pdFirstGetMemberTimeout = pdFirstGetMemberTimeout;
  }

  public long getScanTimeout() {
    return scanTimeout;
  }

  public TiConfiguration setScanTimeout(long scanTimeout) {
    this.scanTimeout = scanTimeout;
    return this;
  }

  public List<URI> getPdAddrs() {
    return pdAddrs;
  }

  public String getPdAddrsString() {
    return listToString(pdAddrs);
  }

  public int getScanBatchSize() {
    return DEF_SCAN_BATCH_SIZE;
  }

  public int getMaxFrameSize() {
    return maxFrameSize;
  }

  public TiConfiguration setMaxFrameSize(int maxFrameSize) {
    this.maxFrameSize = maxFrameSize;
    return this;
  }

  public int getIndexScanBatchSize() {
    return indexScanBatchSize;
  }

  public TiConfiguration setIndexScanBatchSize(int indexScanBatchSize) {
    this.indexScanBatchSize = indexScanBatchSize;
    return this;
  }

  public int getIndexScanConcurrency() {
    return indexScanConcurrency;
  }

  public TiConfiguration setIndexScanConcurrency(int indexScanConcurrency) {
    this.indexScanConcurrency = indexScanConcurrency;
    return this;
  }

  public int getTableScanConcurrency() {
    return tableScanConcurrency;
  }

  public TiConfiguration setTableScanConcurrency(int tableScanConcurrency) {
    this.tableScanConcurrency = tableScanConcurrency;
    return this;
  }

  public int getBatchGetConcurrency() {
    return batchGetConcurrency;
  }

  public TiConfiguration setBatchGetConcurrency(int batchGetConcurrency) {
    this.batchGetConcurrency = batchGetConcurrency;
    return this;
  }

  public int getBatchPutConcurrency() {
    return batchPutConcurrency;
  }

  public TiConfiguration setBatchPutConcurrency(int batchPutConcurrency) {
    this.batchPutConcurrency = batchPutConcurrency;
    return this;
  }

  public int getBatchDeleteConcurrency() {
    return batchDeleteConcurrency;
  }

  public TiConfiguration setBatchDeleteConcurrency(int batchDeleteConcurrency) {
    this.batchDeleteConcurrency = batchDeleteConcurrency;
    return this;
  }

  public int getBatchScanConcurrency() {
    return batchScanConcurrency;
  }

  public TiConfiguration setBatchScanConcurrency(int batchScanConcurrency) {
    this.batchScanConcurrency = batchScanConcurrency;
    return this;
  }

  public int getDeleteRangeConcurrency() {
    return deleteRangeConcurrency;
  }

  public TiConfiguration setDeleteRangeConcurrency(int deleteRangeConcurrency) {
    this.deleteRangeConcurrency = deleteRangeConcurrency;
    return this;
  }

  public CommandPri getCommandPriority() {
    return commandPriority;
  }

  public TiConfiguration setCommandPriority(CommandPri commandPriority) {
    this.commandPriority = commandPriority;
    return this;
  }

  public IsolationLevel getIsolationLevel() {
    return isolationLevel;
  }

  public TiConfiguration setIsolationLevel(IsolationLevel isolationLevel) {
    this.isolationLevel = isolationLevel;
    return this;
  }

  public boolean ifShowRowId() {
    return showRowId;
  }

  public TiConfiguration setShowRowId(boolean flag) {
    this.showRowId = flag;
    return this;
  }

  public String getDBPrefix() {
    return dbPrefix;
  }

  public TiConfiguration setDBPrefix(String dbPrefix) {
    this.dbPrefix = dbPrefix;
    return this;
  }

  public KVMode getKvMode() {
    return kvMode;
  }

  public boolean isRawKVMode() {
    return getKvMode() == TiConfiguration.KVMode.RAW;
  }

  public boolean isTxnKVMode() {
    return getKvMode() == KVMode.TXN;
  }

  public TiConfiguration setKvMode(String kvMode) {
    this.kvMode = KVMode.valueOf(kvMode);
    return this;
  }

  public int getKvClientConcurrency() {
    return kvClientConcurrency;
  }

  public TiConfiguration setKvClientConcurrency(int kvClientConcurrency) {
    this.kvClientConcurrency = kvClientConcurrency;
    return this;
  }

  public ReplicaRead getReplicaRead() {
    return replicaRead;
  }

  public TiConfiguration setReplicaRead(ReplicaRead replicaRead) {
    this.replicaRead = replicaRead;
    this.internalReplicaSelector = getReplicaSelector(this.replicaRead);
    return this;
  }

  private ReplicaSelector getReplicaSelector(ReplicaRead replicaRead) {
    if (TiConfiguration.ReplicaRead.LEADER.equals(replicaRead)) {
      return ReplicaSelector.LEADER;
    } else if (TiConfiguration.ReplicaRead.FOLLOWER.equals(replicaRead)) {
      return ReplicaSelector.FOLLOWER;
    } else if (TiConfiguration.ReplicaRead.LEADER_AND_FOLLOWER.equals(replicaRead)) {
      return ReplicaSelector.LEADER_AND_FOLLOWER;
    } else {
      return null;
    }
  }

  public ReplicaSelector getReplicaSelector() {
    if (replicaSelector != null) {
      return replicaSelector;
    } else {
      return internalReplicaSelector;
    }
  }

  public void setReplicaSelector(ReplicaSelector replicaSelector) {
    this.replicaSelector = replicaSelector;
  }

  public boolean isMetricsEnable() {
    return metricsEnable;
  }

  public TiConfiguration setMetricsEnable(boolean metricsEnable) {
    this.metricsEnable = metricsEnable;
    return this;
  }

  public int getMetricsPort() {
    return metricsPort;
  }

  public TiConfiguration setMetricsPort(int metricsPort) {
    this.metricsPort = metricsPort;
    return this;
  }

  public String getNetworkMappingName() {
    return this.networkMappingName;
  }

  public HostMapping getHostMapping() {
    return hostMapping;
  }

  public void setHostMapping(HostMapping mapping) {
    this.hostMapping = mapping;
  }

  public boolean getEnableGrpcForward() {
    return this.enableGrpcForward;
  }

  public void setEnableGrpcForward(boolean enableGrpcForward) {
    this.enableGrpcForward = enableGrpcForward;
  }

  public long getGrpcHealthCheckTimeout() {
    return this.grpcHealthCheckTimeout;
  }

  public void setGrpcHealthCheckTimeout(int grpcHealthCheckTimeout) {
    this.grpcHealthCheckTimeout = grpcHealthCheckTimeout;
  }

  public long getHealthCheckPeriodDuration() {
    return this.healthCheckPeriodDuration;
  }

  public void setHealthCheckPeriodDuration(int healthCheckPeriodDuration) {
    this.healthCheckPeriodDuration = healthCheckPeriodDuration;
  }

  public boolean isEnableAtomicForCAS() {
    return enableAtomicForCAS;
  }

  public void setEnableAtomicForCAS(boolean enableAtomicForCAS) {
    this.enableAtomicForCAS = enableAtomicForCAS;
  }

  public int getImporterMaxKVBatchBytes() {
    return importerMaxKVBatchBytes;
  }

  public void setImporterMaxKVBatchBytes(int importerMaxKVBatchBytes) {
    this.importerMaxKVBatchBytes = importerMaxKVBatchBytes;
  }

  public int getImporterMaxKVBatchSize() {
    return importerMaxKVBatchSize;
  }

  public void setImporterMaxKVBatchSize(int importerMaxKVBatchSize) {
    this.importerMaxKVBatchSize = importerMaxKVBatchSize;
  }

  public int getScatterWaitSeconds() {
    return scatterWaitSeconds;
  }

  public void setScatterWaitSeconds(int scatterWaitSeconds) {
    this.scatterWaitSeconds = scatterWaitSeconds;
  }

  public int getRawKVDefaultBackoffInMS() {
    return rawKVDefaultBackoffInMS;
  }

  public void setRawKVDefaultBackoffInMS(int rawKVDefaultBackoffInMS) {
    this.rawKVDefaultBackoffInMS = rawKVDefaultBackoffInMS;
  }

  public boolean isTest() {
    return isTest;
  }

  public void setTest(boolean test) {
    isTest = test;
  }

  public int getKeepaliveTime() {
    return keepaliveTime;
  }

  public void setKeepaliveTime(int keepaliveTime) {
    this.keepaliveTime = keepaliveTime;
  }

  public int getKeepaliveTimeout() {
    return keepaliveTimeout;
  }

  public void setKeepaliveTimeout(int timeout) {
    this.keepaliveTimeout = timeout;
  }

  public int getIdleTimeout() {
    return idleTimeout;
  }

  public void setIdleTimeout(int timeout) {
    this.idleTimeout = timeout;
  }

  public boolean isTiFlashEnabled() {
    return tiFlashEnable;
  }

  public boolean isWarmUpEnable() {
    return warmUpEnable;
  }

  public void setWarmUpEnable(boolean warmUpEnable) {
    this.warmUpEnable = warmUpEnable;
  }

  public boolean isTlsEnable() {
    return tlsEnable;
  }

  public void setTlsEnable(boolean tlsEnable) {
    this.tlsEnable = tlsEnable;
  }

  public String getTrustCertCollectionFile() {
    return trustCertCollectionFile;
  }

  public void setTrustCertCollectionFile(String trustCertCollectionFile) {
    this.trustCertCollectionFile = trustCertCollectionFile;
  }

  public String getKeyCertChainFile() {
    return keyCertChainFile;
  }

  public void setKeyCertChainFile(String keyCertChainFile) {
    this.keyCertChainFile = keyCertChainFile;
  }

  public String getKeyFile() {
    return keyFile;
  }

  public void setKeyFile(String keyFile) {
    this.keyFile = keyFile;
  }

  public boolean isJksEnable() {
    return useJks;
  }

  public void setJksEnable(boolean useJks) {
    this.useJks = useJks;
  }

  public String getJksKeyPath() {
    return jksKeyPath;
  }

  public void setJksKeyPath(String jksKeyPath) {
    this.jksKeyPath = jksKeyPath;
  }

  public String getJksKeyPassword() {
    return jksKeyPassword;
  }

  public void setJksKeyPassword(String jksKeyPassword) {
    this.jksKeyPassword = jksKeyPassword;
  }

  public String getJksTrustPath() {
    return jksTrustPath;
  }

  public void setJksTrustPath(String jksTrustPath) {
    this.jksTrustPath = jksTrustPath;
  }

  public String getJksTrustPassword() {
    return jksTrustPassword;
  }

  public void setJksTrustPassword(String jksTrustPassword) {
    this.jksTrustPassword = jksTrustPassword;
  }

  public int getRawKVReadTimeoutInMS() {
    return rawKVReadTimeoutInMS;
  }

  public void setRawKVReadTimeoutInMS(int rawKVReadTimeoutInMS) {
    this.rawKVReadTimeoutInMS = rawKVReadTimeoutInMS;
  }

  public int getRawKVWriteTimeoutInMS() {
    return rawKVWriteTimeoutInMS;
  }

  public void setRawKVWriteTimeoutInMS(int rawKVWriteTimeoutInMS) {
    this.rawKVWriteTimeoutInMS = rawKVWriteTimeoutInMS;
  }

  public int getRawKVBatchReadTimeoutInMS() {
    return rawKVBatchReadTimeoutInMS;
  }

  public void setRawKVBatchReadTimeoutInMS(int rawKVBatchReadTimeoutInMS) {
    this.rawKVBatchReadTimeoutInMS = rawKVBatchReadTimeoutInMS;
  }

  public int getRawKVBatchWriteTimeoutInMS() {
    return rawKVBatchWriteTimeoutInMS;
  }

  public void setRawKVBatchWriteTimeoutInMS(int rawKVBatchWriteTimeoutInMS) {
    this.rawKVBatchWriteTimeoutInMS = rawKVBatchWriteTimeoutInMS;
  }

  public int getRawKVScanTimeoutInMS() {
    return rawKVScanTimeoutInMS;
  }

  public void setRawKVScanTimeoutInMS(int rawKVScanTimeoutInMS) {
    this.rawKVScanTimeoutInMS = rawKVScanTimeoutInMS;
  }

  public int getRawKVCleanTimeoutInMS() {
    return rawKVCleanTimeoutInMS;
  }

  public void setRawKVCleanTimeoutInMS(int rawKVCleanTimeoutInMS) {
    this.rawKVCleanTimeoutInMS = rawKVCleanTimeoutInMS;
  }

  public Integer getRawKVReadSlowLogInMS() {
    return Optional.ofNullable(rawKVReadSlowLogInMS).orElse((int) (getTimeout() * 2));
  }

  public void setRawKVReadSlowLogInMS(Integer rawKVReadSlowLogInMS) {
    this.rawKVReadSlowLogInMS = rawKVReadSlowLogInMS;
  }

  public Integer getRawKVWriteSlowLogInMS() {
    return Optional.ofNullable(rawKVWriteSlowLogInMS).orElse((int) (getTimeout() * 2));
  }

  public void setRawKVWriteSlowLogInMS(Integer rawKVWriteSlowLogInMS) {
    this.rawKVWriteSlowLogInMS = rawKVWriteSlowLogInMS;
  }

  public Integer getRawKVBatchReadSlowLogInMS() {
    return Optional.ofNullable(rawKVBatchReadSlowLogInMS).orElse((int) (getTimeout() * 2));
  }

  public void setRawKVBatchReadSlowLogInMS(Integer rawKVBatchReadSlowLogInMS) {
    this.rawKVBatchReadSlowLogInMS = rawKVBatchReadSlowLogInMS;
  }

  public Integer getRawKVBatchWriteSlowLogInMS() {
    return Optional.ofNullable(rawKVBatchWriteSlowLogInMS).orElse((int) (getTimeout() * 2));
  }

  public void setRawKVBatchWriteSlowLogInMS(Integer rawKVBatchWriteSlowLogInMS) {
    this.rawKVBatchWriteSlowLogInMS = rawKVBatchWriteSlowLogInMS;
  }

  public int getRawKVScanSlowLogInMS() {
    return rawKVScanSlowLogInMS;
  }

  public void setRawKVScanSlowLogInMS(int rawKVScanSlowLogInMS) {
    this.rawKVScanSlowLogInMS = rawKVScanSlowLogInMS;
  }

  public double getRawKVServerSlowLogFactor() {
    return rawKVServerSlowLogFactor;
  }

  public void setRawKVServerSlowLogFactor(double rawKVServerSlowLogFactor) {
    if (rawKVServerSlowLogFactor < 0 || rawKVServerSlowLogFactor > 1) {
      throw new IllegalArgumentException("rawkv_server_slowlog_factor must be in range [0, 1]");
    }
    this.rawKVServerSlowLogFactor = rawKVServerSlowLogFactor;
  }

  public boolean isCircuitBreakEnable() {
    return circuitBreakEnable;
  }

  public void setCircuitBreakEnable(boolean circuitBreakEnable) {
    this.circuitBreakEnable = circuitBreakEnable;
  }

  public int getCircuitBreakAvailabilityWindowInSeconds() {
    return circuitBreakAvailabilityWindowInSeconds;
  }

  public void setCircuitBreakAvailabilityWindowInSeconds(
      int circuitBreakAvailabilityWindowInSeconds) {
    this.circuitBreakAvailabilityWindowInSeconds = circuitBreakAvailabilityWindowInSeconds;
  }

  public int getCircuitBreakAvailabilityErrorThresholdPercentage() {
    return circuitBreakAvailabilityErrorThresholdPercentage;
  }

  public void setCircuitBreakAvailabilityErrorThresholdPercentage(
      int circuitBreakAvailabilityErrorThresholdPercentage) {
    this.circuitBreakAvailabilityErrorThresholdPercentage =
        circuitBreakAvailabilityErrorThresholdPercentage;
  }

  public int getCircuitBreakAvailabilityRequestVolumnThreshold() {
    return circuitBreakAvailabilityRequestVolumnThreshold;
  }

  public void setCircuitBreakAvailabilityRequestVolumnThreshold(
      int circuitBreakAvailabilityRequestVolumnThreshold) {
    this.circuitBreakAvailabilityRequestVolumnThreshold =
        circuitBreakAvailabilityRequestVolumnThreshold;
  }

  public int getCircuitBreakSleepWindowInSeconds() {
    return circuitBreakSleepWindowInSeconds;
  }

  public void setCircuitBreakSleepWindowInSeconds(int circuitBreakSleepWindowInSeconds) {
    this.circuitBreakSleepWindowInSeconds = circuitBreakSleepWindowInSeconds;
  }

  public int getCircuitBreakAttemptRequestCount() {
    return circuitBreakAttemptRequestCount;
  }

  public void setCircuitBreakAttemptRequestCount(int circuitBreakAttemptRequestCount) {
    this.circuitBreakAttemptRequestCount = circuitBreakAttemptRequestCount;
  }

  public int getScanRegionsLimit() {
    return scanRegionsLimit;
  }

  public void setScanRegionsLimit(int scanRegionsLimit) {
    this.scanRegionsLimit = scanRegionsLimit;
  }

  public ApiVersion getApiVersion() {
    return apiVersion;
  }

  public TiConfiguration setApiVersion(ApiVersion version) {
    this.apiVersion = version;
    return this;
  }
}
