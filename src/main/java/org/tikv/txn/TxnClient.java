/*
 * Copyright 2020 PingCAP, Inc.
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

package org.tikv.txn;

import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.*;
import org.tikv.common.exception.TiBatchWriteException;
import org.tikv.common.exception.TiKVException;
import org.tikv.common.util.BackOffer;
import org.tikv.common.util.ConcreteBackOffer;

public class TxnClient implements AutoCloseable {
  public TiSession session;
  public TiConfiguration conf;
  public PDClient pdClient;

  private TTLManager ttlManager;

  private static final int PREWRITE_BACKOFFER_MS = 240000;
  private static final int COMMIT_BACKOFFER_MS = 20000;
  private static final int MIN_DELAY_CLEAN_TABLE_LOCK = 60000;
  private static final int DELAY_CLEAN_TABLE_LOCK_AND_COMMIT_BACKOFF_DELTA = 30000;
  private static final int PRIMARY_KEY_COMMIT_BACKOFF =
      MIN_DELAY_CLEAN_TABLE_LOCK - DELAY_CLEAN_TABLE_LOCK_AND_COMMIT_BACKOFF_DELTA;

  private static final Logger logger = LoggerFactory.getLogger(TxnClient.class);

  public TxnClient(TiSession session) {
    this.session = session;
    this.conf = session.getConf();
    this.pdClient = session.getPDClient();
  }

  /**
   * Write keys and values in a transaction, if an error occurs,
   * the whole transaction would fail.
   *
   * @param keys list of keys
   * @param values list of values
   */
  public void transaction(List<ByteString> keys, List<ByteString> values) {
    transaction(keys, values, false);
  }

  public void transaction(
      List<ByteString> keys, List<ByteString> values, boolean skipCommitSecondaryKeys) {
    if (keys.size() != values.size()) {
      throw new TiKVException("Keys and values size do not match");
    }
    if (keys.isEmpty()) {
      logger.warn("Empty transaction");
      return;
    }
    byte[] primaryKey = keys.get(0).toByteArray(), primaryValue = values.get(0).toByteArray();
    List<ByteWrapper> secondaryKeys = new ArrayList<>();
    List<BytePairWrapper> secondaryKVs = new ArrayList<>();
    for (int i = 1; i < keys.size(); i++) {
      secondaryKeys.add(new ByteWrapper(keys.get(i).toByteArray()));
      secondaryKVs.add(new BytePairWrapper(keys.get(i).toByteArray(), values.get(i).toByteArray()));
    }
    Iterator<ByteWrapper> keyIterator = secondaryKeys.iterator();
    Iterator<BytePairWrapper> kvIterator = secondaryKVs.iterator();

    long startTs = session.getTimestamp().getVersion();
    TwoPhaseCommitter ti2PCClient = new TwoPhaseCommitter(conf, startTs);

    BackOffer prewritePrimaryBackoff = ConcreteBackOffer.newCustomBackOff(PREWRITE_BACKOFFER_MS);
    logger.info("start to prewritePrimaryKey");
    // pre-write primary keys
    ti2PCClient.prewritePrimaryKey(prewritePrimaryBackoff, primaryKey, primaryValue);
    logger.info("prewritePrimaryKey success");

    boolean isTTLUpdate = StoreVersion.minTiKVVersion("3.0.5", session.getPDClient());

    // start primary key ttl update
    if (isTTLUpdate) {
      ttlManager = new TTLManager(conf, startTs, primaryKey);
      ttlManager.keepAlive();
    }

    logger.info("start to prewriteSecondaryKeys");
    // pre-write secondary keys
    ti2PCClient.prewriteSecondaryKeys(primaryKey, kvIterator, PREWRITE_BACKOFFER_MS);
    logger.info("prewriteSecondaryKeys success");

    long commitTs = session.getTimestamp().getVersion();
    // check commitTS
    if (commitTs <= startTs) {
      throw new TiBatchWriteException(
          "invalid transaction tso with startTs=" + startTs + ", commitTs=" + commitTs);
    }
    BackOffer commitPrimaryBackoff = ConcreteBackOffer.newCustomBackOff(PRIMARY_KEY_COMMIT_BACKOFF);
    logger.info("start to commitPrimaryKey");
    // commit primary keys
    ti2PCClient.commitPrimaryKey(commitPrimaryBackoff, primaryKey, commitTs);
    logger.info("commitPrimaryKey success");

    // stop primary key ttl update
    if (isTTLUpdate) {
      try {
        ttlManager.close();
      } catch (Exception ignore) {
        // ignore
      }
    }

    if (!skipCommitSecondaryKeys) {
      try {
        logger.info("start to commitSecondaryKeys");
        ti2PCClient.commitSecondaryKeys(keyIterator, commitTs, COMMIT_BACKOFFER_MS);
        logger.info("commitSecondaryKeys success");
      } catch (TiBatchWriteException e) {
        // ignored
        logger.warn("commit secondary key error", e);
      }
    }
  }

  @Override
  public void close() {}
}
