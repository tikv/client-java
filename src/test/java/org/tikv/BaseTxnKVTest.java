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

package org.tikv;

import org.tikv.common.StoreApiVersion;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiConfiguration.ApiVersion;
import org.tikv.common.TiSession;
import org.tikv.util.TestUtils;

public class BaseTxnKVTest {
  protected TiConfiguration createTiConfiguration() {
    String pdAddrsStr = TestUtils.getEnv("TXNKV_PD_ADDRESSES");

    TiConfiguration conf =
        pdAddrsStr == null
            ? TiConfiguration.createDefault()
            : TiConfiguration.createDefault(pdAddrsStr);
    conf.setTest(true);
    conf.setEnableGrpcForward(false);

    try (TiSession session = TiSession.create(conf)) {
      int version = StoreApiVersion.acquire(session.getPDClient()).get();
      conf.setApiVersion(ApiVersion.fromInt(version));
    } catch (Exception ignore) {
    }
    return conf;
  }
}
