package org.tikv;

import org.tikv.common.TiConfiguration;
import org.tikv.util.TestUtils;

public class BaseTxnKVTest {
  protected TiConfiguration createTiConfiguration() {
    String pdAddrsStr = TestUtils.getEnv("TXNKV_PD_ADDRESSES");

    TiConfiguration conf =
        pdAddrsStr == null
            ? TiConfiguration.createDefault()
            : TiConfiguration.createDefault(pdAddrsStr);
    conf.setEnableGrpcForward(false);
    return conf;
  }
}
