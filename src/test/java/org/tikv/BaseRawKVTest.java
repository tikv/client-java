package org.tikv;

import org.tikv.common.TiConfiguration;
import org.tikv.util.TestUtils;

public class BaseRawKVTest {
  protected TiConfiguration createTiConfiguration() {
    String pdAddrsStr = TestUtils.getEnv("RAWKV_PD_ADDRESSES");

    TiConfiguration conf =
        pdAddrsStr == null
            ? TiConfiguration.createRawDefault()
            : TiConfiguration.createRawDefault(pdAddrsStr);
    conf.setEnableGrpcForward(false);
    return conf;
  }
}
