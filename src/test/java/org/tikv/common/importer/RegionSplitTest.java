package org.tikv.common.importer;

import static org.junit.Assert.assertArrayEquals;
import static org.tikv.util.TestUtils.genRandomKey;

import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.tikv.BaseRawKVTest;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.common.region.TiRegion;

public class RegionSplitTest extends BaseRawKVTest {
  private TiSession session;

  private static final int KEY_NUMBER = 10;
  private static final String KEY_PREFIX = "prefix_region_split_test_";
  private static final int KEY_LENGTH = KEY_PREFIX.length() + 10;

  @Before
  public void setup() {
    TiConfiguration conf = createTiConfiguration();
    session = TiSession.create(conf);
  }

  @After
  public void tearDown() throws Exception {
    if (session != null) {
      session.close();
    }
  }

  @Test
  public void rawKVSplitTest() {
    List<byte[]> splitKeys = new ArrayList<>(KEY_NUMBER);
    for (int i = 0; i < KEY_NUMBER; i++) {
      splitKeys.add(genRandomKey(KEY_PREFIX, KEY_LENGTH));
    }

    session.splitRegionAndScatter(splitKeys);
    session.getRegionManager().invalidateAll();

    for (int i = 0; i < KEY_NUMBER; i++) {
      byte[] key = splitKeys.get(i);
      TiRegion region = session.getRegionManager().getRegionByKey(ByteString.copyFrom(key));
      assertArrayEquals(key, region.getStartKey().toByteArray());
    }
  }
}
