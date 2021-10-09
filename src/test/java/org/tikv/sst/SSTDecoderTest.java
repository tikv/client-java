package org.tikv.sst;

import com.google.protobuf.ByteString;
import java.util.Iterator;
import org.junit.Assert;
import org.junit.Test;
import org.rocksdb.RocksDBException;
import org.tikv.BaseRawKVTest;
import org.tikv.common.util.Pair;

public class SSTDecoderTest extends BaseRawKVTest {

  private static final int TOTAL_COUNT = 500;
  private static final String KEY_PREFIX = "test_";
  private static final String VALUE =
      "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

  @Test
  public void rawKVSSTDecoderTest() throws RocksDBException {
    SSTDecoder sstDecoder = new SSTDecoder("src/test/resources/rawkv.sst");
    Iterator<Pair<ByteString, ByteString>> iterator = sstDecoder.getIterator();
    int count = 0;
    while (iterator.hasNext()) {
      Pair<ByteString, ByteString> pair = iterator.next();
      Assert.assertEquals(VALUE, pair.second.toStringUtf8());
      Assert.assertTrue(pair.first.toStringUtf8().startsWith(KEY_PREFIX));
      count += 1;
    }
    sstDecoder.close();
    Assert.assertEquals(TOTAL_COUNT, count);
  }
}
