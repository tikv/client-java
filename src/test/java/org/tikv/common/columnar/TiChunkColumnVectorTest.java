package org.tikv.common.columnar;

import static org.tikv.common.types.MySQLType.TypeBit;

import java.nio.ByteBuffer;
import junit.framework.TestCase;
import org.junit.Assert;
import org.junit.Test;
import org.tikv.common.types.BitType;
import org.tikv.common.types.DataType;
import org.tikv.common.types.MySQLType;

public class TiChunkColumnVectorTest extends TestCase {

  @Test
  public void testGetLong() {
    long expect = 32767;
    ByteBuffer buffer = ByteBuffer.allocate(8);
    buffer.putLong(expect);
    TiChunkColumnVector tiChunkColumnVector = new TiChunkColumnVector(
        BitType.BIT, -1, 1, 0, new byte[]{-1}, new long[]{0, 8}, buffer);
    Assert.assertEquals(expect, tiChunkColumnVector.getLong(0));
  }
}
