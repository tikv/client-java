package org.tikv.cdc;

import org.tikv.kvproto.Kvrpcpb;

public class CDCConfig {
  private static final int EVENT_BUFFER_SIZE = 50000;
  private static final int MAX_ROW_KEY_SIZE = 10240;
  private static final boolean READ_OLD_VALUE = true;

  private int eventBufferSize = EVENT_BUFFER_SIZE;
  private int maxRowKeySize = MAX_ROW_KEY_SIZE;
  private boolean readOldValue = READ_OLD_VALUE;

  public void setEventBufferSize(final int bufferSize) {
    eventBufferSize = bufferSize;
  }

  public void setMaxRowKeySize(final int rowKeySize) {
    maxRowKeySize = rowKeySize;
  }

  public void setReadOldValue(final boolean value) {
    readOldValue = value;
  }

  public int getEventBufferSize() {
    return eventBufferSize;
  }

  public int getMaxRowKeySize() {
    return maxRowKeySize;
  }

  public boolean getReadOldValue() {
    return readOldValue;
  }

  Kvrpcpb.ExtraOp getExtraOp() {
    return readOldValue ? Kvrpcpb.ExtraOp.ReadOldValue : Kvrpcpb.ExtraOp.Noop;
  }
}
