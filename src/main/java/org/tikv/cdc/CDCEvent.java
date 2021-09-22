package org.tikv.cdc;

import org.tikv.kvproto.Cdcpb.Event.Row;

class CDCEvent {
  enum CDCEventType {
    ROW,
    RESOLVED_TS,
    ERROR
  }

  public final long regionId;

  public final CDCEventType eventType;

  public final long resolvedTs;

  public final Row row;

  public final Throwable error;

  private CDCEvent(
      final long regionId,
      final CDCEventType eventType,
      final long resolvedTs,
      final Row row,
      final Throwable error) {
    this.regionId = regionId;
    this.eventType = eventType;
    this.resolvedTs = resolvedTs;
    this.row = row;
    this.error = error;
  }

  public static CDCEvent rowEvent(final long regionId, final Row row) {
    return new CDCEvent(regionId, CDCEventType.ROW, 0, row, null);
  }

  public static CDCEvent resolvedTsEvent(final long regionId, final long resolvedTs) {
    return new CDCEvent(regionId, CDCEventType.RESOLVED_TS, resolvedTs, null, null);
  }

  public static CDCEvent error(final long regionId, final Throwable error) {
    return new CDCEvent(regionId, CDCEventType.ERROR, 0, null, error);
  }

  @Override
  public String toString() {
    final StringBuilder builder = new StringBuilder();
    builder.append("CDCEvent[").append(eventType.toString()).append("] {");
    switch (eventType) {
      case ERROR:
        builder.append("error=").append(error.getMessage());
        break;
      case RESOLVED_TS:
        builder.append("resolvedTs=").append(resolvedTs);
        break;
      case ROW:
        builder.append("row=").append(row);
        break;
    }
    return builder.append("}").toString();
  }
}
