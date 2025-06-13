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

package org.tikv.common.meta;

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.pingcap.tidb.tipb.TableInfo;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.tikv.common.exception.TiClientInternalException;
import org.tikv.common.meta.TiColumnInfo.InternalTypeHolder;
import org.tikv.common.types.DataType;
import org.tikv.common.types.DataTypeFactory;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TiTableInfo implements Serializable {
  private final long id;
  private final String name;
  private final String charset;
  private final String collate;
  private final List<org.tikv.common.meta.TiColumnInfo> columns;
  private final Map<String, org.tikv.common.meta.TiColumnInfo> columnsMap;
  private final List<org.tikv.common.meta.TiIndexInfo> indices;
  private final boolean pkIsHandle;
  private final boolean isCommonHandle;
  // CommonHandleVersion is the version of the clustered index.
  // 0 for the clustered index created == 5.0.0 RC.
  // 1 for the clustered index created > 5.0.0 RC.
  private final int commonHandleVersion;
  private final String comment;
  private final long autoIncId;
  private final long maxColumnId;
  private final long maxIndexId;
  private final long oldSchemaId;
  private final long rowSize; // estimated row size
  private final org.tikv.common.meta.TiPartitionInfo partitionInfo;
  private final org.tikv.common.meta.TiColumnInfo primaryKeyColumn;
  private final TiViewInfo viewInfo;
  private final TiFlashReplicaInfo tiflashReplicaInfo;
  private final long version;
  private final long updateTimestamp;
  private final long maxShardRowIDBits;
  private final org.tikv.common.meta.TiSequenceInfo sequenceInfo;
  private final long autoRandomBits;

  /** without hiden column */
  private final List<TiColumnInfo> columnsWithoutHidden;

  private final List<TiIndexInfo> indicesWithoutHiddenAndInvisible;

  @JsonCreator
  @JsonIgnoreProperties(ignoreUnknown = true)
  public TiTableInfo(
      @JsonProperty("id") long id,
      @JsonProperty("name") org.tikv.common.meta.CIStr name,
      @JsonProperty("charset") String charset,
      @JsonProperty("collate") String collate,
      @JsonProperty("pk_is_handle") boolean pkIsHandle,
      @JsonProperty("is_common_handle") boolean isCommonHandle,
      @JsonProperty("common_handle_version") int commonHandleVersion,
      @JsonProperty("cols") List<org.tikv.common.meta.TiColumnInfo> columns,
      @JsonProperty("index_info") List<org.tikv.common.meta.TiIndexInfo> indices,
      @JsonProperty("comment") String comment,
      @JsonProperty("auto_inc_id") long autoIncId,
      @JsonProperty("max_col_id") long maxColumnId,
      @JsonProperty("max_idx_id") long maxIndexId,
      @JsonProperty("old_schema_id") long oldSchemaId,
      @JsonProperty("partition") org.tikv.common.meta.TiPartitionInfo partitionInfo,
      @JsonProperty("view") TiViewInfo viewInfo,
      @JsonProperty("tiflash_replica") TiFlashReplicaInfo tiFlashReplicaInfo,
      @JsonProperty("version") long version,
      @JsonProperty("update_timestamp") long updateTimestamp,
      @JsonProperty("max_shard_row_id_bits") long maxShardRowIDBits,
      @JsonProperty("sequence") org.tikv.common.meta.TiSequenceInfo sequenceInfo,
      @JsonProperty("auto_random_bits") long autoRandomBits) {
    this.id = id;
    this.name = name.getL();
    this.charset = charset;
    this.collate = collate;
    if (sequenceInfo == null) {
      this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
      this.columnsWithoutHidden =
          columns.stream().filter(col -> !col.isHidden()).collect(Collectors.toList());
      this.columnsMap = new HashMap<>();
      for (TiColumnInfo col : this.columns) {
        this.columnsMap.put(col.getName(), col);
      }
      this.rowSize = columns.stream().mapToLong(TiColumnInfo::getSize).sum();
    } else {
      this.columns = null;
      this.columnsWithoutHidden = null;
      this.columnsMap = null;
      // 9 is the rowSize for type bigint
      this.rowSize = 9;
    }
    // TODO: Use more precise predication according to types
    this.pkIsHandle = pkIsHandle;
    this.isCommonHandle = isCommonHandle;
    this.commonHandleVersion = commonHandleVersion;
    this.indices = indices != null ? ImmutableList.copyOf(indices) : ImmutableList.of();
    this.indicesWithoutHiddenAndInvisible =
        this.indices
            .stream()
            .filter(
                idx -> {
                  if (idx.isInvisible()) {
                    return false;
                  }
                  for (TiIndexColumn idc : idx.getIndexColumns()) {
                    if (getColumn(idc.getName()).isHidden()) {
                      return false;
                    }
                  }
                  return true;
                })
            .collect(Collectors.toList());
    if (this.columns != null) {
      this.indices.forEach(x -> x.calculateIndexSize(columns));
    }
    this.comment = comment;
    this.autoIncId = autoIncId;
    this.maxColumnId = maxColumnId;
    this.maxIndexId = maxIndexId;
    this.oldSchemaId = oldSchemaId;
    this.partitionInfo = partitionInfo;
    this.viewInfo = viewInfo;
    this.tiflashReplicaInfo = tiFlashReplicaInfo;
    this.version = version;
    this.updateTimestamp = updateTimestamp;
    this.maxShardRowIDBits = maxShardRowIDBits;
    this.sequenceInfo = sequenceInfo;
    this.autoRandomBits = autoRandomBits;

    TiColumnInfo primaryKey = null;
    if (sequenceInfo == null) {
      for (TiColumnInfo col : this.columns) {
        if (col.isPrimaryKey()) {
          primaryKey = col;
          break;
        }
      }
    }
    primaryKeyColumn = primaryKey;
  }

  public boolean isView() {
    return this.viewInfo != null;
  }

  public boolean isSequence() {
    return this.sequenceInfo != null;
  }

  // auto increment column must be a primary key column
  public boolean hasAutoIncrementColumn() {
    if (primaryKeyColumn != null) {
      return primaryKeyColumn.isAutoIncrement();
    }
    return false;
  }

  // auto increment column must be a primary key column
  public org.tikv.common.meta.TiColumnInfo getAutoIncrementColInfo() {
    if (hasAutoIncrementColumn()) {
      return primaryKeyColumn;
    }
    return null;
  }

  public boolean isAutoIncColUnsigned() {
    org.tikv.common.meta.TiColumnInfo col = getAutoIncrementColInfo();
    if (col == null) return false;
    return col.getType().isUnsigned();
  }

  public long getMaxShardRowIDBits() {
    return this.maxShardRowIDBits;
  }

  public long getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  public String getCharset() {
    return charset;
  }

  public String getCollate() {
    return collate;
  }

  public List<org.tikv.common.meta.TiColumnInfo> getColumns() {
    return columns;
  }

  public long getEstimatedRowSizeInByte() {
    return rowSize;
  }

  public org.tikv.common.meta.TiColumnInfo getColumn(String name) {
    return this.columnsMap.get(name.toLowerCase());
  }

  public org.tikv.common.meta.TiColumnInfo getColumn(int offset) {
    if (offset < 0 || offset >= columns.size()) {
      throw new TiClientInternalException(String.format("Column offset %d out of bound", offset));
    }
    return columns.get(offset);
  }

  public boolean isPkHandle() {
    return pkIsHandle;
  }

  public List<org.tikv.common.meta.TiIndexInfo> getIndices() {
    return getIndices(false);
  }

  public List<TiIndexInfo> getIndices(boolean includingAll) {
    if (includingAll) {
      return this.indices;
    } else {
      return this.indicesWithoutHiddenAndInvisible;
    }
  }

  public String getComment() {
    return comment;
  }

  private long getAutoIncId() {
    return autoIncId;
  }

  private long getMaxColumnId() {
    return maxColumnId;
  }

  private long getMaxIndexId() {
    return maxIndexId;
  }

  private long getOldSchemaId() {
    return oldSchemaId;
  }

  public boolean isCommonHandle() {
    return isCommonHandle;
  }

  public org.tikv.common.meta.TiPartitionInfo getPartitionInfo() {
    return partitionInfo;
  }

  public TiFlashReplicaInfo getTiflashReplicaInfo() {
    return tiflashReplicaInfo;
  }

  TableInfo toProto() {
    return TableInfo.newBuilder()
        .setTableId(getId())
        .addAllColumns(
            getColumns().stream().map(col -> col.toProto(this)).collect(Collectors.toList()))
        .build();
  }

  public boolean hasPrimaryKey() {
    return primaryKeyColumn != null;
  }

  public TiIndexInfo getPrimaryKey() {
    for (TiIndexInfo index : getIndices()) {
      if (index.isPrimary()) {
        return index;
      }
    }
    return null;
  }

  // Only Integer Column will be a PK column
  // and there exists only one PK column
  public org.tikv.common.meta.TiColumnInfo getPKIsHandleColumn() {
    if (isPkHandle()) {
      for (org.tikv.common.meta.TiColumnInfo col : getColumns()) {
        if (col.isPrimaryKey()) {
          return col;
        }
      }
    }
    return null;
  }

  private org.tikv.common.meta.TiColumnInfo copyColumn(org.tikv.common.meta.TiColumnInfo col) {
    DataType type = col.getType();
    InternalTypeHolder typeHolder = type.toTypeHolder();
    typeHolder.setFlag(type.getFlag() & (~DataType.PriKeyFlag));
    DataType newType = DataTypeFactory.of(typeHolder);
    return new org.tikv.common.meta.TiColumnInfo(
            col.getId(),
            col.getName(),
            col.getOffset(),
            newType,
            col.getSchemaState(),
            col.getOriginDefaultValue(),
            col.getDefaultValue(),
            col.getDefaultValueBit(),
            col.getComment(),
            col.getVersion(),
            col.getGeneratedExprString(),
            col.isHidden())
        .copyWithoutPrimaryKey();
  }

  public TiTableInfo copyTableWithRowId() {
    if (!isPkHandle()) {
      ImmutableList.Builder<org.tikv.common.meta.TiColumnInfo> newColumns = ImmutableList.builder();
      for (org.tikv.common.meta.TiColumnInfo col : getColumns()) {
        newColumns.add(copyColumn(col));
      }
      newColumns.add(org.tikv.common.meta.TiColumnInfo.getRowIdColumn(getColumns().size()));
      return new TiTableInfo(
          getId(),
          org.tikv.common.meta.CIStr.newCIStr(getName()),
          getCharset(),
          getCollate(),
          true,
          isCommonHandle,
          commonHandleVersion,
          newColumns.build(),
          getIndices(),
          getComment(),
          getAutoIncId(),
          getMaxColumnId(),
          getMaxIndexId(),
          getOldSchemaId(),
          partitionInfo,
          null,
          getTiflashReplicaInfo(),
          version,
          updateTimestamp,
          maxShardRowIDBits,
          null,
          autoRandomBits);
    } else {
      return this;
    }
  }

  @Override
  public String toString() {
    return toProto().toString();
  }

  public boolean isPartitionEnabled() {
    if (partitionInfo == null) return false;
    return partitionInfo.isEnable();
  }

  public boolean hasGeneratedColumn() {
    for (org.tikv.common.meta.TiColumnInfo col : getColumns()) {
      if (col.isGeneratedColumn()) {
        return true;
      }
    }
    return false;
  }

  public long getVersion() {
    return version;
  }

  public long getUpdateTimestamp() {
    return updateTimestamp;
  }
}
