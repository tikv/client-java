/*
 * Copyright 2020 TiKV Project Authors.
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

package org.tikv.common.meta;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class TiViewInfo implements Serializable {
  // ViewAlgorithm is VIEW's SQL ALGORITHM characteristic.
  // See https://dev.mysql.com/doc/refman/5.7/en/view-algorithms.html
  private final long viewAlgorithm;
  private final org.tikv.common.meta.TiUserIdentity userIdentity;
  // ViewSecurity is VIEW's SQL SECURITY characteristic.
  // See https://dev.mysql.com/doc/refman/5.7/en/create-view.html
  private final long viewSecurity;
  private final String viewSelect;
  // ViewCheckOption is VIEW's WITH CHECK OPTION clause part.
  // See https://dev.mysql.com/doc/refman/5.7/en/view-check-option.html
  private final long viewCheckOpt;
  private final List<String> viewCols;

  @JsonCreator
  public TiViewInfo(
      @JsonProperty("view_algorithm") long viewAlgorithm,
      @JsonProperty("view_definer") org.tikv.common.meta.TiUserIdentity userIdentity,
      @JsonProperty("view_security") long viewSecurity,
      @JsonProperty("view_select") String viewSelect,
      @JsonProperty("view_checkoption") long viewCheckOpt,
      @JsonProperty("view_cols") List<org.tikv.common.meta.CIStr> viewCols) {
    this.viewAlgorithm = viewAlgorithm;
    this.userIdentity = userIdentity;
    this.viewSecurity = viewSecurity;
    this.viewSelect = viewSelect;
    this.viewCheckOpt = viewCheckOpt;
    if (viewCols != null) {
      this.viewCols =
          viewCols.stream().map(org.tikv.common.meta.CIStr::getO).collect(Collectors.toList());
    } else {
      this.viewCols = new ArrayList<>();
    }
  }
}
