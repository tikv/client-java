/*
 * Copyright 2021 PingCAP, Inc.
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

package org.tikv.common;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.tikv.kvproto.Metapb;

public class InternalReplicaSelector implements ReplicaSelector {
  private final TiConfiguration.ReplicaRead replicaRead;

  public InternalReplicaSelector(TiConfiguration.ReplicaRead replicaRead) {
    this.replicaRead = replicaRead;
  }

  @Override
  public List<Metapb.Peer> select(
      Metapb.Peer leader, List<Metapb.Peer> followers, List<Metapb.Peer> learners) {
    List<Metapb.Peer> list = new ArrayList<>();

    if (TiConfiguration.ReplicaRead.LEADER.equals(replicaRead)) {
      list.add(leader);
    } else if (TiConfiguration.ReplicaRead.FOLLOWER.equals(replicaRead)) {
      list.addAll(followers);
      Collections.shuffle(list);
    } else if (TiConfiguration.ReplicaRead.LEADER_AND_FOLLOWER.equals(replicaRead)) {
      list.addAll(followers);
      Collections.shuffle(list);
      list.add(leader);
    }

    return list;
  }
}
