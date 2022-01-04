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

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import slowlog.SlowLog;

public class SlowLogTest {

  @Test
  public void testTimelines() throws IOException {
    String[] logs = {
        "function: putIfAbsent\n"
            + "start: 22:50:36.364\n"
            + "end: 22:50:37.232\n"
            + "region: {Region[111] ConfVer[33] Version[5] Store[13] KeyRange[key0]:[key1]}\n"
            + "key: key0\n"
            + "================================================================================  total 868ms\n"
            + "================================================================================  868ms callWithRetry tikvpb.Tikv/RawCompareAndSwap\n"
            + "=============                                                                     150ms gRPC tikvpb.Tikv/RawCompareAndSwap\n"
            + "                                                                                  6ms backoff BoTiKVRPC\n"
            + "              =============                                                       151ms gRPC tikvpb.Tikv/RawCompareAndSwap\n"
            + "                            =                                                     16ms backoff BoTiKVRPC\n"
            + "                             =============                                        150ms gRPC tikvpb.Tikv/RawCompareAndSwap\n"
            + "                                           ==                                     26ms backoff BoTiKVRPC\n"
            + "                                             =============                        150ms gRPC tikvpb.Tikv/RawCompareAndSwap\n"
            + "                                                           ======                 68ms backoff BoTiKVRPC\n"
            + "                                                                  =============   150ms gRPC tikvpb.Tikv/RawCompareAndSwap\n"
            + "                                                                                  0ms backoff BoTiKVRPC\n"
            + "                                                                                  0ms backoff BoRegionMiss\n",
        "function: putIfAbsent\n"
            + "start: 22:50:36.945\n"
            + "end: 22:50:37.570\n"
            + "region: {Region[111] ConfVer[33] Version[5] Store[13] KeyRange[key1]:[key2]}\n"
            + "key: key1\n"
            + "================================================================================  total 625ms\n"
            + "===============================================================================   624ms callWithRetry tikvpb.Tikv/RawCompareAndSwap\n"
            + "===================                                                               150ms gRPC tikvpb.Tikv/RawCompareAndSwap\n"
            + "                   =                                                              8ms backoff BoTiKVRPC\n"
            + "                    ===================                                           150ms gRPC tikvpb.Tikv/RawCompareAndSwap\n"
            + "                                       =                                          10ms backoff BoTiKVRPC\n"
            + "                                        ===================                       150ms gRPC tikvpb.Tikv/RawCompareAndSwap\n"
            + "                                                                                  4ms seekLeaderStore\n"
            + "                                                                                  2ms seekProxyStore\n"
            + "                                                            ===                   28ms backoff BoTiKVRPC\n"
            + "                                                                ===============   121ms gRPC tikvpb.Tikv/RawCompareAndSwap\n"
    };

    List<SlowLog> actualLogs = new LogParser("src/test/resources/example.log").parse();
    for (int i = 0; i < actualLogs.size(); i++) {
      String timelines = actualLogs.get(i).getTimelines(80, Duration.ofMillis(10));
      timelines = timelines.replaceAll("\u001B\\[[;\\d]*m", "");
      Assertions.assertEquals(logs[i], timelines);
    }
  }
}
