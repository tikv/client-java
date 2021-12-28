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
        "function: get\n"
            + "start: 21:10:45.398\n"
            + "end: 21:10:46.582\n"
            + "region: {Region[33] ConfVer[45] Version[2] Store[6] KeyRange[]:[indexInfo_:_pf01_:_APD01_:_2102100000278630073]}\n"
            + "key: test.state990\n"
            + "================================================================================  total 1184ms\n"
            + "       =====                                                                      77ms callWithRetry pdpb.PD/GetRegion\n"
            + "       =====                                                                      77ms gRPC pdpb.PD/GetRegion\n"
            + "               =======                                                            106ms callWithRetry pdpb.PD/GetStore\n"
            + "               =======                                                            106ms gRPC pdpb.PD/GetStore\n"
            + "                      =                                                           28ms callWithRetry pdpb.PD/GetStore\n"
            + "                      =                                                           28ms gRPC pdpb.PD/GetStore\n"
            + "                                                                                  12ms callWithRetry pdpb.PD/GetStore\n"
            + "                                                                                  12ms gRPC pdpb.PD/GetStore\n"
            + "                         ==                                                       35ms callWithRetry pdpb.PD/GetStore\n"
            + "                         ==                                                       35ms gRPC pdpb.PD/GetStore\n"
            + "                           =                                                      15ms callWithRetry pdpb.PD/GetStore\n"
            + "                           =                                                      15ms gRPC pdpb.PD/GetStore\n"
            + "                            =                                                     15ms callWithRetry pdpb.PD/GetStore\n"
            + "                            =                                                     15ms gRPC pdpb.PD/GetStore\n"
            + "                                    ===========================================   637ms callWithRetry tikvpb.Tikv/RawGet\n"
            + "                                    ===========                                   168ms gRPC tikvpb.Tikv/RawGet\n"
            + "                                                =======================           350ms seekLeaderStore\n"
            + "                                                                       =======    114ms seekProxyStore\n"
            + "                                                                                  0ms backoff BoTiKVRPC\n"
            + "                                                                                  0ms backoff BoRegionMiss\n"

        , "function: putIfAbsent\n"
        + "start: 22:50:36.364\n"
        + "end: 22:50:37.232\n"
        + "region: {Region[111] ConfVer[33] Version[5] Store[13] KeyRange[tempIndex_:_pf01_:_0248080000500000513970094]:[tempIndex_:_pf01_:_0257170000100000107840020]}\n"
        + "key: tempIndex_:_pf01_:_0255150001400001435640009\n"
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
        + "                                                                                  0ms backoff BoRegionMiss\n"
    };

    List<SlowLog> actualLogs = new LogParser("src/test/resources/example.log").parse();
    for (int i = 0; i < actualLogs.size(); i++) {
      String timelines = actualLogs.get(i).getTimelines(80, Duration.ofMillis(10));
      timelines = timelines.replaceAll("\u001B\\[[;\\d]*m", "");
      Assertions.assertEquals(logs[i], timelines);
    }
  }
}
