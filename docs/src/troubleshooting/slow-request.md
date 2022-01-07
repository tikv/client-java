# Slow Request Diagnosis

If a request take too much time, we can collect the detailed time spend in each component in a “slow log”.

```
22/01/07 16:16:07 WARN log.SlowLogImpl: SlowLog:{"start":"16:16:07.613","end":"16:16:07.618","duration":"5ms","func":"get","region":"{Region[6] ConfVer[5] Version[2] Store[4] KeyRange[]:[t\\200\\000\\000\\000\\000\\000\\000\\377\\005\\000\\000\\000\\000\\000\\000\\000\\370]}","key":"k1","spans":[{"name":"getRegionByKey","start":"16:16:07.613","end":"16:16:07.613","duration":"0ms"},{"name":"callWithRetry tikvpb.Tikv/RawGet","start":"16:16:07.613","end":"16:16:07.617","duration":"4ms"},{"name":"gRPC tikvpb.Tikv/RawGet","start":"16:16:07.613","end":"16:16:07.617","duration":"4ms"}]}
```

## Slow log configurations

| SlowLog settings | default value |
| -- | -- |
| tikv.rawkv.read_slowlog_in_ms | tikv.grpc.timeout_in_ms * 2 |
| tikv.rawkv.write_slowlog_in_ms | tikv.grpc.timeout_in_ms * 2 |
| tikv.rawkv.batch_read_slowlog_in_ms | tikv.grpc.timeout_in_ms * 2 |
| tikv.rawkv.batch_write_slowlog_in_ms | tikv.grpc.timeout_in_ms * 2 |
| tikv.rawkv.scan_slowlog_in_ms | 5s |

Each settings can be set by system properties, configuration files or `set...` method of `TiConfiguration`.

System properties can be set by `-D` parameter of `java` command.

```
java -cp target/java-client-example-1.0-SNAPSHOT-jar-with-dependencies.jar -Dtikv.rawkv.read_slowlog_in_ms=100 com.example.App
```

Configuration file is `src/main/resources/tikv.properties` in maven projects.

## Visualize slow log