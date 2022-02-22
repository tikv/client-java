# Slow Request Diagnosis

If a request take too much time, we can collect the detailed time spend in each component in a “slow log”.

<!-- wrap text in the code block -->
<pre>
<code class="hljs" style="white-space: pre-wrap;">
2022-02-11 11:07:56 WARN  SlowLogImpl:88 - A request spent 55 ms. start=11:07:56.938, end=11:07:56.993, SlowLog:{"trace_id":4361090673996453790,"spans":[{"event":"put","begin":"11:07:56.938","duration_ms":55,"properties":{"region":"{Region[2] ConfVer[5] Version[60] Store[1] KeyRange[]:[]}","key":"Hello"}},{"event":"getRegionByKey","begin":"11:07:56.938","duration_ms":0},{"event":"callWithRetry","begin":"11:07:56.943","duration_ms":49,"properties":{"method":"tikvpb.Tikv/RawPut"}},{"event":"gRPC","begin":"11:07:56.943","duration_ms":49,"properties":{"method":"tikvpb.Tikv/RawPut"}}]}
</code>
</pre>

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

TBD