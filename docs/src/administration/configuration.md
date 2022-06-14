## Java Client Configuration Parameter

### JVM Parameter

The following includes JVM related parameters.

#### tikv.pd.addresses
- pd addresses, separated by comma
- default: 127.0.0.1:2379

#### tikv.grpc.timeout_in_ms
- timeout of grpc request  
- default: 600ms

#### tikv.grpc.scan_timeout_in_ms
- timeout of scan/delete range grpc request
- default: 20s

#### tikv.importer.max_kv_batch_bytes
- Maximal package size transporting from clients to TiKV Server (ingest API)
- default: 1048576 (1M)

#### tikv.importer.max_kv_batch_size
- Maximal batch size transporting from clients to TiKV Server (ingest API)
- default: 32768 (32K)

#### tikv.scatter_wait_seconds
- time to wait for scattering regions
- default: 300 (5min)

#### tikv.rawkv.default_backoff_in_ms
- RawKV default backoff in milliseconds
- default: 20000 (20 seconds)

### Metrics Parameter

#### tikv.metrics.enable
- whether to enable metrics exporting
- default: false

#### tikv.metrics.port
- the metrics exporting http port
- default: 3140

### ThreadPool Parameter

The following includes ThreadPool related parameters, which can be passed in through JVM parameters.

#### tikv.batch_get_concurrency
- the thread pool size of batchGet on client side
- default: 20

#### tikv.batch_put_concurrency
- the thread pool size of batchPut on client side
- default: 20

#### tikv.batch_delete_concurrency
- the thread pool size of batchDelete on client side
- default: 20

#### tikv.batch_scan_concurrency
- the thread pool size of batchScan on client side
- default: 5

#### tikv.delete_range_concurrency
- the thread pool size of deleteRange on client side
- default: 20

#### tikv.enable_atomic_for_cas
- whether to enable `Compare And Set`, set true if using `RawKVClient.compareAndSet` or `RawKVClient.putIfAbsent`
- default: false

### TLS

#### tikv.tls_enable
- whether to enable TLS
- default: false

#### tikv.trust_cert_collection
- Trusted certificates for verifying the remote endpoint's certificate, e.g. /home/tidb/ca.pem. The file should contain an X.509 certificate collection in PEM format.
- default: null

#### tikv.key_cert_chain
- an X.509 certificate chain file in PEM format, e.g. /home/tidb/client.pem.
- default: null

#### tikv.key_file
- a PKCS#8 private key file in PEM format. e.g. /home/tidb/client-key.pem.
- default: null

#### tikv.tls.reload_interval
- The interval in seconds to poll the change of TLS context, if a change detected, the SSL context will be rebuilded.
- default: `"10s"`, `"0s"` means disable TLS context reload.

#### tikv.conn.recycle_time
- After `tikv.conn.recycle_time` (in seconds) with a TLS context reloading, the old connections will be forced to shutdown preventing channel leak.
- default: `"60s"`.

#### tikv.rawkv.read_timeout_in_ms
- RawKV read timeout in milliseconds. This parameter controls the timeout of `get` `getKeyTTL`.
- default: 2000 (2 seconds)

#### tikv.rawkv.write_timeout_in_ms
- RawKV write timeout in milliseconds. This parameter controls the timeout of `put` `putAtomic` `putIfAbsent` `delete` `deleteAtomic`.
- default: 2000 (2 seconds)

#### tikv.rawkv.batch_read_timeout_in_ms
- RawKV batch read timeout in milliseconds. This parameter controls the timeout of `batchGet`.
- default: 2000 (2 seconds)

#### tikv.rawkv.batch_write_timeout_in_ms
- RawKV batch write timeout in milliseconds. This parameter controls the timeout of `batchPut` `batchDelete` `batchDeleteAtomic`.
- default: 2000 (2 seconds)

#### tikv.rawkv.scan_timeout_in_ms
- RawKV scan timeout in milliseconds. This parameter controls the timeout of `batchScan` `scan` `scanPrefix`.
- default: 10000 (10 seconds)

#### tikv.rawkv.clean_timeout_in_ms
- RawKV clean timeout in milliseconds. This parameter controls the timeout of `deleteRange` `deletePrefix`.
- default: 600000 (10 minutes)