## TiKV JAVA Client

A Java client for [TiDB](https://github.com/pingcap/tidb)/[TiKV](https://github.com/tikv/tikv).
It is supposed to:
+ Communicate via [gRPC](http://www.grpc.io/)
+ Talk to Placement Driver searching for a region
+ Talk to TiKV for reading/writing data and the resulted data is encoded/decoded just like what we do in TiDB.
+ Talk to Coprocessor for calculation pushdown

## How to build

```
mvn clean package -Dmaven.test.skip=true
```

## How to run test

```
export RAWKV_PD_ADDRESSES=127.0.0.1:2379
export TXNKV_PD_ADDRESSES=127.0.0.1:2379
mvn clean test
```

## Usage

This project is designed to hook with [pd](https://github.com/tikv/pd) and [tikv](https://github.com/tikv/tikv).

When you work with this project, you have to communicate with `pd` and `tikv`. Please run TiKV and PD in advance.

## Component: Raw Ti-Client in Java

Java Implementation of Raw TiKV-Client to support RawKVClient commands.

Demo is avaliable in [KVRawClientTest](https://github.com/birdstorm/KVRawClientTest/)

### Build
```
mvn clean install -Dmaven.test.skip=true
```

### Add to dependency

#### Use jar for binary

Add your jar built with all dependencies into you project's library to use `tikv-client-java` as dependency

#### Use as maven dependency

After building, add following lines into your `pom.xml` if you are using Maven

```xml
<dependency>
	<groupId>org.tikv</groupId>
	<artifactId>tikv-client-java</artifactId>
	<version>3.1.0</version>
</dependency>
```

### Entrance
`org.tikv.raw.RawKVClient`

### Create a RawKVClient

```java
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.raw.RawKVClient;

public class Main {
	public static void main() {
		// You MUST create a raw configuration if you are using RawKVClient.
		TiConfiguration conf = TiConfiguration.createRawDefault(YOUR_PD_ADDRESSES);
		TiSession session = TiSession.create(conf);
		RawKVClient client = session.createRawClient();
	}
}
```

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

## Metrics

Client Java supports exporting metrics to Prometheus using poll mode and viewing on Grafana. The following steps shows how to enable this function.

### Step 1: Enable metrics exporting

- set the config `tikv.metrics.enable` to `true`
- call TiConfiguration.setMetricsEnable(true)

### Step 2: Set the metrics port

- set the config `tikv.metrics.port`
- call TiConfiguration.setMetricsPort

Default port is 3140.

### Step 3: Config Prometheus

Add the following config to `conf/prometheus.yml` and restart Prometheus.

```yaml
- job_name: "tikv-client"
    honor_labels: true
    static_configs:
    - targets:
        - '127.0.0.1:3140'
        - '127.0.0.2:3140'
        - '127.0.0.3:3140'
```

### Step 4: Config Grafana

Import the [Client-Java-Summary dashboard config](/metrics/grafana/client_java_summary.json) to Grafana.

## License
Apache 2.0 license. See the [LICENSE](./LICENSE) file for details.
