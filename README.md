## TiKV JAVA Client

A Java client for [TiDB](https://github.com/pingcap/tidb)/[TiKV](https://github.com/tikv/tikv).
It is supposed to:
+ Communicate via [gRPC](http://www.grpc.io/)
+ Talk to Placement Driver searching for a region
+ Talk to TiKV for reading/writing data and the resulted data is encoded/decoded just like what we do in TiDB.
+ Talk to Coprocessor for calculation pushdown

## How to build

### Maven

The alternative way to build a usable jar for testing will be

```
mvn clean install -Dmaven.test.skip=true
```

The following command can install dependencies for you.

```
mvn package
```

The jar can be found in `./target/`

## Usage

This project is designed to hook with `[pd](https://github.com/tikv/pd)` and `[tikv](https://github.com/tikv/tikv)`.

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