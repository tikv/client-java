## TiKV JAVA Client

A Java client for [TiDB](https://github.com/pingcap/tidb)/[TiKV](https://github.com/tikv/tikv).
It is supposed to:
+ Communicate via [gRPC](http://www.grpc.io/)
+ Talk to Placement Driver searching for a region
+ Talk to TiKV for reading/writing data and the resulted data is encoded/decoded just like what we do in TiDB.
+ Talk to Coprocessor for calculation pushdown

## Quick Start

> TiKV Java Client is designed to communicate with [pd](https://github.com/tikv/pd) and [tikv](https://github.com/tikv/tikv), please run TiKV and PD in advance.

Build java client from source file:

```sh
mvn clean install -Dmaven.test.skip=true
```

Add maven dependency to `pom.xml`:

```xml
<dependency>
	<groupId>org.tikv</groupId>
	<artifactId>tikv-client-java</artifactId>
	<version>3.1.0</version>
</dependency>
```

Create a RawKVClient and communicates with TiKV:

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

Find more demo in [KVRawClientTest](https://github.com/birdstorm/KVRawClientTest/)

## Documentation

See [Java Client Documents](/docs/README.md) for references about how to config and monitor Java Client.

A [Maven site](https://tikv.github.io/client-java/site) is also available. It includes:
1. [API reference](https://tikv.github.io/client-java/site/xref/index.html) 
2. [Spotbugs Reports](https://tikv.github.io/client-java/site/spotbugs.html)

## Community

See [Contribution Guide](https://tikv.github.io/client-java/contribution/introduction.html) for references about how to contribute to this project.

## License

Apache 2.0 license. See the [LICENSE](./LICENSE) file for details.