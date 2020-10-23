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

This project is designed to hook with `pd` and `tikv` which you can find in `PingCAP` github page.

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
	<version>3.0.0</version>
</dependency>
```

### Entrance
`org.tikv.raw.RawKVClient`

### Create a RawKVClient

```java
import org.tikv.common.TiSession;
import org.tikv.raw.RawKVClient;

public class Main {
	public static void main() {
		// You MUST create a raw configuration if you are using RawKVClient.
		TiConfiguration conf = TiConfiguration.createRawDefault(YOUR_PD_ADDRESSES);
		TiSession session = TiSession.create(conf);
		RawKVClient = session.createRawKVClient();
	}
}
```

### API

```java
/**
 * Put a raw key-value pair to TiKV
 *
 * @param key raw key
 * @param value raw value
 */
void put(ByteString key, ByteString value)
```

```java
/**
 * Get a raw key-value pair from TiKV if key exists
 *
 * @param key raw key
 * @return a ByteString value if key exists, ByteString.EMPTY if key does not exist
 */
ByteString get(ByteString key)
```

```java
/**
 * Scan raw key-value pairs from TiKV in range [startKey, endKey)
 *
 * @param startKey raw start key, inclusive
 * @param endKey raw end key, exclusive
 * @param limit limit of key-value pairs scanned, should be less than {@link #MAX_RAW_SCAN_LIMIT}
 * @return list of key-value pairs in range
 */
List<Kvrpcpb.KvPair> scan(ByteString startKey, ByteString endKey, int limit)
```

```java
/**
 * Scan raw key-value pairs from TiKV in range [startKey, endKey)
 *
 * @param startKey raw start key, inclusive
 * @param limit limit of key-value pairs scanned, should be less than {@link #MAX_RAW_SCAN_LIMIT}
 * @return list of key-value pairs in range
 */
List<Kvrpcpb.KvPair> scan(ByteString startKey, int limit)
```

```java
/**
 * Delete a raw key-value pair from TiKV if key exists
 *
 * @param key raw key to be deleted
 */
void delete(ByteString key)
```

## License
Apache 2.0 license. See the [LICENSE](./LICENSE) file for details.
