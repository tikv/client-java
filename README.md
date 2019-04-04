## Ti-Client in Java [Under Construction]

A Java client for [TiDB](https://github.com/pingcap/tidb)/[TiKV](https://github.com/tikv/tikv).
It is supposed to:
+ Communicate via [gRPC](http://www.grpc.io/)
+ Talk to Placement Driver searching for a region
+ Talk to TiKV for reading/writing data and the resulted data is encoded/decoded just like what we do in TiDB.
+ Talk to Coprocessor for calculation pushdown

## How to build

### Gradle

Alternatively, you can build `tikv-client-java` with gradle.

The following command will build and test the project.

```
gradle init
gradle clean build -x test
```

To make a jar with dependencies

```
gradle clean fatJar -x test
```

The jar can be found in `./build/lib/`

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

### Bazel

Alternatively, you can use `bazel` for much faster build. When you try this approach, you should run `git submodule update --init --recursive` before you build project.

Making a uber jar:

```
make uber_jar
```

run Main class:

```
make run
```

run test cases:

```
make test
```

this project is designed to hook with `pd` and `tikv` which you can find in `PingCAP` github page.

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
	<version>2.0-SNAPSHOT</version>
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
 * @return list of key-value pairs in range
 */
List<Kvrpcpb.KvPair> scan(ByteString startKey, ByteString endKey)
```

```java
/**
 * Scan raw key-value pairs from TiKV in range [startKey, endKey)
 *
 * @param startKey raw start key, inclusive
 * @param limit limit of key-value pairs
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


## TODO
Contributions are welcomed. Here is a [TODO](https://github.com/tikv/client-java/wiki/TODO-Lists) and you might contact maxiaoyu@pingcap.com if needed.

## License
Apache 2.0 license. See the [LICENSE](./LICENSE) file for details.
