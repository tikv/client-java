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

### RawKVClient API

```java
/**
 * Put a raw key-value pair to TiKV
 *
 * @param key raw key
 * @param value raw value
 */
public void put(ByteString key, ByteString value)

/**
 * Put a raw key-value pair to TiKV
 *
 * @param key raw key
 * @param value raw value
 * @param ttl the ttl of the key (in seconds), 0 means the key will never be outdated
 */
public void put(ByteString key, ByteString value, long ttl)

/**
 * Put a key-value pair if it does not exist. This API is atomic.
 *
 * @param key key
 * @param value value
 * @return a ByteString. returns ByteString.EMPTY if the value is written successfully. returns
 *     the previous key if the value already exists, and does not write to TiKV.
 */
public ByteString putIfAbsent(ByteString key, ByteString value)

/**
 * Put a key-value pair with TTL if it does not exist. This API is atomic.
 *
 * @param key key
 * @param value value
 * @param ttl TTL of key (in seconds), 0 means the key will never be outdated.
 * @return a ByteString. returns ByteString.EMPTY if the value is written successfully. returns
 *     the previous key if the value already exists, and does not write to TiKV.
 */
public ByteString putIfAbsent(ByteString key, ByteString value, long ttl)
```

```java
/**
 * Put a set of raw key-value pair to TiKV, this API does not ensure the operation is atomic.
 *
 * @param kvPairs kvPairs
 */
public void batchPut(Map<ByteString, ByteString> kvPairs)

/**
 * Put a set of raw key-value pair to TiKV, this API does not ensure the operation is atomic.
 *
 * @param kvPairs kvPairs
 * @param ttl the TTL of keys to be put (in seconds), 0 means the keys will never be outdated
 */
public void batchPut(Map<ByteString, ByteString> kvPairs, long ttl)

/**
 * Put a set of raw key-value pair to TiKV, this API is atomic
 *
 * @param kvPairs kvPairs
 */
public void batchPutAtomic(Map<ByteString, ByteString> kvPairs)

/**
 * Put a set of raw key-value pair to TiKV, this API is atomic.
 *
 * @param kvPairs kvPairs
 * @param ttl the TTL of keys to be put (in seconds), 0 means the keys will never be outdated
 */
public void batchPutAtomic(Map<ByteString, ByteString> kvPairs, long ttl)


```

```java
/**
 * Get a raw key-value pair from TiKV if key exists
 *
 * @param key raw key
 * @return a ByteString value if key exists, ByteString.EMPTY if key does not exist
 */
public ByteString get(ByteString key)
```

```java
/**
 * Get a list of raw key-value pair from TiKV if key exists
 *
 * @param keys list of raw key
 * @return a ByteString value if key exists, ByteString.EMPTY if key does not exist
 */
public List<KvPair> batchGet(List<ByteString> keys)
```

```java
/**
 * Delete a list of raw key-value pair from TiKV if key exists
 *
 * @param keys list of raw key
 */
public void batchDelete(List<ByteString> keys)

/**
 * Delete a list of raw key-value pair from TiKV if key exists, this API is atomic
 *
 * @param keys list of raw key
 */
public void batchDeleteAtomic(List<ByteString> keys)
```

```java
/**
 * Get the TTL of a raw key from TiKV if key exists
 *
 * @param key raw key
 * @return a Long indicating the TTL of key ttl is a non-null long value indicating TTL if key
 *     exists. - ttl=0 if the key will never be outdated. - ttl=null if the key does not exist
 */
public Long getKeyTTL(ByteString key)
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
public List<KvPair> scan(ByteString startKey, ByteString endKey, int limit)

/**
 * Scan raw key-value pairs from TiKV in range [startKey, endKey)
 *
 * @param startKey raw start key, inclusive
 * @param endKey raw end key, exclusive
 * @param limit limit of key-value pairs scanned, should be less than {@link #MAX_RAW_SCAN_LIMIT}
 * @param keyOnly whether to scan in key-only mode
 * @return list of key-value pairs in range
 */
public List<KvPair> scan(ByteString startKey, ByteString endKey, int limit, boolean keyOnly)

/**
 * Scan raw key-value pairs from TiKV in range [startKey, ♾)
 *
 * @param startKey raw start key, inclusive
 * @param limit limit of key-value pairs scanned, should be less than {@link #MAX_RAW_SCAN_LIMIT}
 * @return list of key-value pairs in range
 */
public List<KvPair> scan(ByteString startKey, int limit)

/**
 * Scan raw key-value pairs from TiKV in range [startKey, ♾)
 *
 * @param startKey raw start key, inclusive
 * @param limit limit of key-value pairs scanned, should be less than {@link #MAX_RAW_SCAN_LIMIT}
 * @param keyOnly whether to scan in key-only mode
 * @return list of key-value pairs in range
 */
public List<KvPair> scan(ByteString startKey, int limit, boolean keyOnly)

/**
 * Scan all raw key-value pairs from TiKV in range [startKey, endKey)
 *
 * @param startKey raw start key, inclusive
 * @param endKey raw end key, exclusive
 * @return list of key-value pairs in range
 */
public List<KvPair> scan(ByteString startKey, ByteString endKey)

/**
 * Scan all raw key-value pairs from TiKV in range [startKey, endKey)
 *
 * @param startKey raw start key, inclusive
 * @param endKey raw end key, exclusive
 * @param keyOnly whether to scan in key-only mode
 * @return list of key-value pairs in range
 */
public List<KvPair> scan(ByteString startKey, ByteString endKey, boolean keyOnly)
```

```java
/**
 * Scan keys with prefix
 *
 * @param prefixKey prefix key
 * @param limit limit of keys retrieved
 * @param keyOnly whether to scan in keyOnly mode
 * @return kvPairs with the specified prefix
 */
public List<KvPair> scanPrefix(ByteString prefixKey, int limit, boolean keyOnly)

/**
 * Scan keys with prefix without limit and retrieve key and value
 *
 * @param prefixKey prefix key
 * @return kvPairs with the specified prefix
 */
public List<KvPair> scanPrefix(ByteString prefixKey)

/**
 * Scan keys with prefix without limit
 *
 * @param prefixKey prefix key
 * @param keyOnly whether to scan in keyOnly mode
 * @return kvPairs with the specified prefix
 */
public List<KvPair> scanPrefix(ByteString prefixKey, boolean keyOnly)
```

```java
/**
 * Delete a raw key-value pair from TiKV if key exists
 *
 * @param key raw key to be deleted
 */
public void delete(ByteString key)

/**
 * Delete all raw key-value pairs in range [startKey, endKey) from TiKV
 *
 * <p>Cautious, this API cannot be used concurrently, if multiple clients write keys into this
 * range along with deleteRange API, the result will be undefined.
 *
 * @param startKey raw start key to be deleted
 * @param endKey raw start key to be deleted
 */
public synchronized void deleteRange(ByteString startKey, ByteString endKey)

/**
 * Delete all raw key-value pairs with the prefix `key` from TiKV
 *
 * <p>Cautious, this API cannot be used concurrently, if multiple clients write keys into this
 * range along with deleteRange API, the result will be undefined.
 *
 * @param key prefix of keys to be deleted
 */
public synchronized void deletePrefix(ByteString key) 
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

## License
Apache 2.0 license. See the [LICENSE](./LICENSE) file for details.
