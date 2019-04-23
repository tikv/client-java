## Ti-Client in Java [Under Construction]

A Java client for [TiDB](https://github.com/pingcap/tidb)/[TiKV](https://github.com/tikv/tikv).
It is supposed to:
+ Communicate via [gRPC](http://www.grpc.io/)
+ Talk to Placement Driver searching for a region
+ Talk to TiKV for reading/writing data and the resulted data is encoded/decoded just like what we do in TiDB.
+ Talk to Coprocessor for calculation pushdown

## How to build

The alternative way to build a usable jar for testing will be
```
mvn clean install -Dmaven.test.skip=true
```

The following command can install dependencies for you.
```
mvn package
```

Alternatively, you can use `bazel` for much faster build. When you try this approach, you should run `git submodule update --init --recursive` before you build project.
Because bazel often has incompatiable issue acrros different version, we use `bazelisk` to use specific bazel to build our project. You can
download `bazelisk` by `go get github.com/philwo/bazelisk` if you already have `GO` setup.
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

## Raw TiKV-Client in Java
Java Implementation of Raw TiKV-Client

Demo is avaliable in [KVRawClientTest](https://github.com/birdstorm/KVRawClientTest/)

### Build
```
mvn clean install -Dmaven.test.skip=true
```

### Use as maven dependency
After building, add following lines into your `pom.xml` 
```xml
<dependency>
    <groupId>org.tikv</groupId>
    <artifactId>tikv-client-java</artifactId>
    <version>2.0-SNAPSHOT</version>
</dependency>
```

### Entrance
`com.pingcap.tikv.RawKVClient`

### API

```java
  /**
   * create a RawKVClient using specific pd addresses
   *
   * @param address pd addresses(comma seperated)
   */
  static RawKVClient create(String address)
```

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
