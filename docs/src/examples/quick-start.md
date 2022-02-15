# Quick Start

The package is hosted on maven central repository. To build from source, refer to the [Contribution Guide](../contribution/introduction.html).

## Create a maven project

First download [maven] and follow the [installation instructoins][install]. Then `mvn` command should be available in the `$PATH`.

[maven]: https://maven.apache.org/download.html
[install]: https://maven.apache.org/install.html

create a maven project by following command:

```
mvn archetype:generate -DgroupId=com.example -DartifactId=java-client-example -DarchetypeArtifactId=maven-archetype-quickstart -DinteractiveMode=false
cd java-client-example
```

## Add dependency

Add maven dependency to `pom.xml`.

```xml
<dependency>
  <groupId>org.tikv</groupId>
  <artifactId>tikv-client-java</artifactId>
  <version>3.1.0</version>
</dependency>
<dependency>
  <groupId>org.slf4j</groupId>
  <artifactId>slf4j-log4j12</artifactId>
  <version>1.7.32</version>
</dependency>
```

Now `pom.xml` should look like this:

```xml
<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/maven-v4_0_0.xsd">
  <modelVersion>4.0.0</modelVersion>
  <groupId>com.example</groupId>
  <artifactId>java-project</artifactId>
  <packaging>jar</packaging>
  <version>1.0-SNAPSHOT</version>
  <name>java-project</name>
  <url>http://maven.apache.org</url>
  <properties>
    <maven.compiler.source>1.8</maven.compiler.source>
    <maven.compiler.target>1.8</maven.compiler.target>
  </properties>
  <dependencies>
    <dependency>
      <groupId>junit</groupId>
      <artifactId>junit</artifactId>
      <version>4.12</version>
      <scope>test</scope>
    </dependency>
    <dependency>
      <groupId>org.tikv</groupId>
      <artifactId>tikv-client-java</artifactId>
      <version>3.1.0</version>
    </dependency>
    <dependency>
      <groupId>org.slf4j</groupId>
      <artifactId>slf4j-log4j12</artifactId>
      <version>1.7.32</version>
    </dependency>
  </dependencies>
</project>
```

## Writing code

To interact with TiKV, we should first create a `TiConfiguration` with PD address, create a `TiSession` using `TiSession.create`, and then create a client.
For example, if we want to put a `World` in `Hello` key in RawKV, write the following code in `src/main/java/com/example/App.java`.

```java
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.raw.RawKVClient;
import org.tikv.shade.com.google.protobuf.ByteString;

public class App {
  public static void main(String[] args) throws Exception {
    String pdAddr = "127.0.0.1:2379";
    // You MUST create a raw configuration if you are using RawKVClient.
    TiConfiguration conf = TiConfiguration.createRawDefault(pdAddr);
    try (TiSession session = TiSession.create(conf)) {
      try (RawKVClient client = session.createRawClient()) {
        client.put(ByteString.copyFromUtf8("Hello"), ByteString.copyFromUtf8("World"));
        ByteString value = client.get(ByteString.copyFromUtf8("Hello"));
        System.out.println(value);
      }
    }
  }
}
```

More examples for RawKV and TxnKV are in following chapters.

## Running program

Run following command:

```
mvn assembly:assembly -DdescriptorId=jar-with-dependencies
java -cp target/java-client-example-1.0-SNAPSHOT-jar-with-dependencies.jar com.example.App
```
