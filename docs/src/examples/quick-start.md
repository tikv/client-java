# Quick Start

## Prerequisites

Version Requirements:

- JDK: >= 1.8
- Maven: >= 3.8.4 (TODO: lower the required version)
- Java Client: >= v3.1.0
- TiKV: >= v4.0.0

To run the example codes, there must be a TiKV installed. Please refer to
[Install TiKV](https://tikv.org/docs/5.1/deploy/install/install/) to deploy a
TiKV cluster.

## Quick Start: Access TiKV via RawKV API

Create a quick start maven project:

```sh
mvn archetype:generate -DgroupId=com.mycompany.app -DartifactId=my-app  \
-DarchetypeArtifactId=maven-archetype-quickstart -DarchetypeVersion=1.4 \
-DinteractiveMode=false
```

Add the java client dependency to `pom.xml`:

```xml
<dependency>
	<groupId>org.tikv</groupId>
	<artifactId>tikv-client-java</artifactId>
	<version>3.1.0</version>
</dependency>
```

Write test codes to interact with TiKV:

```java
package com.mycompany.app;

import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.raw.RawKVClient;
import org.tikv.shade.com.google.protobuf.ByteString;

public class App {
    public static void main(String[] args) {
        String pdAddress = "127.0.0.1:2379";
        TiConfiguration conf = TiConfiguration.createRawDefault(pdAddress);
        TiSession session = TiSession.create(conf);
        RawKVClient client = session.createRawClient();

        // put one key
        ByteString key = ByteString.copyFromUtf8("k0");
        ByteString val = ByteString.copyFromUtf8("Hello, World!");
        client.put(key, val);
        System.out.println(client.get(key));

        // put another key
        key = ByteString.copyFromUtf8("k1");
        val = ByteString.copyFromUtf8("Hello, TiKV!");
        client.put(key, val);
        System.out.println(client.get(key));
    }
}
```

Build and run the project:

```sh
mvn clean package
java -cp target/my-app-1.0-SNAPSHOT.jar com.mycompany.app.App
```
