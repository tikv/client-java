# Quick Start

The package is hosted on maven central repository. To build from source, refer to the [Contribution Guide](../contribution/introduction.html).

Add maven dependency to `pom.xml`:

```xml
<dependency>
	<groupId>org.tikv</groupId>
	<artifactId>tikv-client-java</artifactId>
	<version>3.1.0</version>
</dependency>
```

Create a `RawKVClient` by `TiSession` and `TiConfiguration`. It is recommended to keep using a single client and session through program, unless having performance issues.

```java
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.raw.RawKVClient;

public class Main {
	public static void main() {
		// You MUST create a raw configuration if you are using RawKVClient.
		TiConfiguration conf = TiConfiguration.createRawDefault("127.0.0.1:2379");
		try (TiSession session = TiSession.create(conf)) {
			try (RawKVClient client = session.createRawClient()) {
				// ... do operations with client
			}
		}
	}
}
```
