# protomap-test-java

A synthetic Java code base for testing [ProtoMap](https://protomap.ai) by [HoundDog.ai](https://hounddog.ai). It is part
of the code of Acme, a fictional company.

The code mixes in, on purpose, what makes static analysis of real code bases hard, and `expected.json` is the answer key
of what a scanner should find in it.

Acme's code is spread over multiple repositories, one per programming language. They were generated together from one
catalog of Acme's APIs. Each owns its own domains and calls the others' services, so it holds copies of the protobuf
files it needs from them, byte for byte as their owners have them. Scan them together to follow calls and data from one
repository to another.

| Repository                                                                         | Language   |
|------------------------------------------------------------------------------------|------------|
| [protomap-test-csharp](https://github.com/hounddogai/protomap-test-csharp)         | C#         |
| [protomap-test-golang](https://github.com/hounddogai/protomap-test-golang)         | Go         |
| **protomap-test-java** (this one)                                                  | Java       |
| [protomap-test-javascript](https://github.com/hounddogai/protomap-test-javascript) | JavaScript |
| [protomap-test-kotlin](https://github.com/hounddogai/protomap-test-kotlin)         | Kotlin     |
| [protomap-test-python](https://github.com/hounddogai/protomap-test-python)         | Python     |
| [protomap-test-rust](https://github.com/hounddogai/protomap-test-rust)             | Rust       |
| [protomap-test-swift](https://github.com/hounddogai/protomap-test-swift)           | Swift      |
| [protomap-test-typescript](https://github.com/hounddogai/protomap-test-typescript) | TypeScript |

## Repository Layout

A modular finance server on Java 21 and Spring Boot 3, built with Maven:

- `api/`: compiles the protobuf files in `proto/` with protobuf-maven-plugin. The generated messages and grpc-java stubs
  stay under each module's `target/` and are not committed.
- `domains/`: constructor-injected workflows, gRPC endpoints, mapper interfaces and implementations, record DTOs, and
  JDBC repository adapters.
- `platform/`: channel lookup, deadlines, codecs, retries, and event executors.
- `application/`: discovers every `@GrpcService` endpoint behind a global interceptor that checks identity. Its jobs and
  admin commands run under separate Spring profiles.

PostgreSQL is the normal store; the `smoke` profile uses H2.

## Build and Run

Build from the repository root with Maven 3.9 and Java 21 or newer. The build forks compilation by API module to bound
memory. Leave out `-DskipTests` to also run the platform tests and the transport checks of each service's busiest and
streaming methods.

```sh
mvn -q -B package -DskipTests
java -jar application/target/application-1.0.0-SNAPSHOT.jar --spring.profiles.active=smoke
```

The second command starts the server locally with the `smoke` profile. Set `ACME_SERVICE_TOKEN` to the credential of its
identity lookups.
