# protomap-test-java

A synthetic code base of the fictional company Acme for testing ProtoMap: 400 gRPC services with 3057 methods in 20
domains, and 11386 protobuf messages with 49571 fields, many nested several levels deep.

It was generated, with the eight other `protomap-test-*` repositories, from one catalog of Acme's APIs. The repositories
own disjoint domains and call each other's services, so each holds copies of the protobuf files it needs from the
others, byte for byte as their owners have them.

## Layout

Java 21 and Spring Boot 3 run a modular finance server. `api/` compiles the vendored contracts in `proto/` with
protobuf-maven-plugin; generated protobuf messages and grpc-java stubs stay under each module's `target/`. `domains/`
contains constructor-injected workflows, gRPC endpoints, mapper interfaces and implementations, record DTOs, and JDBC
repository adapters. `platform/` supplies channel lookup, deadlines, codecs, retries, and event executors.
`application/` discovers all 400 `@GrpcService` endpoints behind the identity-checking global interceptor. Its jobs and
admin commands run under separate Spring profiles. PostgreSQL is the normal store; the `smoke` profile uses H2. Build
from the repository root with `mvn -q -B package -DskipTests`, using Maven 3.9 and Java 21 or newer. The reactor forks
compilation by API module to bound memory. Run the server with `java -jar
application/target/application-1.0.0-SNAPSHOT.jar`, with `ACME_SERVICE_TOKEN`, the credential of its identity lookups;
append `--spring.profiles.active=smoke` for local startup. Run `mvn -q -B package` for platform tests and
transport-boundary checks of each service's busiest and streaming methods. Background event flows alone are marked
`known_gap`; reflection, computed descriptors, generics, inherited handlers, and synchronous callbacks remain required.

The domains are `financeportal`, `audit`, `ledger`, `accounting`, `billing`, `invoicing`, `treasury`, `payroll`,
`expenses`, `procurement`, `suppliers`, `contracts`, `budgeting`, `assets`, `compliance`, `risk`, `lending`,
`collections`, `banking`, `insurance`. It calls services of `admissions`, `analytics`, `anomalies`, `appointments`,
`audiences`, `authorization`, `benefits`, `boxoffice`, `broadcasts`, `campaigns`, `careplans`, `cart`, `catalog`,
`certificates`, `chat`, `checkout`, `community`, `concessions`, `configuration`, `content`, `corporateevents`,
`customers`, `datahub`, `datasets`, `deliveries`, `devices`, `dispatch`, `documents`, `driverapp`, `drivers`,
`edgegateway`, `employeeportal`, `employees`, `experiments`, `fanportal`, `featureflags`, `featurestore`, `fleet`,
`forecasting`, `forms`, `fraud`, `fuel`, `fulfillment`, `giftcards`, `helpdesk`, `identity`, `itsm`, `keymanagement`,
`labeling`, `learning`, `licensing`, `lineage`, `localization`, `loyalty`, `maintenance`, `media`, `merchandising`,
`messaging`, `metrics`, `modelregistry`, `notifications`, `onboarding`, `orders`, `patientapp`, `patients`, `payments`,
`performance`, `performers`, `personalization`, `pipelines`, `prescriptions`, `pricing`, `ratelimiting`,
`recommendations`, `recruiting`, `returns`, `routing`, `scheduling`, `search`, `seating`, `secrets`, `sellers`,
`shipping`, `shows`, `sponsorships`, `subscriptions`, `surveys`, `telehealth`, `telematics`, `ticketing`, `timekeeping`,
`tours`, `travel`, `venues`, `webbff`, `webhooks`, `workplaces`, `yards` too.

## Oracle

`expected.json` lists what scanning the repository must find: every handler, every call a handler or a program makes,
and every field-level flow from where a handler has data to where a call or its response takes it, each tagged with the
shape of the code that makes it. `crates/graph/tests/fixture_coverage.rs` in `protomap-source` checks a scan against it.

The code mixes in, on purpose, what makes static analysis of real code bases hard. Flows by pattern:

| Pattern | Flows |
|---|---|
| `anonymous_class` | 980 |
| `any_roundtrip` | 986 |
| `application_event` | 888 |
| `async_each` | 492 |
| `batch_job` | 3 |
| `bidi_call` | 524 |
| `blocking_stub` | 1836 |
| `builder_chain` | 928 |
| `channel_registry` | 1046 |
| `client_stream_call` | 419 |
| `completable_fanout` | 2057 |
| `dynamic_descriptor` | 1067 |
| `future_stub` | 877 |
| `generic_call` | 1052 |
| `inner_adapter` | 886 |
| `json_roundtrip` | 988 |
| `loop_each` | 488 |
| `mapper_interface` | 1070 |
| `method_reference` | 858 |
| `oneof_switch` | 2700 |
| `optional_chain` | 894 |
| `port_adapter` | 1047 |
| `record_dto` | 876 |
| `reflection_copy` | 908 |
| `request_echo` | 4556 |
| `retry_lambda` | 261 |
| `server_stream_call` | 886 |
| `stream_each` | 603 |
| `stream_pipeline` | 1005 |
| `to_builder` | 1034 |

Flows whose values pass through something on their way, by what:

| Transform | Flows |
|---|---|
| `any_pack` | 337 |
| `any_pack_unpack` | 672 |
| `json_bytes` | 255 |
| `json_roundtrip` | 690 |
| `mapper_interface` | 744 |
| `oneof_switch` | 583 |
| `record_dto` | 602 |
| `reflection_copy` | 634 |
| `to_builder` | 718 |

Calls by pattern:

| Pattern | Calls |
|---|---|
| `anonymous_class` | 201 |
| `any_roundtrip` | 205 |
| `application_event` | 191 |
| `async_each` | 143 |
| `batch_job` | 6 |
| `bidi_call` | 131 |
| `blocking_stub` | 376 |
| `builder_chain` | 194 |
| `channel_registry` | 224 |
| `client_stream_call` | 134 |
| `completable_fanout` | 457 |
| `dynamic_descriptor` | 225 |
| `future_stub` | 185 |
| `generic_call` | 225 |
| `inner_adapter` | 194 |
| `json_roundtrip` | 210 |
| `loop_each` | 135 |
| `mapper_interface` | 222 |
| `method_reference` | 175 |
| `oneof_switch` | 505 |
| `optional_chain` | 184 |
| `port_adapter` | 222 |
| `record_dto` | 186 |
| `reflection_copy` | 187 |
| `retry_lambda` | 62 |
| `server_stream_call` | 256 |
| `stream_each` | 166 |
| `stream_pipeline` | 211 |
| `to_builder` | 215 |
