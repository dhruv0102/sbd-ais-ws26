# Decision Record — Kafka for the Line Guardian Readings Pipeline

**Course:** SBD/AIS — Exercise 5: Messaging Platforms  
**Student:** Dhruv Gangwani  
**Decision:** Use Apache Kafka for machine-reading events; retain PostgreSQL as the system of record for the machine registry.

## Context

The laboratory setup has 60 machines producing 10 readings per second each, or 600 readings per second. The planned rollout has 2,000 machines producing 50 readings per second each, or 100,000 readings per second. The bearing model is stateful per machine and requires every reading in sequence. Anomalies must be detected within five seconds at the full rate. Raw readings must also be available to an archive and dashboard independently of the detector, without increasing PostgreSQL polling load.

## Options considered

### Option 1 — Keep PostgreSQL and improve polling

The existing design writes readings to PostgreSQL and has the detector poll for rows with IDs greater than its last processed ID. Polling could be tuned or divided among several pollers. However, the Stage A experiment illustrates a correctness risk: sequence IDs are allocated at insertion, not commit. A lower-ID transaction that commits late can fall behind the detector's saved ID checkpoint and never be processed. Adding pollers does not automatically remove this issue. At 100,000 readings per second, repeated polling would also create substantial database load, and each additional consumer would need to query PostgreSQL.

### Option 2 — Publish readings to Kafka

Gateways publish readings to a Kafka topic named `readings`. Each message uses `machine_id` as its key. Kafka routes a given machine's messages to the same partition and preserves their order within that partition. The topic can be split into partitions so that detector consumers in one consumer group process partitions in parallel. The archive and dashboard use separate consumer groups, so they can independently consume the same raw event stream and maintain their own offsets without polling PostgreSQL.

## Decision against the requirements

| Requirement | Kafka design |
|---|---|
| **R1 — Scale from 600 to 100,000 readings/second** | Increase topic partitions and detector consumer capacity. At 3 ms per reading, one instance has a theoretical capacity of about 333.33 readings/second: at least 2 instances for 600/second and at least 300 for 100,000/second. These are minimum estimates; production needs headroom. |
| **R2 — Complete and ordered readings per machine** | Use `machine_id` as the Kafka key. All readings for a machine are routed to one partition, where their order is preserved. The detector must process each record and correctly maintain or recover the model state for that machine. |
| **R3 — Anomaly detection within five seconds** | Use enough partitions and detector consumers to keep processing capacity above the incoming rate. Monitor consumer lag and end-to-end reading-to-event latency. Kafka removes periodic database polling, but the five-second target still needs to be validated under the full target load. |
| **R4 — Independent archive and dashboard** | Run the detector, archive and dashboard as separate consumer groups. Each group consumes the readings topic independently and has its own offsets. |

## Operational considerations and consequences

Kafka introduces responsibilities for partition sizing, consumer-group management, offset handling, monitoring and recovery. Ordering is guaranteed within a partition, not across all partitions. Because the bearing model keeps per-machine state in memory, a partition rebalance can move a machine to another detector instance. The implementation must preserve or reconstruct that machine's model state; relying only on Kafka's ordering guarantee is not sufficient for stateful inference.

The theoretical capacity calculation is a sizing baseline, not proof that the five-second latency requirement is met at 100,000 readings per second. That target must be load-tested with the intended number of partitions and detector instances.

## Final decision

Adopt Kafka as the event-streaming backbone for high-volume machine readings. Use `machine_id` as the key on the `readings` topic, scale detector consumers through partitions and a shared consumer group, and give the archive and dashboard their own consumer groups. Keep PostgreSQL for the machine registry and relational system-of-record data.
