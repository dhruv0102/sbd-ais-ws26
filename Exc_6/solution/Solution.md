
# Exercise 6 – Card Sentinel: Real-time Fraud Detection with CDC

## Part 1 – Decision

I chose **log-based Change Data Capture (CDC) using Debezium**. Since the payment application is certified and should not be modified, approaches such as dual writes or an outbox would require changes to a system outside our control. Polling is also unsuitable because transactions committed out of ID order can be missed, while repeated queries add database load. Debezium reads PostgreSQL's Write-Ahead Log (WAL), capturing committed changes without modifying the payment application. The main trade-off is that an inactive connector can cause its replication slot to retain WAL, so connector health, replication lag and database disk usage must be monitored.

## Part 2 – Three experimental runs

### Run 1 – Default Debezium configuration

**Prediction:** I expected the default configuration to limit parallelism because the Kafka topic would have only one partition. Consequently, I expected the velocity agents to fall behind at 1,000 transactions per second.

**Observed velocity scoreboard:**
- R3 – Fraud detection: 3/9 (33%) – **FAIL** (requirement: >= 90%)
- R3 – False alerts: 0 – **PASS**
- R4 – Alert latency p50/p95: 44.0 s / 78.4 s – **FAIL** (requirement: p95 <= 5 s)

Only one of the three velocity agents received the single partition, leaving the other two idle. The active agent could not keep up with the incoming transaction rate, resulting in increasing lag and delayed alerts.

### Run 2 – Six Kafka partitions

**Prediction:** I expected six partitions to distribute the workload across all three velocity agents and improve processing speed. However, I expected card-testing detection might still fail because the default Kafka message key was the transaction primary key (`id`) rather than the cardholder ID.

**Observed velocity scoreboard:**
- R3 – Fraud detection: 1/10 (10%) – **FAIL** (requirement: >= 90%)
- R3 – False alerts: 0 – **PASS**
- R4 – Alert latency p50/p95: 2.2 s / 2.2 s – **PASS** (requirement: p95 <= 5 s)

All three velocity agents received two partitions each, significantly reducing lag and alert latency. However, transactions belonging to the same cardholder were distributed across different partitions. As a result, individual agents could not reliably see the complete transaction sequence needed to detect card testing.

### Run 3 – Six partitions with user_id as the message key

**Prediction:** I expected six partitions to maintain parallel processing while using `user_id` as the Kafka message key would ensure that transactions from the same cardholder were routed to the same partition. Therefore, I expected both fraud detection and alert latency to meet the requirements.

**Observed velocity scoreboard:**
- R3 – Fraud detection: 8/8 (100%) – **PASS** (requirement: >= 90%)
- R3 – False alerts: 0 – **PASS**
- R4 – Alert latency p50/p95: 0.7 s / 1.3 s – **PASS** (requirement: p95 <= 5 s)

Changing the message key to `user_id` ensured that all transactions belonging to the same cardholder were processed by the same velocity agent. This allowed the model to evaluate the complete transaction sequence while retaining parallel processing across different cardholders. Both velocity requirements were successfully met.

## Part 3 – Production implications of Debezium defaults

The two defaults that needed changing were the **single Kafka topic partition** and the **transaction primary key (`id`) as the default message key**. If nobody had checked them in production, the single partition could have created a processing bottleneck, increasing consumer lag and delaying fraud alerts. The default message key could also have distributed one cardholder's transactions across different partitions, causing the velocity model to miss card-testing patterns even if processing latency appeared acceptable. The team could have noticed these problems by monitoring Kafka consumer-group lag, partition assignments, alert latency and fraud-detection recall. Testing the partition distribution and validating detection using cardholder-level transaction sequences would also have exposed the incorrect message-key configuration.

## Final configuration

The final connector configuration is provided in the accompanying file:

`fraud-connector.json`

It includes six Kafka partitions, replication factor 1 and `user_id` as the message key.
