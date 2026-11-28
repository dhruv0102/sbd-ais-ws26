# Exercise 8 – The crash report, live

## 1. Architecture decision: B — Alert early, correct later

I chose **Architecture B: one streaming query that sends an alert as soon as a user reaches three qualifying crashes, then publishes a new version whenever that count changes**.

The implementation in Section 6 is:

```python
def start_query():
    records = read_stream().withWatermark("event_time", "30 seconds")
    return write_alerts(crash_report(records), mode="update")
```

The crash report filters for log content containing `crash` and severity `High` or `Critical`. It groups records by user and 10-second event-time interval, counts them, and keeps groups with `crash_count > 2`.

This architecture serves the two teams differently:

- **On-call engineer:** should receive the first alert when the count reaches three. The `pager()` reader keeps only the first message for each problem key, so later versions do not create another page.
- **Operations team:** needs the final, correct count, including late records. The `ops_view()` reader keeps the latest message for each problem key, so a later update replaces the earlier count.

The cost of Architecture B is that **every consumer of `crash_alerts` must understand that a key can appear more than once**. The pager must act only on the first message for a key; a report consumer must retain the latest version. A consumer that treats every Kafka message as a new problem would generate duplicate pages or count the same problem more than once.

I chose B instead of A because Architecture A waits for the watermark and emits final results, which is not suitable for early paging. I chose B instead of C because B uses one streaming query and one alert topic, rather than a separate batch reporting path from the lake. The measured result shows the trade-off: B produced a fully correct operations report and paged every problem, but narrowly missed the on-call p95 latency target. It therefore did **not** meet both requirements completely in this run.

## 2. How the Spark query runs

### Micro-batches and aggregation

Spark Structured Streaming continuously reads the Kafka topic `logs`. It processes the incoming data in **micro-batches**. For each batch, the query:

1. Reads Kafka records and parses their JSON values.
2. Creates `event_time` from the event timestamp. This represents when the event happened, not when Spark received it.
3. Filters for content containing `crash` and severity `High` or `Critical`.
4. Groups records by user and a 10-second event-time window.
5. Counts qualifying records and keeps groups where `crash_count > 2`.
6. Writes alert messages to the Kafka topic `crash_alerts`.

The query maintains **aggregation state** for each user/window so that it can update the count when more records arrive for that same interval.

### Watermark and late records

The query uses:

```python
.withWatermark("event_time", "30 seconds")
```

The watermark gives Spark a 30-second event-time lateness allowance. It helps Spark decide when an old window can be considered complete and its aggregation state can eventually be removed. Records that arrive after Spark has advanced beyond the relevant event-time window may be dropped.

The output mode is `update`:

```python
write_alerts(crash_report(records), mode="update")
```

This allows Spark to emit a qualifying count before the window is final, then emit a newer count when more records arrive. This is how Architecture B can alert early while still correcting the operations report later.

### Checkpoint and simulated restart

The query stores its checkpoint at:

```text
/home/jovyan/data/stream/checkpoint
```

The checkpoint stores the streaming progress, including source offsets, and the state needed to resume the aggregation. The checker stopped the query after alerts had been written and restarted it from its checkpoint.

In the Spark UI, the failed and restarted runs had the **same Query ID** but different Run IDs:

| Execution | Status | Query ID | Run ID |
|---|---|---|---|
| Simulated-crash run | FAILED | `0928585f-349c-4d86-91ec-de978e7f4a68` | `61f19c44-6a78-46ba-9969-6b85f81c9b7c` |
| Restarted run | FINISHED | `0928585f-349c-4d86-91ec-de978e7f4a68` | `ec999285-2dee-4b18-9864-223a5252ad9e` |

The Query ID identifies the same logical streaming query; the new Run ID identifies its restarted execution. The checkpoint let Spark resume the query's source progress and aggregation state rather than starting the logical query from scratch.

In terms of Exercise 7, the checkpoint behaves most like **Architecture A (state file)**: progress is remembered in a separate durable checkpoint. It is not like Exercise 7's Architecture B, where the lake itself is used to determine the next position. Spark's checkpoint also contains streaming state, not just a simple offset.

The Kafka write and Spark checkpoint commit are not one atomic transaction. The simulated crash occurred after writing alerts to Kafka but before the batch was fully committed in the checkpoint. On restart, some messages were therefore written again. This is why the reader needs to handle repeats.

### Spark UI evidence and observations

The three required Spark UI screenshots are included in this solution folder:

- `ui-runs.png` — the **Completed Streaming Queries** list, showing the failed run and restarted run, their shared Query ID and different Run IDs.
- `ui-delay.png` — query statistics, including **Batch Duration** and **Global Watermark Gap**.
- `ui-state.png` — aggregation state statistics, including **Aggregated Number Of Total State Rows** and **Aggregated Number Of Late Rows Dropped By Watermark**.

Observations from the Architecture B run:

- **Batch Duration:** most micro-batches in the graph took approximately 2–3 seconds, with some higher peaks.
- **Global Watermark Gap:** it started at roughly 44 seconds and settled around 32–34 seconds. This is the observed gap between the latest event-time progress and the watermark; it is not simply a fixed 30-second timer.
- **Total aggregation state rows:** approximately 2,200–3,300 rows were visible in the graph. Spark retains these rows to maintain the windowed counts.
- **Late rows dropped by watermark:** the graph remained at zero during this run.

The watermark gap and batch duration help explain the alert-delay behaviour. Update mode allows early alerts, but end-to-end delay also depends on micro-batch scheduling and processing. The watermark gap alone does not determine the alert delay. The observed p95 was 20.5 seconds, slightly above the 20-second requirement.

## 3. Pager behaviour: what it ignored and why

The notebook's pager is:

```python
def pager(messages):
    paged = {}
    for key, count in messages:
        if key not in paged:
            paged[key] = count
    return paged
```

The `if key not in paged` condition makes the pager keep only the first message for each problem key. In Architecture B, a key can appear more than once for two reasons:

1. **Repeats after restart:** a batch whose Kafka write happened before the simulated crash can be written again when Spark reprocesses it after restarting from the checkpoint.
2. **Updates:** with `update` output mode, Spark writes a new message when the count for the same user and interval changes, for example when another qualifying or late crash arrives.

The Architecture B checker reported:

| Kafka messages / pager handling | Count |
|---|---:|
| Messages written to `crash_alerts` | 798 |
| Pages produced by `pager()` | 639 |
| Repeats ignored after restart | 28 |
| Updates ignored by pager | 131 |

Without the `if key not in paged` condition, the on-call engineer would receive a page for every message: **798 page-triggering messages instead of 639 distinct problem pages**. That would include 28 repeated messages and 131 later count updates for problems that had already been paged.

The operations reader has different semantics:

```python
def ops_view(messages):
    return {key: count for key, count in messages}
```

When a key appears again, its dictionary value is replaced. Therefore, `ops_view()` retains the latest count for each problem, which is what the operations team needs.

## 4. Output of `check_all`

### Baseline example: 5-second watermark and append mode

The Section 5 example used a 5-second watermark and `append` output mode:

```python
def start_query():
    records = read_stream().withWatermark("event_time", "5 seconds")
    return write_alerts(crash_report(records), mode="append")
```

The checker reported that neither team was served:

| Measure | Result | Requirement |
|---|---:|---:|
| Actual problems | 624 | — |
| Problems paged | 585 / 624 (93.8%) | At least 95% |
| p50 page delay | 10.9 s | — |
| p95 page delay | 20.1 s | At most 20 s |
| Correct operations rows | 573 / 624 (91.8%) | At least 99% |
| Missing problems | 39 | — |
| Wrong counts | 12 | — |
| Late input records | 13,013 of 260,000 | — |

The 5-second watermark was too short for records that could arrive up to 30 seconds late. Some late records were not incorporated into the aggregation, causing missing problems or incorrect counts. Append mode emitted final results only after the window was considered complete, which was not suitable for the early-alert requirement.

### Chosen Architecture B: 30-second watermark and update mode

The chosen implementation used a 30-second watermark and `update` mode. The checker reported:

| Measure | Result | Requirement |
|---|---:|---:|
| Actual problems | 639 | — |
| Problems paged | 639 / 639 (100.0%) | At least 95% |
| p50 page delay | 0.7 s | — |
| p95 page delay | 20.5 s | At most 20 s |
| Correct operations rows | 639 / 639 (100.0%) | At least 99% |
| Missing problems | 0 | — |
| Wrong counts | 0 | — |
| Extra problems | 0 | — |
| Late input records | 13,014 of 260,000 | — |

The query was stopped once after writing alerts and restarted from its checkpoint. It wrote 798 messages to `crash_alerts`; the pager produced 639 distinct pages, ignoring 28 repeats and 131 updates.

**Conclusion:** Architecture B met the operations requirement: all 639 problems were reported with the correct count. It also paged all 639 problems. However, the p95 page delay was **20.5 seconds**, which is 0.5 seconds above the required maximum of 20 seconds. The on-call latency requirement was therefore narrowly missed, so Architecture B did not fully satisfy both requirements in this run. The p50 delay was 0.7 seconds, meaning the median alert was very fast, but the slowest 5% pushed the p95 above the target.

The two checker runs produced different numbers of actual problems (624 and 639). They are separate generated workloads, so their raw counts should not be compared as if they were identical data. The percentages and measured delays/accuracy are the useful comparison.

## 5. Files in the solution folder

The submitted folder should have this structure:

```text
Exc_8/
└── solution/
    ├── README.md
    ├── exercise8.ipynb
    ├── ui-runs.png
    ├── ui-delay.png
    └── ui-state.png
```

- `README.md` explains the architecture decision, Spark query and checkpoint, pager behaviour, Spark UI observations and checker output.
- `exercise8.ipynb` contains the implementation and the executed checks.
- The three PNG files provide the Spark UI evidence described in Section 2.

## 6. Submission and presentation

Push the complete `Exc_8/solution/` folder to your fork and add the repository link to the Moodle submission.

- **Deadline:** 02.12.2026, 18:00.
- **Presentation:** approximately 5 minutes in the next exercise session on 03.12.2026.
