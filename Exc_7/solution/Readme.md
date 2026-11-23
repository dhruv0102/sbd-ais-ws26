# Exercise 7 – From the Log to the Lake

**Student:** Dhruv Gangwani  
**Course:** SBD – AIS WS26

## 1. Architecture decision

I selected **Architecture B – The lake is the source of truth**.

In this architecture, the data lake determines the last successfully processed Kafka offset for each partition. The pipeline does not rely on a separate state file to track progress.

The implementation follows four steps:

1. `lake_position()` retrieves the next Kafka offsets from the existing published Parquet files.
2. `read_new(start)` reads only the records that have not yet been published.
3. `write_files(new, STAGING)` writes the new records into a temporary staging directory.
4. `publish(run)` publishes the completed run into the data lake.

The implementation of `run_once()` is:

```python
def run_once():
    start = lake_position()
    new = read_new(start)
    run = write_files(new, STAGING)
    publish(run)
```

## 2. Why Architecture B?

Architecture A uses a separate state file to remember the last processed offsets. However, a crash between writing the data and updating the state file can result in duplicate records during the next run.

Architecture B avoids this problem by using the published data lake as the source of truth.

Files are first written to staging and only become part of the published lake after the run is completed. If a run crashes before publishing, the next run can retry the records without duplicating previously published data.

## 3. Test results

I executed `check_all(run_once)` to verify the implementation.

All seven test scenarios passed:

| Scenario | Records in lake | Duplicates | Missing | Result |
|---|---:|---:|---:|---|
| First run | 2,000,000 | 0 | 0 | PASS |
| New records arrive | 2,100,000 | 0 | 0 | PASS |
| Nothing new | 2,100,000 | 0 | 0 | PASS |
| Crash while writing | 2,100,000 | 0 | 0 | PASS |
| Recovery after crash | 2,200,000 | 0 | 0 | PASS |
| Crash after writing | 2,200,000 | 0 | 0 | PASS |
| Recovery after crash | 2,300,000 | 0 | 0 | PASS |

### Interpretation

- The first run successfully loaded 2 million records.
- The second run correctly processed an additional 100,000 records.
- When no new records were available, the pipeline did not create duplicate files or records.
- When a run crashed during writing, the incomplete data was not published. The following run successfully recovered the missing records.
- When a run crashed after writing but before publishing, the following run also recovered the records without duplication.

The two crash scenarios temporarily showed 100,000 records not yet loaded. This was expected because the simulated runs had not completed publishing. Both subsequent recovery runs passed.

## 4. Conclusion

The implementation successfully demonstrates exactly-once processing for the tested scenarios.

The final checker output confirmed:

**Exactly once after every run: YES**

There were zero duplicate records and zero missing records after each successful run, including recovery following simulated crashes.

Architecture B was therefore a suitable solution for this exercise because it uses the published data lake as the source of truth and separates writing from publishing.
