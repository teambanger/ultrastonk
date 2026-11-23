# Implementation validation

Validated locally on 16 October 2026. These are correctness and deployment smoke checks, not sustained-capacity or full-dataset results.

| Check | Result |
|---|---|
| Flink Maven build | 13 tests passed: contracts, EMA recurrence, event-time closes, ticker isolation, policies, TTL, checkpoint/timer restoration, and prevention of reopening finalized windows after recovery |
| Dummy emitter tests | 7 tests passed: repeatable generation, duplicates/corrections, delays/backdating, all malformed modes, rate plans, validation, and telemetry |
| Policy benchmark matrix | All 9 dedup/late combinations passed the expected price multiset check; 500 originals/s, 3 measured seconds, 1 worker, 1 repetition, seed 42 |
| Final quality-labeled runner | Baseline/EXACT/GRACE run passed: 200 expected/observed prices, zero missing/extra; reproducibility manifest exported with source hashes and host metadata |
| Two-worker recovery workload | 13170 input records including duplicates; 12000 expected and observed accepted prices; zero missing/extra prices after one worker was killed and restarted; JobManager logs confirmed checkpoint restoration |
| Replicated Kafka | Six output partitions, replication factor 3, minimum ISR 2; exact EMA integration check passed |
| Broker failure | Same integration check passed with one of three brokers stopped; final Flink image also passed against the remaining two brokers |
| Monitoring | JobManager, both TaskManagers, and Kafka scrape targets healthy; Grafana dashboard available; Flink log streams queried successfully through Grafana/Loki |
| Dummy emitter / Kafka | 300 clean records acknowledged; a separate 2000-record quality run acknowledged all records and included all six malformed/tombstone fault types |
| Injection trace | Captured values and trace row counts matched; duplicate copies shared origin IDs and payload hashes |
| Emitter scheduler | Dry run generated 20000 clean originals in about 1 second at a 20000/s target; this excludes Kafka and is not a pipeline throughput claim |
| Configuration | Base/cluster/monitoring Compose merged successfully; shell entrypoint, Python files, and JSON configs validated |

The nine-run raw matrix and per-run JSON/logs are in the local ignored `benchmarks/results/` directory, with session prefix `be487420`. The final baseline uses session prefix `e9316030`, including its manifest. `recovery-check.json` contains the recovery workload result. The report generator produces a grouped Markdown comparison. Results remain local and are not added to Git. Temporary validation containers and their generated Kafka/Flink/monitoring volumes were removed after testing.

This host's Docker storage filesystem exceeded Loki's default 90% disk-use guard during testing. Log ingestion was then verified using a 97% threshold **only in the isolated test deployment**. The shipped default remains 90%; provision sufficient Docker storage or deliberately configure `LOKI_WAL_DISK_FULL_THRESHOLD` for the deployment. The WAL uses the persistent Loki volume, and a disk-pressure alert is included.

The benchmark does not assert identical late classifications across asynchronous failure schedules. Exact EMA/policy checks use controlled operator tests and the deterministic integration verifier. JobManager HA and orchestration across physical hosts are not provided by the local Compose examples; shared checkpoint storage and reachable broker addresses are required when distributing containers across hosts.
