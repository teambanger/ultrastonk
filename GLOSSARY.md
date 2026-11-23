# Ultrastonk

Ultrastonk processes market events into prices and indicators and delivers them to subscribers. Its experiments measure how that pipeline behaves under controlled workloads.

## Experiment Language

**Sustainable limit**:
The highest tested offered load at which the measured boundary maintains correct delivery without a growing backlog for the stated workload and resources. Tail latency is reported separately; eventual delivery during drain does not establish sustainability.
_Avoid_: Peak throughput

**Module experiment**:
A controlled run whose measurement boundary isolates one module, so the result describes that module under the stated input and output conditions.

**End-to-end experiment**:
A controlled run measured from publication at the pipeline input to receipt by a WebSocket subscriber.

**Historical replay**:
A run driven by recorded market data to observe behavior under historical event patterns. Its results are distinct from synthetic load sweeps.
