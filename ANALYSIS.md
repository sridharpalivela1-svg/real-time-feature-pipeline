# Real-Time Feature Engineering Analysis

## Batch vs. Streaming Divergence
*   **Windowing:** Streaming uses Event-Time windows which may differ from batch "processing time" if data arrives late.
*   **State:** Streaming maintains incremental state, whereas batch recalculates from scratch.
*   **Consistency:** Late-arriving data is handled by watermarks in Flink, but might be missing in a naive batch job.

## Late Event Handling
*   **Strategy:** We used `BoundedOutOfOrderness(30s)`.
*   **Evidence:** The Producer sends 5% late events (35-90s old). Events within the 30s buffer are included; events older than that are dropped (visible in dashboard "Late Events" counter).
