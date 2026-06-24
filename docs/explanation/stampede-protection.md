# Stampede Protection

When many requests ask for the same expired or missing key at once, `Async Hybrid Cache` uses one async lock per key.

```mermaid
sequenceDiagram
    participant R1 as Request 1
    participant R2 as Request 2
    participant Cache
    participant Work as Async function

    R1->>Cache: read key
    Cache->>Work: refresh value
    R2->>Cache: read same key
    Cache-->>R2: wait on key lock
    Work-->>Cache: value
    Cache-->>R1: value
    Cache-->>R2: refreshed value
```

The first request for that key refreshes the value. Other requests wait for the same refresh instead of running duplicate work. After the refresh completes, waiting requests read the refreshed value.

This is most useful for expensive calls such as API requests, database queries, or computed responses where many concurrent requests can target the same key.
