# Phase 1 Corridor Scope

The initial data extraction targets the `demo_corridor` corridor composed of
stations `S1`, `S2`, and `S3`. Events for service date `2024-01-01` were
filtered so each train retains at least two consecutive corridor stations.
The cleaned slice has been stored under:

```
artifacts/demo_corridor/2024-01-01/events_clean.parquet
```

This dataset serves as the starting point for downstream modelling and
simulation efforts.