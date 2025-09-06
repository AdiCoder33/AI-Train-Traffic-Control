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

## Time Handling

All timestamps are normalised to **UTC** during ingestion to avoid daylight
saving or locale ambiguities.  User-facing tools render these times in
**Indian Standard Time (UTC+05:30)** so analysts work with familiar local
timestamps while the core pipeline keeps a single, unambiguous reference.

## Station Mapping

Raw event files often reference stations by name.  During normalisation
each unique name is assigned a deterministic identifier (e.g. ``S0001``)
persisted in ``station_map.csv``.  The helper function automatically creates
the file on first run and appends new stations as they are encountered,
ensuring consistent IDs across datasets and sessions.

## Run-Time and Headway Metrics

Edge attributes for the corridor graph are derived from the schedule:

* **Run-time median** – for every pair of consecutive stations the scheduled
  departure of the upstream station and the arrival at the downstream
  station are differenced; the median of these values forms the baseline
  minimum run time.
* **Headway p90** – scheduled departure times are ordered per direction and
  successive gaps are computed.  The 90th percentile of these gaps becomes
  the planning headway, providing a robust buffer against outliers.

## Current Limitations

The present slice and models make several simplifying assumptions:

* Single track and a single platform per station; no explicit support for
  overtakes or multi-track sections.
* Weather effects and other exogenous disruptions are not yet integrated
  into run-time or headway estimation.
* Metrics rely solely on scheduled times when actual data is missing.
* Only a minimal corridor (`demo_corridor`) and one service day are covered
  so far.

These constraints will be relaxed in subsequent development phases.
