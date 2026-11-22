---
title: Summary
weight: 3
---

A Summary samples observations and tracks the total count and sum. Use it when
you want to track the size or duration of events and compute averages. It can
also estimate quantiles (p50/p95/p99) locally over a sliding time window; if
you need quantiles that are aggregatable across instances in your Prometheus
queries, use a [Histogram](../histogram/) instead.

```python
from prometheus_client import Summary
s = Summary('request_latency_seconds', 'Description of summary')
s.observe(4.7)    # Observe 4.7 (seconds in this case)
```

A Summary exposes two time series per metric:
- `<name>_count` — total number of observations
- `<name>_sum` — sum of all observed values
- `<name>{quantile="..."}` — estimated quantiles, only when `objectives` is set

## Constructor

```python
Summary(name, documentation, labelnames=(), namespace='', subsystem='', unit='', registry=REGISTRY, objectives=None, max_age=600.0, age_buckets=5, checkpoint_dir=None)
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `name` | `str` | required | Metric name. |
| `documentation` | `str` | required | Help text shown in the `/metrics` output and Prometheus UI. |
| `labelnames` | `Iterable[str]` | `()` | Names of labels for this metric. See [Labels](../labels/). Note: `quantile` is reserved and cannot be used as a label name. |
| `namespace` | `str` | `''` | Optional prefix. |
| `subsystem` | `str` | `''` | Optional middle component. |
| `unit` | `str` | `''` | Optional unit suffix appended to the metric name. |
| `registry` | `CollectorRegistry` | `REGISTRY` | Registry to register with. Pass `None` to skip registration, which is useful in tests where you create metrics without wanting them in the global registry. |
| `objectives` | `Dict[float, float]` | `None` | Quantiles to estimate, mapping quantile to tolerated absolute error, e.g. `{0.5: 0.05, 0.9: 0.01, 0.99: 0.001}`. If `None`, no quantiles are computed. |
| `max_age` | `float` | `600.0` | Length in seconds of the sliding time window over which quantiles are estimated. |
| `age_buckets` | `int` | `5` | Number of buckets the window is split into. Observations expire one bucket (`max_age / age_buckets` seconds) at a time. |
| `checkpoint_dir` | `str` | `None` | Directory used to persist the compressed quantile estimator state, so quantiles resume across process restarts. Defaults to the `PROMETHEUS_SUMMARY_CHECKPOINT_DIR` environment variable when set. |

## Quantiles

With `objectives` set, the Summary estimates the given quantiles over a
sliding window of `max_age` seconds, using a streaming estimator with bounded
error and memory. Raw observations are never stored.

```python
s = Summary('request_latency_seconds', 'Request latency (seconds)',
            objectives={0.5: 0.05, 0.9: 0.01, 0.99: 0.001})
```

Quantile estimation is per-process and is not aggregated in multiprocess mode.

### Resuming quantiles after a restart

By default the estimator lives in process memory, so a restart starts the
window from scratch and quantiles are misleading until the window refills (up
to `max_age` seconds). Set `checkpoint_dir` (or the
`PROMETHEUS_SUMMARY_CHECKPOINT_DIR` environment variable) to checkpoint the
compressed estimator state to disk. The state is written on every bucket
rotation and at process exit, and loaded back on startup; buckets older than
`max_age` are discarded, and a checkpoint whose `objectives`, `max_age` or
`age_buckets` no longer match is ignored. Only the compressed estimator state
is persisted, never raw observations.

```python
s = Summary('request_latency_seconds', 'Request latency (seconds)',
            objectives={0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
            checkpoint_dir='/var/run/myapp/metrics')
```

Call `s.flush()` before a controlled shutdown to bound state loss to less
than one bucket interval.

`namespace`, `subsystem`, and `name` are joined with underscores to form the full metric name:

```python
# namespace='myapp', subsystem='worker', name='task_duration_seconds'
# produces: myapp_worker_task_duration_seconds
Summary('task_duration_seconds', 'Task duration', namespace='myapp', subsystem='worker')
```

## Methods

### `observe(amount)`

Record a single observation. The amount is typically positive or zero.

```python
s.observe(0.43)   # observe 430ms
s.observe(1024)   # observe 1024 bytes
```

### `time()`

Observe the duration in seconds of a block of code or function and add it to the
summary. Every call accumulates — unlike `Gauge.time()`, which only keeps the
most recent duration. Can be used as a decorator or context manager.

```python
@s.time()
def process():
    pass

with s.time():
    pass

with s.time() as t:
    pass
print(t.duration) # observed time in seconds.
```

## Labels

See [Labels](../labels/) for how to use `.labels()`, `.remove()`, `.remove_by_labels()`, and `.clear()`.

## Real-world example

Tracking the duration of background tasks:

```python
from prometheus_client import Summary, start_http_server

TASK_DURATION = Summary(
    'task_duration_seconds',
    'Time spent processing background tasks',
    labelnames=['task_type'],
    namespace='myapp',
)

def run_task(task_type, task):
    with TASK_DURATION.labels(task_type=task_type).time():
        # ... run the task ...
        pass

if __name__ == '__main__':
    start_http_server(8000)  # exposes metrics at http://localhost:8000/metrics
    # ... start your application ...
```

This produces:
```
myapp_task_duration_seconds_count{task_type="email"} 120
myapp_task_duration_seconds_sum{task_type="email"} 48.3
```

You can compute the average duration in PromQL as:
```
rate(myapp_task_duration_seconds_sum[5m]) / rate(myapp_task_duration_seconds_count[5m])
```
