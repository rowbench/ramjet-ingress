
<!-- b1 -->

### Throughput and latency under churn (oha, c64, 2 runs per cell)

| Contender | Arm | RPS (median of runs) | vs own baseline | p50 | p99 | p99.9 | HTTP errors |
|---|---|---:|---:|---:|---:|---:|---:|
| ramjet | baseline | 104,844 | — | 0.35 ms | 3.8 ms | 12.5 ms | 0 |
| ramjet | spec | 107,536 | +2.6% | 0.35 ms | 3.6 ms | 10.3 ms | 0 |
| ramjet | endpoint | 103,298 | -1.5% | 0.35 ms | 3.8 ms | 12.7 ms | 0 |
| nginx | baseline | 87,865 | — | 0.43 ms | 4.6 ms | 13.0 ms | 0 |
| nginx | spec | 78,368 | -10.8% | 0.45 ms | 5.4 ms | 18.3 ms | 1722 |
| nginx | endpoint | 64,305 | -26.8% | 0.57 ms | 6.7 ms | 21.1 ms | 0 |

### Idle keep-alive connections that survived the window

| Contender | Arm | Held | Survived | Lost | Config events the controller applied |
|---|---|---:|---:|---:|---:|
| ramjet | baseline | 100 | 100 | 0 | 0 |
| ramjet | spec | 100 | 100 | 0 | 98 |
| ramjet | endpoint | 100 | 100 | 0 | 98 |
| nginx | baseline | 100 | 100 | 0 | 0 |
| nginx | spec | 100 | 0 | 100 | 66 |
| nginx | endpoint | 100 | 100 | 0 | 0 |

### The single-connection timeline (every request, not a percentile)

| Contender | Arm | Requests | Errors | p50 | p99 | p99.9 | Worst single request |
|---|---|---:|---:|---:|---:|---:|---:|
| ramjet | baseline | 736,328 | 0 | 0.18 ms | 2.3 ms | 7.7 ms | 1,049 ms |
| ramjet | spec | 747,263 | 0 | 0.18 ms | 2.2 ms | 6.9 ms | 149 ms |
| ramjet | endpoint | 753,277 | 0 | 0.17 ms | 2.3 ms | 7.8 ms | 162 ms |
| nginx | baseline | 483,172 | 0 | 0.25 ms | 3.7 ms | 9.7 ms | 347 ms |
| nginx | spec | 470,240 | 28 | 0.22 ms | 3.9 ms | 12.1 ms | 499 ms |
| nginx | endpoint | 370,031 | 0 | 0.31 ms | 5.1 ms | 14.8 ms | 167 ms |

### Visible stalls in the sequential stream

| Contender | Arm | Requests | > 50 ms | > 200 ms | > 1 s | Wall time inside a > 50 ms request | Median gap between stalls |
|---|---|---:|---:|---:|---:|---:|---:|
| ramjet | baseline | 736,328 | 18 | 1 | 1 | 2.3 s of 240 s (1.0%) | 0.92 s |
| ramjet | spec | 747,263 | 13 | 0 | 0 | 1.2 s of 240 s (0.5%) | 9.88 s |
| ramjet | endpoint | 753,277 | 19 | 0 | 0 | 1.5 s of 240 s (0.6%) | 7.63 s |
| nginx | baseline | 483,172 | 11 | 1 | 0 | 1.2 s of 240 s (0.5%) | 1.25 s |
| nginx | spec | 470,240 | 35 | 9 | 0 | 4.8 s of 240 s (2.0%) | 0.51 s |
| nginx | endpoint | 370,031 | 16 | 0 | 0 | 1.2 s of 240 s (0.5%) | 5.53 s |

### Controller cost of the churn window

| Contender | Arm | Pod CPU-seconds | Pod CPU per request | vs own baseline | Pod memory at end |
|---|---|---:|---:|---:|---:|
| ramjet | baseline | 300.9 s | 28.7 us | — | 17.9 MiB |
| ramjet | spec | 310.1 s | 28.8 us | +0% | 18.3 MiB |
| ramjet | endpoint | 302.3 s | 29.3 us | +2% | 16.5 MiB |
| nginx | baseline | 402.8 s | 45.8 us | — | 128.1 MiB |
| nginx | spec | 395.5 s | 50.5 us | +10% | 115.7 MiB |
| nginx | endpoint | 367.7 s | 57.2 us | +25% | 127.8 MiB |

<!-- b1-contended -->

### Throughput and latency under churn (oha, c64, 2 runs per cell)

| Contender | Arm | RPS (median of runs) | vs own baseline | p50 | p99 | p99.9 | HTTP errors |
|---|---|---:|---:|---:|---:|---:|---:|
| ramjet | baseline | 95,581 | — | 0.37 ms | 4.3 ms | 14.8 ms | 0 |
| ramjet | spec | 36,320 | -62.0% | 0.82 ms | 13.7 ms | 48.7 ms | 0 |
| ramjet | endpoint | 62,219 | -34.9% | 0.71 ms | 10.9 ms | 38.0 ms | 0 |
| nginx | baseline | 71,257 | — | 0.54 ms | 8.4 ms | 27.2 ms | 0 |
| nginx | spec | 48,064 | -32.5% | 0.75 ms | 13.1 ms | 50.3 ms | 1772 |
| nginx | endpoint | 39,013 | -45.3% | 0.82 ms | 14.7 ms | 52.7 ms | 0 |

### Idle keep-alive connections that survived the window

| Contender | Arm | Held | Survived | Lost | Config events the controller applied |
|---|---|---:|---:|---:|---:|
| ramjet | baseline | 100 | 100 | 0 | 0 |
| ramjet | spec | 100 | 100 | 0 | 91 |
| ramjet | endpoint | 100 | 100 | 0 | 96 |
| nginx | baseline | 100 | 100 | 0 | 0 |
| nginx | spec | 100 | 0 | 100 | 67 |
| nginx | endpoint | 100 | 100 | 0 | 0 |

### The single-connection timeline (every request, not a percentile)

| Contender | Arm | Requests | Errors | p50 | p99 | p99.9 | Worst single request |
|---|---|---:|---:|---:|---:|---:|---:|
| ramjet | baseline | 625,463 | 0 | 0.19 ms | 3.0 ms | 8.9 ms | 223 ms |
| ramjet | spec | 252,871 | 0 | 0.28 ms | 9.2 ms | 29.4 ms | 2,681 ms |
| ramjet | endpoint | 458,554 | 0 | 0.22 ms | 5.5 ms | 20.9 ms | 1,057 ms |
| nginx | baseline | 412,024 | 0 | 0.24 ms | 5.9 ms | 20.4 ms | 217 ms |
| nginx | spec | 328,590 | 26 | 0.21 ms | 7.7 ms | 30.0 ms | 793 ms |
| nginx | endpoint | 226,923 | 0 | 0.39 ms | 10.2 ms | 36.1 ms | 505 ms |

### Visible stalls in the sequential stream

| Contender | Arm | Requests | > 50 ms | > 200 ms | > 1 s | Wall time inside a > 50 ms request | Median gap between stalls |
|---|---|---:|---:|---:|---:|---:|---:|
| ramjet | baseline | 625,463 | 16 | 1 | 0 | 1.4 s of 240 s (0.6%) | 2.59 s |
| ramjet | spec | 252,871 | 102 | 8 | 2 | 13.5 s of 240 s (5.6%) | 0.55 s |
| ramjet | endpoint | 458,554 | 81 | 4 | 1 | 8.7 s of 240 s (3.6%) | 0.54 s |
| nginx | baseline | 412,024 | 58 | 1 | 0 | 4.7 s of 240 s (2.0%) | 1.05 s |
| nginx | spec | 328,590 | 129 | 6 | 0 | 12.3 s of 240 s (5.1%) | 0.57 s |
| nginx | endpoint | 226,923 | 134 | 4 | 0 | 11.5 s of 240 s (4.8%) | 0.72 s |

### Controller cost of the churn window

| Contender | Arm | Pod CPU-seconds | Pod CPU per request | vs own baseline | Pod memory at end |
|---|---|---:|---:|---:|---:|
| ramjet | baseline | 290.1 s | 30.3 us | — | 18.8 MiB |
| ramjet | spec | 142.9 s | 39.3 us | +30% | 18.6 MiB |
| ramjet | endpoint | 208.3 s | 33.5 us | +10% | 22.9 MiB |
| nginx | baseline | 315.5 s | 44.3 us | — | 148.2 MiB |
| nginx | spec | 301.6 s | 62.8 us | +42% | 137.2 MiB |
| nginx | endpoint | 254.3 s | 65.2 us | +47% | 150.5 MiB |

<!-- b2 -->

### Apply -> served, no other load (milliseconds)

| Contender | Change | Trials | Median | p95 | Min | Max | Median `kubectl apply` |
|---|---|---:|---:|---:|---:|---:|---:|
| ramjet | new Ingress | 10 | 363 | 566 | 324 | 566 | 159 |
| ramjet | backend swap | 10 | 354 | 556 | 322 | 556 | 150 |
| nginx | new Ingress | 10 | 1,151 | 3,638 | 302 | 3,638 | 138 |
| nginx | backend swap | 10 | 459 | 3,657 | 384 | 3,657 | 188 |

<!-- b3 -->

### Loading the routes

| Contender | Ingresses created | `kubectl apply` wall time | Apply -> last route served | Controller CPU | Controller memory before -> after | nginx reloads |
|---|---:|---:|---:|---:|---:|---:|
| ramjet | 500/500 | 10.7 s | 10.9 s | 1 s | 21.1 -> 20.8 MiB | — |
| nginx | 500/500 | 58.5 s | 61.7 s | 67 s | 115.8 -> 214.0 MiB | 19 |

### Propagation of a new Ingress with the routes already loaded

| Contender | Trials | Median | p95 | Min | Max | Median with an empty cluster (benchmark 2) |
|---|---:|---:|---:|---:|---:|---:|
| ramjet | 5 | 507 ms | 723 ms | 414 ms | 723 ms | 363 ms |
| nginx | 5 | 5,006 ms | 5,964 ms | 3,374 ms | 5,964 ms | 1,151 ms |

### Forwarding on the stable route with the routes loaded (c64, 30s)

| Contender | RPS | p50 | p99 |
|---|---:|---:|---:|
| ramjet | 47,535 | 0.69 ms | 9.2 ms |
| nginx | 16,109 | 1.26 ms | 35.8 ms |

<!-- b4 -->

### Container memory across 10,000 idle keep-alive connections

| Contender | Pass | Established | Idle before | At 10k | After close | Per connection | Retained |
|---|---|---:|---:|---:|---:|---:|---:|
| ramjet | 1 | 10,000/10,000 | 1.5 MiB | 266.1 MiB | 229.6 MiB | 27.1 KiB | +228.1 MiB |
| ramjet | 2 | 10,000/10,000 | 229.6 MiB | 329.0 MiB | 292.3 MiB | 10.2 KiB | +62.7 MiB |
| nginx | 1 | 10,000/10,000 | 16.3 MiB | 58.9 MiB | 16.5 MiB | 4.4 KiB | +0.2 MiB |
| nginx | 2 | 10,000/10,000 | 16.5 MiB | 58.8 MiB | 16.5 MiB | 4.3 KiB | +0.0 MiB |
