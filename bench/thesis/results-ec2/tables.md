
#### Steady-state forwarding, c64 (3 x 30 s per contender, interleaved)

| Contender | RPS (median) | spread | % of baseline | p50 | p99 | Controller CPU per request | Controller memory | mean steal |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| backend, no proxy | 22,234 | 3.0% | 100.0% | 2.11 ms | 13.64 ms | — | — | 0.72% |
| ramjet (hyper) | 10,858 | 1.8% | 48.8% | 4.95 ms | 21.11 ms | 100 us | 10.5 MiB | 0.41% |
| ramjet (uring) | 12,355 | 1.2% | 55.6% | 3.81 ms | 22.05 ms | 69 us | 15.2 MiB | 0.34% |
| ingress-nginx | 7,971 | 2.7% | 35.9% | 7.06 ms | 24.90 ms | 187 us | 67.6 MiB | 0.09% |

Non-2xx plus transport errors across every window: backend, no proxy 0, ramjet (hyper) 0, ramjet (uring) 0, ingress-nginx 0.

#### The claim that survives the drift

| Comparison | worst round of the first | best round of the second | verdict |
|---|---:|---:|---|
| ramjet (hyper) vs ingress-nginx | 10,689 | 8,078 | ranges disjoint, ramjet (hyper) ahead by 32% at worst |
| ramjet (uring) vs ingress-nginx | 12,299 | 8,078 | ranges disjoint, ramjet (uring) ahead by 52% at worst |
| ramjet (uring) vs ramjet (hyper) | 12,299 | 10,880 | ranges disjoint, ramjet (uring) ahead by 13% at worst |

#### Steady-state forwarding, c256 (1 x 30 s per contender, interleaved)

| Contender | RPS (median) | spread | % of baseline | p50 | p99 | Controller CPU per request | Controller memory | mean steal |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| backend, no proxy | 22,722 | 0.0% | 100.0% | 9.51 ms | 42.32 ms | — | — | 0.09% |
| ramjet (hyper) | 11,705 | 0.0% | 51.5% | 18.56 ms | 78.61 ms | 100 us | 31.2 MiB | 0.06% |
| ramjet (uring) | 13,490 | 0.0% | 59.4% | 14.62 ms | 77.83 ms | 69 us | 31.7 MiB | 0.53% |
| ingress-nginx | 8,027 | 0.0% | 35.3% | 29.81 ms | 78.47 ms | 188 us | 70.5 MiB | 0.00% |

Non-2xx plus transport errors across every window: backend, no proxy 0, ramjet (hyper) 0, ramjet (uring) 0, ingress-nginx 0.

#### Throughput and latency under churn (oha, c64, 2 runs per cell)

| Contender | Arm | RPS (median) | vs own baseline | p50 | p99 | p99.9 | HTTP errors | mean steal |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| ramjet (hyper) | baseline | 10,799 | — | 5.00 ms | 21.08 ms | 31.4 ms | 0 | 0.24% |
| ramjet (hyper) | spec | 10,232 | -5.2% | 5.22 ms | 22.73 ms | 35.0 ms | 0 | 0.12% |
| ramjet (hyper) | endpoint | 10,093 | -6.5% | 5.25 ms | 23.46 ms | 37.4 ms | 0 | 0.22% |
| ingress-nginx | baseline | 8,089 | — | 6.82 ms | 25.50 ms | 37.9 ms | 0 | 0.04% |
| ingress-nginx | spec | 7,226 | -10.7% | 7.75 ms | 28.87 ms | 48.6 ms | 1829 | 0.04% |
| ingress-nginx | endpoint | 7,578 | -6.3% | 7.15 ms | 29.97 ms | 49.8 ms | 0 | 0.03% |

#### Idle keep-alive connections that survived the window

| Contender | Arm | Held | Survived | Lost | Config events the controller applied |
|---|---|---:|---:|---:|---:|
| ramjet (hyper) | baseline | 100 | 100 | 0 | 0 |
| ramjet (hyper) | spec | 100 | 100 | 0 | 89 |
| ramjet (hyper) | endpoint | 100 | 100 | 0 | 92 |
| ingress-nginx | baseline | 100 | 100 | 0 | 0 |
| ingress-nginx | spec | 100 | 0 | 100 | 66 |
| ingress-nginx | endpoint | 100 | 100 | 0 | 0 |

#### Controller cost of the churn window

| Contender | Arm | Pod CPU-seconds | Pod CPU per request | vs own baseline | Pod memory at end |
|---|---|---:|---:|---:|---:|
| ramjet (hyper) | baseline | 116.0 s | 107.5 us | — | 12.1 MiB |
| ramjet (hyper) | spec | 111.1 s | 108.6 us | +1% | 11.6 MiB |
| ramjet (hyper) | endpoint | 109.9 s | 108.9 us | +1% | 11.5 MiB |
| ingress-nginx | baseline | 156.0 s | 192.9 us | — | 68.9 MiB |
| ingress-nginx | spec | 156.5 s | 216.8 us | +12% | 65.7 MiB |
| ingress-nginx | endpoint | 147.1 s | 194.1 us | +1% | 69.6 MiB |

#### The single-connection timeline (every request, not a percentile)

| Contender | Arm | Requests | Errors | p50 | p99 | > 50 ms | > 200 ms | > 1 s | Worst single request |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| ramjet (hyper) | baseline | 130,797 | 0 | 0.41 ms | 15.17 ms | 4 | 0 | 0 | 56 ms |
| ramjet (hyper) | spec | 131,986 | 0 | 0.40 ms | 15.94 ms | 6 | 0 | 0 | 86 ms |
| ramjet (hyper) | endpoint | 127,958 | 0 | 0.42 ms | 16.33 ms | 7 | 0 | 0 | 69 ms |
| ingress-nginx | baseline | 111,349 | 0 | 0.45 ms | 18.91 ms | 6 | 0 | 0 | 105 ms |
| ingress-nginx | spec | 110,139 | 31 | 0.45 ms | 19.63 ms | 20 | 0 | 0 | 90 ms |
| ingress-nginx | endpoint | 106,702 | 0 | 0.47 ms | 20.79 ms | 27 | 0 | 0 | 127 ms |

#### Apply -> served, new Ingress, no other load (milliseconds)

| Contender | Trials | Median | p95 | Min | Max | Median `kubectl apply` |
|---|---:|---:|---:|---:|---:|---:|
| ramjet (hyper) | 10 | 384 | 426 | 372 | 426 | 189 |
| ramjet (uring) | 10 | 372 | 394 | 370 | 394 | 186 |
| ingress-nginx | 10 | 2,762 | 2,813 | 398 | 2,813 | 191 |
