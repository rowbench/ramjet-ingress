### Concurrency 64 (median of 3 runs)

| Contender | RPS | p50 | p90 | p99 | p99.9 |
|---|---:|---:|---:|---:|---:|
| ramjet-ingress | 61,568 | 967 us | 1,353 us | 3,107 us | 8,468 us |
| nginx | 89,593 | 664 us | 876 us | 1,822 us | 6,173 us |
| baseline (no proxy) | 247,875 | 216 us | 309 us | 915 us | 3,656 us |

### Concurrency 256 (single run)

| Contender | RPS | p50 | p90 | p99 | p99.9 |
|---|---:|---:|---:|---:|---:|
| ramjet-ingress | 59,644 | 4,027 us | 5,730 us | 11,930 us | 28,298 us |
| nginx | 88,867 | 2,544 us | 3,702 us | 8,233 us | 21,370 us |
| baseline (no proxy) | 262,377 | 897 us | 1,179 us | 2,427 us | 5,504 us |

### Run-to-run spread (c64 RPS, every run)

| Contender | run 1 | run 2 | run 3 | spread |
|---|---|---|---|---|
| ramjet-ingress | 61,568 | 61,236 | 64,531 | 5.3% |
| nginx | 89,593 | 89,804 | 88,246 | 1.7% |
| baseline (no proxy) | 247,875 | 219,715 | 251,504 | 13.3% |

### Added latency of the proxy hop (c64, vs baseline)

| Contender | added p50 | added p99 | RPS vs baseline |
|---|---:|---:|---:|
| ramjet-ingress | +752 us | +2,192 us | 25% |
| nginx | +449 us | +907 us | 36% |

