### Concurrency 64 (median of 3 runs)

| Contender | RPS | p50 | p90 | p99 | p99.9 |
|---|---:|---:|---:|---:|---:|
| ramjet-ingress | 85,908 | 666 us | 921 us | 2,528 us | 6,236 us |
| nginx | 86,670 | 671 us | 873 us | 2,314 us | 5,902 us |
| baseline (no proxy) | 229,400 | 223 us | 356 us | 1,219 us | 4,421 us |

### Concurrency 256 (single run)

| Contender | RPS | p50 | p90 | p99 | p99.9 |
|---|---:|---:|---:|---:|---:|
| ramjet-ingress | 82,524 | 2,975 us | 3,617 us | 6,396 us | 14,185 us |
| nginx | 89,636 | 2,652 us | 3,559 us | 7,683 us | 17,180 us |
| baseline (no proxy) | 247,077 | 918 us | 1,233 us | 3,554 us | 9,111 us |

### Run-to-run spread (c64 RPS, every run)

| Contender | run 1 | run 2 | run 3 | spread |
|---|---|---|---|---|
| ramjet-ingress | 86,551 | 85,121 | 85,908 | 1.7% |
| nginx | 84,124 | 86,670 | 88,048 | 4.5% |
| baseline (no proxy) | 229,400 | 228,659 | 242,940 | 6.1% |

### Added latency of the proxy hop (c64, vs baseline)

| Contender | added p50 | added p99 | RPS vs baseline |
|---|---:|---:|---:|
| ramjet-ingress | +443 us | +1,308 us | 37% |
| nginx | +448 us | +1,095 us | 38% |

