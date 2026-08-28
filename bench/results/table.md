### Concurrency 64 (median of 3 runs)

| Contender | RPS | p50 | p90 | p99 | p99.9 |
|---|---:|---:|---:|---:|---:|
| ramjet-ingress | 83,369 | 651 us | 930 us | 2,808 us | 10,112 us |
| nginx | 80,397 | 689 us | 944 us | 3,158 us | 10,186 us |
| baseline (no proxy) | 237,444 | 226 us | 357 us | 982 us | 3,867 us |

### Concurrency 256 (single run)

| Contender | RPS | p50 | p90 | p99 | p99.9 |
|---|---:|---:|---:|---:|---:|
| ramjet-ingress | 83,224 | 2,832 us | 3,793 us | 7,242 us | 21,303 us |
| nginx | 82,748 | 2,715 us | 3,919 us | 9,924 us | 32,915 us |
| baseline (no proxy) | 242,170 | 910 us | 1,336 us | 3,595 us | 11,761 us |

### Run-to-run spread (c64 RPS, every run)

| Contender | run 1 | run 2 | run 3 | spread |
|---|---|---|---|---|
| ramjet-ingress | 83,369 | 82,032 | 88,134 | 7.2% |
| nginx | 74,237 | 80,397 | 86,674 | 15.5% |
| baseline (no proxy) | 238,880 | 237,444 | 231,236 | 3.2% |

### Added latency of the proxy hop (c64, vs baseline)

| Contender | added p50 | added p99 | RPS vs baseline |
|---|---:|---:|---:|
| ramjet-ingress | +425 us | +1,826 us | 35% |
| nginx | +463 us | +2,176 us | 34% |

