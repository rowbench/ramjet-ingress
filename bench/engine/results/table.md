### Concurrency 64 (median of 3 runs)

| Contender | RPS | p50 | p90 | p99 | p99.9 |
|---|---:|---:|---:|---:|---:|
| ramjet (hyper) | 64,385 | 662 us | 1,593 us | 5,947 us | 20,394 us |
| ramjet (uring) | 83,196 | 519 us | 1,308 us | 4,444 us | 17,324 us |
| nginx | 74,486 | 665 us | 1,128 us | 4,248 us | 18,003 us |
| baseline (no proxy) | 186,787 | 219 us | 497 us | 2,659 us | 7,305 us |

### Concurrency 256 (single run)

| Contender | RPS | p50 | p90 | p99 | p99.9 |
|---|---:|---:|---:|---:|---:|
| ramjet (hyper) | 52,886 | 3,288 us | 8,090 us | 26,030 us | 103,405 us |
| ramjet (uring) | 55,440 | 2,313 us | 8,002 us | 38,572 us | 138,925 us |
| nginx | 74,314 | 2,519 us | 5,242 us | 17,577 us | 57,466 us |
| baseline (no proxy) | 149,338 | 1,001 us | 3,195 us | 8,710 us | 55,334 us |

### Run-to-run spread (c64 RPS, every run)

| Contender | run 1 | run 2 | run 3 | spread |
|---|---|---|---|---|
| ramjet (hyper) | 72,116 | 64,385 | 51,523 | 32.9% |
| ramjet (uring) | 116,568 | 83,196 | 77,401 | 42.4% |
| nginx | 76,328 | 50,121 | 74,486 | 39.1% |
| baseline (no proxy) | 209,359 | 186,787 | 161,632 | 25.7% |

### Added latency of the proxy hop (c64, vs baseline)

| Contender | added p50 | added p99 | RPS vs baseline |
|---|---:|---:|---:|
| ramjet (hyper) | +443 us | +3,288 us | 34% |
| ramjet (uring) | +300 us | +1,785 us | 45% |
| nginx | +446 us | +1,589 us | 40% |

### Engine to engine, and against nginx

| Concurrency | uring vs hyper | uring vs nginx | verdict |
|---|---:|---:|---|
| c64 | +29.2% | +11.7% | inside the noise (42.4% spread) |
| c256 | +4.8% | -25.4% | single run, not median |

