### Concurrency 64 (median of 3 runs)

| Contender | RPS | p50 | p90 | p99 | p99.9 |
|---|---:|---:|---:|---:|---:|
| ramjet (hyper) | 80,682 | 687 us | 1,030 us | 2,905 us | 7,562 us |
| ramjet (uring) | 116,927 | 483 us | 702 us | 1,941 us | 5,569 us |
| nginx | 80,790 | 696 us | 1,002 us | 2,853 us | 7,687 us |
| baseline (no proxy) | 229,902 | 227 us | 384 us | 1,160 us | 3,868 us |

### Concurrency 256 (single run)

| Contender | RPS | p50 | p90 | p99 | p99.9 |
|---|---:|---:|---:|---:|---:|
| ramjet (hyper) | 65,458 | 3,130 us | 5,217 us | 15,918 us | 52,723 us |
| ramjet (uring) | 110,057 | 1,962 us | 3,070 us | 8,122 us | 28,387 us |
| nginx | 84,480 | 2,680 us | 3,936 us | 8,465 us | 22,245 us |
| baseline (no proxy) | 231,837 | 924 us | 1,552 us | 3,866 us | 10,629 us |

### Run-to-run spread (c64 RPS, every run)

| Contender | run 1 | run 2 | run 3 | spread |
|---|---|---|---|---|
| ramjet (hyper) | 63,672 | 85,640 | 80,682 | 28.7% |
| ramjet (uring) | 117,553 | 116,927 | 111,250 | 5.5% |
| nginx | 84,862 | 79,688 | 80,790 | 6.3% |
| baseline (no proxy) | 229,902 | 199,099 | 232,428 | 15.1% |

### Added latency of the proxy hop (c64, vs baseline)

| Contender | added p50 | added p99 | RPS vs baseline |
|---|---:|---:|---:|
| ramjet (hyper) | +459 us | +1,746 us | 35% |
| ramjet (uring) | +255 us | +782 us | 51% |
| nginx | +469 us | +1,693 us | 35% |

### Engine to engine, and against nginx

| Concurrency | uring vs hyper | uring vs nginx | verdict |
|---|---:|---:|---|
| c64 | +44.9% | +44.7% | uring ahead |
| c256 | +68.1% | +30.3% | single run, not median |

### Was the machine steady?

Baseline (no proxy) across the 3 rounds: 229,902, 199,099, 232,428 rps — 15.1% spread.

**The baseline moved by more than 15%, so this run measured the machine as much as the contenders.** Treat every number above as indicative only; a difference smaller than this drift cannot be attributed to anything under test.


<!-- THIS RUN FAILED A TRUST CHECK -->
<!-- baseline drift 15.1% across rounds: [229902.09370663448, 199098.9150086352, 232427.71216337068] -->
