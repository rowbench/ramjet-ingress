# ramjet-top

A terminal cockpit for a running ramjet-ingress — `htop` for the data plane.

The admin port already tells you everything: `/admin/routes` has per-route
counters, `/admin/generations` has the configuration history, `/metrics` has the
Prometheus page. All three are fine to `curl`, and none of them answers the
question you actually have while you are asking it, which is *"what is happening
right now, and did the config I just pushed make it worse?"*

Counters do not answer that. Rates do. `ramjet-top` polls, differences the
counters into rates, and draws them.

```
╭ ramjet-top ─ http://127.0.0.1:10254 ───────────────────────────────────────────────────────╮
│gen 0  routes 5  gens 1  conns 0                                   rps · last 6 polls · peak 420│
│rps 103.9  5xx 0.00%  upstream 0.6  up 5s          █                                        │
│                                                   █▃▄▄▄▃                                   │
╰────────────────────────────────────────────────────────────────────────────────────────────╯
╭ routes 5 ──────────────────────────────────────────────────────────────────────────────────╮
│HOST                   PATH          TYPE     BACKEND        EPS  RPS       5XX     ms   CANARY
│shop.example.com       /             Prefix   web              1      52.0   0.00%   0.6 -
│shop.example.com       /api          Prefix   api              2      52.0   0.00%   0.6 20%→api-next
│*                      /status       Prefix   web              1      0.00       -     - -
│*.example.com          /             Prefix   web              1      0.00       -     - -
╰ sorted by rps desc ────────────────────────────────────────────────────────────────────────╯
 ● live · polling every 1s
 q quit  Tab generations  r rps  e 5xx  l latency  h host  / filter  g refresh
```

## Usage

```sh
# The default target is the conventional admin port, 127.0.0.1:10254.
cargo run -p ramjet-top

# Anywhere else. A bare host:port is fine; it gets an http:// scheme.
ramjet-top 10.0.0.5:10254
ramjet-top --url http://10.0.0.5:10254

# Against a pod, through a port-forward.
kubectl port-forward -n ingress ds/ramjet-ingress 10254:10254 &
ramjet-top localhost:10254

# Poll faster, or slower.
ramjet-top -i 250ms
ramjet-top --interval 5s

# Somebody else's cluster: watch, but do not touch.
ramjet-top --read-only

# One shot, for a script, a CI log, or an incident channel.
ramjet-top --once
ramjet-top --json | jq '.routes.routes[] | select(.errors_5xx_total > 0)'
```

`--once` prints an aligned text table and exits: no terminal required, sorted by
host and path so two runs are diffable, and reporting cumulative counters rather
than rates, because a rate is a difference between two polls and this mode does
one. `--json` dumps the merged snapshot — both admin responses verbatim plus the
series read out of `/metrics` — and implies `--once`.

Exit status is `0` on success, `1` if the daemon could not be reached, and `2`
if the command line was wrong.

## Keys

| Key | Does |
|---|---|
| `q`, `Ctrl-C` | Quit. Restores the terminal, including after a panic. |
| `Tab` | Switch between the routes table and the generation timeline. |
| `r` `e` `l` `h` | Sort routes by rps, 5xx rate, latency, host. The same key again reverses. |
| `/` | Filter routes. Substring, case-insensitive, over host, path, backend and type. |
| `Enter` | In the filter: keep it. In the timeline: expand the generation's diff. |
| `Esc` | Collapse a diff, then clear the filter, then clear the selection. |
| `j` `k`, `↑` `↓` | Move the selection. |
| `PgUp` `PgDn`, `Home` `End` | Move further, and to the ends. |
| `g` | Poll now, without waiting for the tick. |
| `p` | Pin traffic to the selected generation. Asks first. |
| `u` | Release the pin. Asks first. |

`p` and `u` are the emergency brake — they drive `POST`/`DELETE
/admin/rollback`, which freezes the data plane on one generation until it is
released. Both need a `y` to confirm, anything else cancels, and `--read-only`
refuses them outright and stops advertising them.

## What the numbers mean

Everything the server exports is cumulative and everything on screen is a rate,
so the interesting part is the subtraction. Three things make it harder than it
looks, and all three are handled:

- **Counters restart.** A removed and re-added route, or a restarted data plane,
  drops a counter below the value held from last poll. Every subtraction
  saturates at zero, so a restart reads as `0.00` rather than as eighteen
  quintillion requests per second.
- **Routes are not rows.** The table is rebuilt every generation, so "the same
  route" is keyed on host, path and path type — deliberately *not* on the
  backend, because a backend swap is the most interesting moment to keep
  watching a route through.
- **A new route has no rate.** Dividing a lifetime counter by one poll interval
  reports an hour's traffic as if it happened this second. New routes show `-`
  for one interval, are flagged green, and report a real rate from the next poll.

The interval divided by is the measured gap between polls, from a monotonic
clock — not `--interval`. A poll that took 900ms because the server was busy
would otherwise inflate every rate on screen at the worst possible moment.

Latency is a *windowed* mean: the delta of the sum over the delta of the count.
On a process that has been up a week, a lifetime mean cannot move, and an
upstream that just started taking two seconds would not show up in it at all.

## When the daemon goes away

The last good data stays on screen, dimmed and marked `STALE`, with the status
line saying how long ago it was true and why the poll failed. It never clears
the screen to print a connection error: the moment the daemon becomes
unreachable is the moment its last known state is most worth looking at.

When it comes back, the rate reported for the gap is the true average across it
— 600 requests over a 60-second outage is `10/s`, not `600/s`.

## Testing

```sh
cargo test -p ramjet-top      # unit, mock-server, and spawned-binary tests
cargo clippy -p ramjet-top --all-targets -- -D warnings
```

The mock-server tests run the real client against a real `hyper` listener
serving canned admin responses, and assert the computed view model rather than
anything about pixels. The `--once` tests spawn the compiled binary and read its
stdout, stderr and exit status.
