#!/usr/bin/env python3
"""Measurement probes that oha cannot express.

Three subcommands, all speaking HTTP/1.1 over raw sockets because every one of
them needs to know something a normal client library hides: exactly when the
server closed a connection, exactly which request first saw a new route, and
the latency of every individual request rather than an aggregate.

    timeline   one sequential keep-alive request stream, every request logged
               with its offset and latency, so a reload stall is visible as a
               shape rather than as a moved percentile.

    idle       hold N idle keep-alive connections open, probe them on a slow
               cadence, and report how many the server closed underneath us.
               This is the whole point of the churn benchmark: nginx retires
               its workers on reload and their idle connections go with them.

    propagate  apply a manifest and poll the data plane until the change is
               actually being served, timing apply -> first-correct-response.

No third-party imports: this runs on whatever python3 the host has.
"""

import argparse
import errno
import json
import os
import select
import socket
import subprocess
import sys
import time


# ---------------------------------------------------------------------------
# A minimal, strict HTTP/1.1 client.
#
# Written out rather than pulled from http.client because every probe here
# depends on connection reuse being real and on close detection being exact,
# and http.client's own reconnect-on-failure behaviour would silently paper
# over the exact event being measured.
# ---------------------------------------------------------------------------


class Closed(Exception):
    """The peer closed the connection."""


class Conn:
    def __init__(self, addr, host_header, timeout=10.0):
        self.addr = addr
        self.host_header = host_header
        self.timeout = timeout
        self.sock = None
        self.buf = b""
        self.opened_at = None

    def connect(self):
        ip, port = self.addr.split(":")
        s = socket.create_connection((ip, int(port)), timeout=self.timeout)
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.sock = s
        self.buf = b""
        self.opened_at = time.monotonic()

    def close(self):
        if self.sock is not None:
            try:
                self.sock.close()
            except OSError:
                pass
            self.sock = None

    def peer_closed(self):
        """True if the server has sent FIN/RST while we were not looking.

        A socket that is readable with nothing buffered from a request we made
        is a socket the peer has finished with. That is the signal an nginx
        reload produces on every idle keep-alive connection its retiring
        workers were holding, and it is invisible to any client that simply
        reconnects.
        """
        if self.sock is None:
            return True
        r, _, x = select.select([self.sock], [], [self.sock], 0)
        if x:
            return True
        if not r:
            return False
        try:
            chunk = self.sock.recv(65536, socket.MSG_PEEK)
        except OSError:
            return True
        return chunk == b""

    def _fill(self):
        chunk = self.sock.recv(65536)
        if not chunk:
            raise Closed()
        self.buf += chunk

    def _read_line(self):
        while b"\r\n" not in self.buf:
            self._fill()
        line, self.buf = self.buf.split(b"\r\n", 1)
        return line

    def _read_exactly(self, n):
        while len(self.buf) < n:
            self._fill()
        body, self.buf = self.buf[:n], self.buf[n:]
        return body

    def request(self, path="/"):
        """Send one request, return (status, body). Raises Closed / OSError."""
        if self.sock is None:
            self.connect()
        req = (
            f"GET {path} HTTP/1.1\r\n"
            f"Host: {self.host_header}\r\n"
            f"Connection: keep-alive\r\n"
            f"Accept: */*\r\n"
            f"User-Agent: ramjet-thesis-probe\r\n\r\n"
        ).encode()
        self.sock.sendall(req)

        status_line = self._read_line()
        parts = status_line.split(b" ", 2)
        if len(parts) < 2 or not parts[1].isdigit():
            raise Closed()
        status = int(parts[1])

        headers = {}
        while True:
            line = self._read_line()
            if line == b"":
                break
            if b":" in line:
                k, v = line.split(b":", 1)
                headers[k.strip().lower()] = v.strip()

        if headers.get(b"transfer-encoding", b"").lower() == b"chunked":
            body = b""
            while True:
                size = int(self._read_line().split(b";")[0] or b"0", 16)
                if size == 0:
                    self._read_line()
                    break
                body += self._read_exactly(size)
                self._read_line()
        else:
            body = self._read_exactly(int(headers.get(b"content-length", b"0")))

        if headers.get(b"connection", b"").lower() == b"close":
            self.close()
        return status, body


# ---------------------------------------------------------------------------
# timeline
# ---------------------------------------------------------------------------


def cmd_timeline(args):
    """One sequential request stream, every request recorded.

    Sequential and single-connection on purpose. A concurrent load generator
    reports percentiles, which smear a 300 ms stall across a 110-second window
    until it is indistinguishable from ordinary tail latency. One request at a
    time, each with a timestamp, shows where the stall was and how long it
    lasted.
    """
    conn = Conn(args.target, args.host)
    samples = []
    start = time.monotonic()
    deadline = start + args.duration

    while time.monotonic() < deadline:
        t0 = time.monotonic()
        try:
            status, _ = conn.request()
            entry = (round(t0 - start, 4), round((time.monotonic() - t0) * 1e6), status)
        except (Closed, OSError, ValueError) as exc:
            kind = type(exc).__name__ if not isinstance(exc, OSError) else errno.errorcode.get(exc.errno, "OSError")
            entry = (round(t0 - start, 4), round((time.monotonic() - t0) * 1e6), kind)
            conn.close()
        samples.append(entry)
        if args.interval:
            time.sleep(args.interval)

    conn.close()
    lats = [s[1] for s in samples if isinstance(s[2], int) and s[2] == 200]
    lats.sort()

    def pct(p):
        return lats[min(len(lats) - 1, int(len(lats) * p / 100))] if lats else None

    json.dump(
        {
            "target": args.target,
            "host": args.host,
            "duration_s": args.duration,
            "requests": len(samples),
            "ok": len(lats),
            "non_200": sum(1 for s in samples if isinstance(s[2], int) and s[2] != 200),
            "errors": sum(1 for s in samples if not isinstance(s[2], int)),
            "latency_us": {"p50": pct(50), "p99": pct(99), "p999": pct(99.9), "max": lats[-1] if lats else None},
            # Every request. The point of this probe is the shape, so the raw
            # series is the artifact, not the summary above it.
            "samples": samples,
        },
        open(args.out, "w"),
    )
    print(f"timeline: {len(samples)} requests, {len(lats)} ok, "
          f"{sum(1 for s in samples if not isinstance(s[2], int))} errors, max {lats[-1] if lats else 0} us")


# ---------------------------------------------------------------------------
# idle
# ---------------------------------------------------------------------------


def cmd_idle(args):
    """Hold N idle keep-alive connections and count what survives.

    Each connection makes one request to establish itself, then goes quiet.
    Every `probe_every` seconds each one is first checked for a server-sent
    FIN (which is the event of interest) and then, if still alive, given one
    request — both to prove it still works and to keep it inside the server's
    keep-alive idle timeout, so that a connection lost here was lost to a
    config change rather than to an ordinary timeout expiring.
    """
    conns = []
    for _ in range(args.count):
        c = Conn(args.target, args.host)
        c.connect()
        status, _ = c.request()
        if status != 200:
            raise SystemExit(f"establishing request returned {status}, expected 200")
        conns.append(c)

    alive = {id(c): True for c in conns}
    events = []
    start = time.monotonic()
    rounds = 0

    while time.monotonic() - start < args.duration:
        time.sleep(min(args.probe_every, max(0.0, args.duration - (time.monotonic() - start))))
        rounds += 1
        now = round(time.monotonic() - start, 3)
        for c in conns:
            if not alive[id(c)]:
                continue
            if c.peer_closed():
                alive[id(c)] = False
                events.append({"t": now, "event": "closed_while_idle"})
                c.close()
                continue
            try:
                status, _ = c.request()
                if status != 200:
                    alive[id(c)] = False
                    events.append({"t": now, "event": f"probe_status_{status}"})
                    c.close()
            except (Closed, OSError) as exc:
                alive[id(c)] = False
                name = errno.errorcode.get(getattr(exc, "errno", None), type(exc).__name__)
                events.append({"t": now, "event": f"probe_failed_{name}"})
                c.close()

    survivors = sum(1 for c in conns if alive[id(c)])
    for c in conns:
        c.close()

    result = {
        "target": args.target,
        "host": args.host,
        "held": args.count,
        "survived": survivors,
        "lost": args.count - survivors,
        "probe_rounds": rounds,
        "probe_every_s": args.probe_every,
        "duration_s": args.duration,
        "events": events,
    }
    json.dump(result, open(args.out, "w"))
    print(f"idle keep-alive: {survivors}/{args.count} survived {args.duration}s "
          f"({args.count - survivors} lost)")


# ---------------------------------------------------------------------------
# propagate
# ---------------------------------------------------------------------------


def cmd_propagate(args):
    """Time `kubectl apply` -> the data plane actually serving the change.

    The clock starts before kubectl is invoked, because that is when a human
    or a CI job asked for the change; the moment kubectl *returned* is recorded
    separately, so the admission-webhook share of the wait can be read off
    rather than argued about.

    Polling is on its own connection at a fixed 20 ms cadence. `--expect` picks
    the success condition: absent, the first HTTP 200 wins (a brand-new host
    404s until its route exists); present, the body must also contain that
    marker (a backend swap keeps answering 200 from the *old* backend the whole
    time, so status alone would report zero).
    """
    conn = Conn(args.target, args.host, timeout=5.0)

    t0 = time.monotonic()
    proc = subprocess.Popen(
        args.apply_cmd,
        shell=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )

    applied_at = None
    served_at = None
    polls = 0
    deadline = t0 + args.timeout

    while time.monotonic() < deadline:
        if applied_at is None and proc.poll() is not None:
            applied_at = time.monotonic()
            if proc.returncode != 0:
                err = proc.stderr.read().decode(errors="replace")
                raise SystemExit(f"apply failed ({proc.returncode}): {err.strip()[:400]}")
        polls += 1
        try:
            status, body = conn.request()
            hit = status == 200 and (args.expect.encode() in body if args.expect else True)
            if hit:
                served_at = time.monotonic()
                break
        except (Closed, OSError, ValueError):
            conn.close()
        time.sleep(args.poll_interval)

    if applied_at is None:
        proc.wait(timeout=max(1.0, deadline - time.monotonic()))
        applied_at = time.monotonic()
    conn.close()

    result = {
        "host": args.host,
        "target": args.target,
        "expect": args.expect,
        "apply_ms": round((applied_at - t0) * 1000, 1),
        "serve_ms": round((served_at - t0) * 1000, 1) if served_at else None,
        "post_apply_ms": round((served_at - applied_at) * 1000, 1) if served_at else None,
        "polls": polls,
        "timed_out": served_at is None,
    }
    if args.out:
        json.dump(result, open(args.out, "w"))
    print(json.dumps(result))


# ---------------------------------------------------------------------------


def main():
    p = argparse.ArgumentParser(description=__doc__)
    sub = p.add_subparsers(dest="cmd", required=True)

    t = sub.add_parser("timeline")
    t.add_argument("--target", required=True)
    t.add_argument("--host", required=True)
    t.add_argument("--duration", type=float, required=True)
    t.add_argument("--interval", type=float, default=0.0)
    t.add_argument("--out", required=True)
    t.set_defaults(func=cmd_timeline)

    i = sub.add_parser("idle")
    i.add_argument("--target", required=True)
    i.add_argument("--host", required=True)
    i.add_argument("--count", type=int, default=50)
    i.add_argument("--duration", type=float, required=True)
    i.add_argument("--probe-every", type=float, default=10.0)
    i.add_argument("--out", required=True)
    i.set_defaults(func=cmd_idle)

    g = sub.add_parser("propagate")
    g.add_argument("--target", required=True)
    g.add_argument("--host", required=True)
    g.add_argument("--apply-cmd", required=True)
    g.add_argument("--expect", default="")
    g.add_argument("--poll-interval", type=float, default=0.02)
    g.add_argument("--timeout", type=float, default=120.0)
    g.add_argument("--out", default="")
    g.set_defaults(func=cmd_propagate)

    args = p.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
