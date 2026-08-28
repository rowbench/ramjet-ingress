#!/usr/bin/env python3
"""Open N keep-alive connections, make each one real, then hold them idle.

"Real" matters: a connected socket that has never carried a request costs a
proxy an accept and a read buffer, while a connection that has completed a
request has whatever per-connection state the proxy actually keeps. Measuring
the first and calling it the second would flatter whichever side allocates
lazily.

Prints a line to stdout at each phase so the harness outside can sample memory
at the right moments, and waits on stdin between phases so the harness — not a
sleep — decides when to move on.
"""

import socket
import sys
import time


def main():
    target, count = sys.argv[1], int(sys.argv[2])
    host_header = sys.argv[3]
    ip, port = target.split(":")
    port = int(port)

    req = (
        f"GET / HTTP/1.1\r\nHost: {host_header}\r\n"
        f"Connection: keep-alive\r\nAccept: */*\r\n\r\n"
    ).encode()

    conns = []
    failed = 0
    t0 = time.monotonic()
    for i in range(count):
        try:
            s = socket.create_connection((ip, port), timeout=15)
            s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            s.sendall(req)
            # Read exactly one response. The bodies here are a fixed ~128 bytes
            # with Content-Length, so one recv is enough in practice; loop
            # anyway so a split response cannot desynchronise the socket.
            buf = b""
            while b"\r\n\r\n" not in buf:
                chunk = s.recv(65536)
                if not chunk:
                    raise ConnectionError("closed during handshake")
                buf += chunk
            head, _, rest = buf.partition(b"\r\n\r\n")
            clen = 0
            for line in head.split(b"\r\n"):
                if line.lower().startswith(b"content-length:"):
                    clen = int(line.split(b":", 1)[1])
            while len(rest) < clen:
                rest += s.recv(65536)
            if b" 200 " not in head.split(b"\r\n")[0]:
                raise ConnectionError(f"status {head.split(b' ')[1]!r}")
            conns.append(s)
        except (OSError, ConnectionError) as exc:
            failed += 1
            if failed <= 3:
                print(f"note: connection {i} failed: {exc}", file=sys.stderr, flush=True)
            if failed > count // 10:
                print(f"ABORT {len(conns)} {failed}", flush=True)
                return

    print(f"OPEN {len(conns)} {failed} {time.monotonic() - t0:.1f}", flush=True)
    sys.stdin.readline()          # harness samples memory here

    for s in conns:
        s.close()
    print(f"CLOSED {len(conns)}", flush=True)
    sys.stdin.readline()          # harness samples memory again


if __name__ == "__main__":
    main()
