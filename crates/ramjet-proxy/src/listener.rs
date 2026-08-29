//! Binding and accepting, with the socket options an ingress actually needs.
//!
//! `TcpListener::bind` is one line and wrong for this job in three ways, all of
//! which show up only under load or during a rollout:
//!
//! - **`SO_REUSEADDR`** so a restarting pod can rebind a port still holding
//!   sockets in `TIME_WAIT`. Without it a redeploy fails to bind for up to two
//!   minutes, which turns a rolling update into an outage.
//! - **`SO_REUSEPORT`** so several processes — or several bound listeners in
//!   one process — can share a port and let the kernel spread accepts across
//!   them. This is what makes a zero-downtime handover possible: the new
//!   process binds the same port, both accept for a moment, and the old one
//!   drains. It is also the escape hatch for the single-accept-queue bottleneck
//!   at very high connection rates.
//! - **`TCP_NODELAY`** on every accepted connection. Nagle's algorithm delays a
//!   small write waiting for more data to coalesce; for a proxy, a small write
//!   is a response header block that has nothing following it. The result is up
//!   to 40ms of latency added to exactly the requests that were supposed to be
//!   fast.
//!
//! Nothing here is async except [`Listener::accept`]; binding happens before
//! the runtime is doing anything interesting, which is also what lets a caller
//! bind port 0 and read back the assigned port before serving.

use std::io;
use std::net::SocketAddr;

use socket2::{Domain, Protocol, Socket, Type};
use tokio::net::{TcpListener, TcpStream};

/// The first port a process with no capability of its own may bind on Linux,
/// unless the node's `net.ipv4.ip_unprivileged_port_start` says otherwise.
const FIRST_UNPRIVILEGED_PORT: u16 = 1024;

/// Replace a bare `EACCES` from `bind(2)` with an error an operator can act on.
///
/// This is the one bind failure whose kernel message says nothing useful.
/// `Permission denied (os error 13)` names neither the port that was refused
/// nor the reason, and the reason is genuinely surprising: a Kubernetes
/// `securityContext` that adds `NET_BIND_SERVICE` puts the capability in the
/// container's permitted and bounding sets, and a **non-root** process still
/// drops it on `execve` — the kernel raises a capability into a non-root
/// process's effective set only from a file capability on the binary. So the
/// values file reads as though the capability was granted, the pod crash-loops,
/// and the log says "Permission denied".
///
/// Everything else is passed through unchanged. `EACCES` on any other port is a
/// different problem (SELinux, an LSM, a seccomp filter) and guessing at it
/// would bury the real error under a paragraph about ports. Port 0 counts as
/// "any other": the kernel picks from the ephemeral range, which is never
/// privileged.
pub fn explain_bind_failure(addr: SocketAddr, error: io::Error) -> io::Error {
    let privileged = (1..FIRST_UNPRIVILEGED_PORT).contains(&addr.port());
    if error.kind() != io::ErrorKind::PermissionDenied || !privileged {
        return error;
    }

    io::Error::new(
        io::ErrorKind::PermissionDenied,
        format!(
            "cannot bind {addr}: permission denied. \
             Binding a port below {FIRST_UNPRIVILEGED_PORT} as uid {uid} needs CAP_NET_BIND_SERVICE \
             in this process's *effective* set, and a non-root process gets one there only from a \
             file capability on the binary — Kubernetes' securityContext.capabilities.add grants \
             it to the container, and execve then drops it again. Two ways out. (1) Run an image \
             whose binary carries the capability, which the published one does \
             (`setcap cap_net_bind_service=+ep /usr/local/bin/ramjet-ingressd`), and keep \
             NET_BIND_SERVICE in securityContext.capabilities.add so it stays in the container's \
             bounding set. (2) `sysctl -w net.ipv4.ip_unprivileged_port_start={port}` on the \
             node, which makes this port unprivileged for everything on it — and has to be set \
             on the node, because a host-network pod shares the node's network namespace and the \
             kubelet refuses to set net.* sysctls for such a pod",
            uid = effective_uid(),
            port = addr.port(),
        ),
    )
}

/// The uid the kernel checked the capability against, for the message above.
///
/// `rustix` rather than a `libc::geteuid` call because this crate forbids
/// unsafe code, and rather than nothing at all because "as uid 65532" is the
/// line that tells a reader the process is not root — which is the whole
/// mechanism, and the thing a values file does not make obvious.
fn effective_uid() -> String {
    #[cfg(unix)]
    {
        rustix::process::geteuid().as_raw().to_string()
    }
    #[cfg(not(unix))]
    {
        "this process's user".to_owned()
    }
}

/// How a listening socket is configured.
#[derive(Debug, Clone, Copy)]
pub struct ListenerConfig {
    /// Address to bind. Port `0` asks the kernel to assign one.
    pub addr: SocketAddr,
    /// Pending-connection queue depth handed to `listen(2)`.
    pub backlog: i32,
    /// Set `SO_REUSEPORT` where the platform has it.
    pub reuse_port: bool,
    /// Set `TCP_NODELAY` on accepted connections.
    pub nodelay: bool,
}

impl ListenerConfig {
    /// A configuration for `addr` with the defaults every listener wants.
    pub fn new(addr: SocketAddr) -> Self {
        ListenerConfig {
            addr,
            // 1024 rather than the customary 128: an ingress absorbs
            // thundering herds (a node rebooting, a client fleet reconnecting)
            // and a short queue turns those into connection refusals rather
            // than into latency. Linux clamps this to `somaxconn` anyway.
            backlog: 1024,
            reuse_port: true,
            nodelay: true,
        }
    }
}

/// A bound, listening socket.
#[derive(Debug)]
pub struct Listener {
    inner: TcpListener,
    nodelay: bool,
}

impl Listener {
    /// Binds and starts listening.
    ///
    /// Options are set on the socket *before* `bind(2)`, which is the only
    /// order in which `SO_REUSEADDR` and `SO_REUSEPORT` have any effect.
    ///
    /// Not async, but it must be called from inside a tokio runtime: handing
    /// the socket to tokio registers it with the reactor, and there has to be
    /// one. In practice that means calling it from within `#[tokio::main]`,
    /// which is also where a caller wants a bind failure reported.
    pub fn bind(config: &ListenerConfig) -> io::Result<Self> {
        let socket = Socket::new(
            Domain::for_address(config.addr),
            Type::STREAM,
            Some(Protocol::TCP),
        )?;

        socket.set_reuse_address(true)?;

        #[cfg(all(
            unix,
            not(any(target_os = "solaris", target_os = "illumos", target_os = "cygwin"))
        ))]
        if config.reuse_port {
            socket.set_reuse_port(true)?;
        }

        // tokio requires a non-blocking socket; converting a blocking one
        // produces a listener that stalls the whole worker on accept.
        socket.set_nonblocking(true)?;
        socket
            .bind(&config.addr.into())
            .map_err(|error| explain_bind_failure(config.addr, error))?;
        socket.listen(config.backlog)?;

        Ok(Listener {
            inner: TcpListener::from_std(socket.into())?,
            nodelay: config.nodelay,
        })
    }

    /// The address actually bound, which is how a caller recovers the port it
    /// asked the kernel to choose.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }

    /// Accepts one connection, applying `TCP_NODELAY` if configured.
    ///
    /// A failure to set `TCP_NODELAY` is not fatal: the connection still works,
    /// it is just potentially slower, and refusing to serve it would be a
    /// strictly worse outcome.
    pub async fn accept(&self) -> io::Result<(TcpStream, SocketAddr)> {
        let (stream, peer) = self.inner.accept().await?;
        if self.nodelay {
            let _ = stream.set_nodelay(true);
        }
        Ok((stream, peer))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, SocketAddrV4};

    fn loopback() -> SocketAddr {
        SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))
    }

    #[tokio::test]
    async fn port_zero_reports_the_assigned_port() {
        let listener = Listener::bind(&ListenerConfig::new(loopback())).expect("binds");
        let addr = listener.local_addr().expect("addr");
        assert_ne!(addr.port(), 0, "the kernel must have assigned a port");
    }

    #[tokio::test]
    async fn reuse_port_allows_a_second_bind_to_the_same_port() {
        // This is the property the zero-downtime handover depends on, so it is
        // worth asserting rather than assuming.
        let first = Listener::bind(&ListenerConfig::new(loopback())).expect("binds");
        let addr = first.local_addr().expect("addr");
        let second = Listener::bind(&ListenerConfig::new(addr));
        assert!(
            second.is_ok(),
            "SO_REUSEPORT should permit a second listener on {addr}"
        );
    }

    #[test]
    fn a_refused_privileged_bind_says_what_to_do_about_it() {
        let addr: SocketAddr = "0.0.0.0:80".parse().expect("literal");
        let error = explain_bind_failure(addr, io::Error::from(io::ErrorKind::PermissionDenied));

        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
        let message = error.to_string();
        // The port, the uid, and both remedies — each of which somebody has to
        // be able to find in a `kubectl logs` of a crash-looping pod.
        assert!(message.contains("0.0.0.0:80"), "{message}");
        assert!(
            message.contains(&format!("uid {}", effective_uid())),
            "{message}"
        );
        assert!(message.contains("setcap cap_net_bind_service=+ep"), "{message}");
        assert!(message.contains("capabilities.add"), "{message}");
        assert!(
            message.contains("net.ipv4.ip_unprivileged_port_start"),
            "{message}"
        );
    }

    #[test]
    fn every_other_bind_failure_is_passed_through_untouched() {
        // A port already in use is the common bind failure and the kernel's
        // own message is the right one; so is EACCES on a high port, which is
        // an LSM or a seccomp filter and has nothing to do with capabilities.
        let addr: SocketAddr = "0.0.0.0:80".parse().expect("literal");
        let in_use = explain_bind_failure(addr, io::Error::from(io::ErrorKind::AddrInUse));
        assert!(!in_use.to_string().contains("setcap"), "{in_use}");

        let high: SocketAddr = "0.0.0.0:8080".parse().expect("literal");
        let denied = explain_bind_failure(high, io::Error::from(io::ErrorKind::PermissionDenied));
        assert!(!denied.to_string().contains("setcap"), "{denied}");

        // Port 0 is the kernel picking from the ephemeral range, which is never
        // privileged — so an EACCES there is somebody else's problem too.
        let any: SocketAddr = "0.0.0.0:0".parse().expect("literal");
        let ephemeral = explain_bind_failure(any, io::Error::from(io::ErrorKind::PermissionDenied));
        assert!(!ephemeral.to_string().contains("setcap"), "{ephemeral}");
    }

    #[tokio::test]
    async fn ipv6_binds_too() {
        let addr: SocketAddr = "[::1]:0".parse().expect("literal");
        let listener = Listener::bind(&ListenerConfig::new(addr)).expect("binds");
        assert!(listener.local_addr().expect("addr").is_ipv6());
    }
}
