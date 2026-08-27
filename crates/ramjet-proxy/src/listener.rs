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
        socket.bind(&config.addr.into())?;
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

    #[tokio::test]
    async fn ipv6_binds_too() {
        let addr: SocketAddr = "[::1]:0".parse().expect("literal");
        let listener = Listener::bind(&ListenerConfig::new(addr)).expect("binds");
        assert!(listener.local_addr().expect("addr").is_ipv6());
    }
}
