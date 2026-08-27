//! Every raw syscall this crate makes, in one place.
//!
//! The reactor covers accept, read, write and close. What it does not cover —
//! opening an outbound socket, learning that a `connect` finished, a pipe to
//! wake a thread with — has to be done directly, and doing it directly means
//! `unsafe`. Confining it to one module with a safe signature on each function
//! is the same discipline the runtime itself uses: there is one file to audit,
//! and the rest of the crate is ordinary Rust.
//!
//! Every function here returns `io::Result` and every `unsafe` block carries
//! the argument for why it is sound.

use std::io;
use std::mem;
use std::net::{SocketAddr, SocketAddrV4, SocketAddrV6};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};

/// Make `fd` non-blocking.
///
/// The reactor requires this of every descriptor handed to it and neither
/// checks nor sets it: a blocking fd stalls a whole core on its first syscall.
pub fn set_nonblocking(fd: RawFd) -> io::Result<()> {
    // SAFETY: F_GETFL reads the descriptor's flag word and touches no memory
    // of ours.
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    if flags < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: same call with the word we just read, plus one bit.
    if unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Stop `fd` leaking across an `exec`.
pub fn set_cloexec(fd: RawFd) -> io::Result<()> {
    // SAFETY: F_SETFD takes an int by value. FD_CLOEXEC is the only
    // descriptor flag there is, so writing the whole word clobbers nothing.
    if unsafe { libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

fn set_flag(fd: RawFd, level: libc::c_int, name: libc::c_int, on: bool) -> io::Result<()> {
    let value: libc::c_int = on.into();
    // SAFETY: `value` is a live c_int and we pass exactly its size as optlen,
    // so setsockopt reads only bytes we own.
    let r = unsafe {
        libc::setsockopt(
            fd,
            level,
            name,
            std::ptr::from_ref(&value).cast(),
            mem::size_of::<libc::c_int>() as libc::socklen_t,
        )
    };
    if r < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Stop a write to a dead peer from killing the process.
///
/// The io_uring backend sends with `MSG_NOSIGNAL` and needs nothing from us.
/// The kqueue backend calls plain `write(2)`, which has no such flag, and sets
/// `SO_NOSIGPIPE` only on descriptors it accepted itself — an outbound socket
/// we opened gets none of that, so the first write to a closed upstream would
/// raise `SIGPIPE` and terminate the daemon.
fn set_nosigpipe(fd: RawFd) -> io::Result<()> {
    #[cfg(any(target_os = "macos", target_os = "ios", target_os = "freebsd"))]
    {
        set_flag(fd, libc::SOL_SOCKET, libc::SO_NOSIGPIPE, true)?;
    }
    #[cfg(not(any(target_os = "macos", target_os = "ios", target_os = "freebsd")))]
    {
        let _ = fd;
    }
    Ok(())
}

/// Ignore `SIGPIPE` process-wide.
///
/// Belt and braces beside [`set_nosigpipe`]: the per-socket option covers the
/// sockets this crate opens, and this covers everything else in the process
/// that might write to a closed descriptor. A server that dies because a client
/// hung up is not a server.
pub fn ignore_sigpipe() {
    // SAFETY: setting a signal to SIG_IGN takes two scalars and touches no
    // memory of ours. Doing it more than once is harmless.
    unsafe {
        libc::signal(libc::SIGPIPE, libc::SIG_IGN);
    }
}

/// Start a TCP connection without waiting for it.
///
/// Returns the socket and whether the connection is **already** established —
/// which happens for a loopback peer often enough that skipping the wait is
/// worth a branch. Otherwise the caller must wait for writability before it can
/// send, which on this reactor means handing the descriptor to
/// [`crate::helper`].
///
/// The socket arrives non-blocking, close-on-exec, `TCP_NODELAY`, and (where
/// the platform has it) `SO_NOSIGPIPE`. The reactor sets none of that on a
/// descriptor it did not accept itself.
pub fn tcp_connect(addr: SocketAddr) -> io::Result<(OwnedFd, bool)> {
    let domain = match addr {
        SocketAddr::V4(_) => libc::AF_INET,
        SocketAddr::V6(_) => libc::AF_INET6,
    };
    // SAFETY: socket() takes three scalars and returns a fresh descriptor
    // or -1.
    let raw = unsafe { libc::socket(domain, libc::SOCK_STREAM, 0) };
    if raw < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: socket() just minted `raw` and nothing else owns it. Handing it
    // to OwnedFd now is what closes it on every `?` below — and it matters more
    // than usual here, because the io_uring backend indexes an unbounded table
    // by fd and a negative one would abort the process.
    let fd = unsafe { OwnedFd::from_raw_fd(raw) };

    set_cloexec(raw)?;
    set_nonblocking(raw)?;
    set_nosigpipe(raw)?;
    // A proxy writes one whole request at a time and then waits; Nagle would
    // add up to 40ms to every one of them.
    set_flag(raw, libc::IPPROTO_TCP, libc::TCP_NODELAY, true)?;

    let connected = match addr {
        SocketAddr::V4(v4) => {
            let sa = sockaddr_v4(&v4);
            // SAFETY: `sa` is a fully initialised sockaddr_in and we pass its
            // exact size, so connect reads only bytes we own.
            connect_result(unsafe {
                libc::connect(
                    raw,
                    std::ptr::from_ref(&sa).cast(),
                    mem::size_of::<libc::sockaddr_in>() as libc::socklen_t,
                )
            })?
        }
        SocketAddr::V6(v6) => {
            let sa = sockaddr_v6(&v6);
            // SAFETY: as above, for a fully initialised sockaddr_in6.
            connect_result(unsafe {
                libc::connect(
                    raw,
                    std::ptr::from_ref(&sa).cast(),
                    mem::size_of::<libc::sockaddr_in6>() as libc::socklen_t,
                )
            })?
        }
    };
    Ok((fd, connected))
}

/// `Ok(true)` if the connection completed at once, `Ok(false)` if it is under
/// way, `Err` if it failed outright.
fn connect_result(r: libc::c_int) -> io::Result<bool> {
    if r == 0 {
        return Ok(true);
    }
    let err = io::Error::last_os_error();
    match err.raw_os_error() {
        Some(libc::EINPROGRESS) | Some(libc::EINTR) => Ok(false),
        _ => Err(err),
    }
}

/// The pending error on a socket, as `SO_ERROR`. Zero means "connected".
///
/// This is the only way to learn how a non-blocking `connect` finished:
/// writability says it is *over*, not that it *worked*.
pub fn socket_error(fd: RawFd) -> io::Result<i32> {
    let mut value: libc::c_int = 0;
    let mut len = mem::size_of::<libc::c_int>() as libc::socklen_t;
    // SAFETY: `value` and `len` are live and correctly sized; getsockopt
    // writes at most `len` bytes into `value`.
    let r = unsafe {
        libc::getsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_ERROR,
            std::ptr::from_mut(&mut value).cast(),
            &mut len,
        )
    };
    if r < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(value)
}

/// A pipe, read end first, both non-blocking and close-on-exec.
pub fn pipe_pair() -> io::Result<(OwnedFd, OwnedFd)> {
    let mut fds = [0 as libc::c_int; 2];
    // SAFETY: pipe(2) writes exactly two ints into the array we hand it.
    if unsafe { libc::pipe(fds.as_mut_ptr()) } < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: pipe() just created both descriptors and nothing else owns them.
    let read = unsafe { OwnedFd::from_raw_fd(fds[0]) };
    // SAFETY: as above.
    let write = unsafe { OwnedFd::from_raw_fd(fds[1]) };
    for fd in [read.as_raw_fd(), write.as_raw_fd()] {
        set_cloexec(fd)?;
        set_nonblocking(fd)?;
    }
    Ok((read, write))
}

/// A connected pair of stream sockets, for handing descriptors between threads.
pub fn socket_pair() -> io::Result<(OwnedFd, OwnedFd)> {
    let mut fds = [0 as libc::c_int; 2];
    // SAFETY: socketpair(2) writes exactly two ints into the array.
    if unsafe { libc::socketpair(libc::AF_UNIX, libc::SOCK_STREAM, 0, fds.as_mut_ptr()) } < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: socketpair() just created both and nothing else owns them.
    let a = unsafe { OwnedFd::from_raw_fd(fds[0]) };
    // SAFETY: as above.
    let b = unsafe { OwnedFd::from_raw_fd(fds[1]) };
    for fd in [a.as_raw_fd(), b.as_raw_fd()] {
        set_cloexec(fd)?;
    }
    Ok((a, b))
}

/// `write(2)`, returning the count or the error.
pub fn write(fd: RawFd, buf: &[u8]) -> io::Result<usize> {
    // SAFETY: we pass a pointer to `buf` and exactly its length, so the kernel
    // reads only bytes we own.
    let n = unsafe { libc::write(fd, buf.as_ptr().cast(), buf.len()) };
    if n < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(n as usize)
}

/// `read(2)`, returning the count or the error. Zero is end of file.
pub fn read(fd: RawFd, buf: &mut [u8]) -> io::Result<usize> {
    // SAFETY: we pass a pointer to `buf` and exactly its capacity, so the
    // kernel writes only into bytes we own.
    let n = unsafe { libc::read(fd, buf.as_mut_ptr().cast(), buf.len()) };
    if n < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(n as usize)
}

/// Close a descriptor, ignoring the result.
///
/// There is nothing useful to do about a failing `close`: the descriptor is
/// gone either way, and on Linux even `EINTR` does not mean it survived.
///
/// # Safety
///
/// `fd` must be owned by the caller and not registered with the reactor or the
/// helper thread. Closing a descriptor either of those still holds is the
/// classic use-after-free of async I/O: the number is recycled immediately and
/// an operation in flight lands on whatever took its place.
pub unsafe fn close(fd: RawFd) {
    // SAFETY: the caller promises it owns this descriptor and that nothing
    // else refers to it.
    unsafe {
        libc::close(fd);
    }
}

/// `poll(2)` over a set of descriptors, with a millisecond timeout.
///
/// Returns how many entries have a non-zero `revents`. `EINTR` is retried
/// rather than surfaced, because a signal is not an answer to the question.
pub fn poll(fds: &mut [libc::pollfd], timeout_ms: i32) -> io::Result<usize> {
    loop {
        // SAFETY: we pass the slice's own pointer and length, so poll reads and
        // writes only entries we own.
        let n = unsafe { libc::poll(fds.as_mut_ptr(), fds.len() as libc::nfds_t, timeout_ms) };
        if n >= 0 {
            return Ok(n as usize);
        }
        let err = io::Error::last_os_error();
        if err.raw_os_error() == Some(libc::EINTR) {
            continue;
        }
        return Err(err);
    }
}

fn sockaddr_v4(a: &SocketAddrV4) -> libc::sockaddr_in {
    // SAFETY: sockaddr_in is plain old data; all-zero is a valid starting
    // value and every field that matters is set below.
    let mut sa: libc::sockaddr_in = unsafe { mem::zeroed() };
    #[cfg(any(target_os = "macos", target_os = "ios", target_os = "freebsd"))]
    {
        sa.sin_len = mem::size_of::<libc::sockaddr_in>() as u8;
    }
    sa.sin_family = libc::AF_INET as libc::sa_family_t;
    sa.sin_port = a.port().to_be();
    // `octets()` is already in network order, so reading it back as a native
    // u32 reproduces exactly the byte pattern `s_addr` wants.
    sa.sin_addr = libc::in_addr {
        s_addr: u32::from_ne_bytes(a.ip().octets()),
    };
    sa
}

fn sockaddr_v6(a: &SocketAddrV6) -> libc::sockaddr_in6 {
    // SAFETY: as above — plain old data, zeroed then filled in.
    let mut sa: libc::sockaddr_in6 = unsafe { mem::zeroed() };
    #[cfg(any(target_os = "macos", target_os = "ios", target_os = "freebsd"))]
    {
        sa.sin6_len = mem::size_of::<libc::sockaddr_in6>() as u8;
    }
    sa.sin6_family = libc::AF_INET6 as libc::sa_family_t;
    sa.sin6_port = a.port().to_be();
    sa.sin6_flowinfo = a.flowinfo();
    sa.sin6_addr = libc::in6_addr {
        s6_addr: a.ip().octets(),
    };
    sa.sin6_scope_id = a.scope_id();
    sa
}

/// The peer address of a connected socket.
pub fn peer_addr(fd: RawFd) -> io::Result<SocketAddr> {
    // SAFETY: sockaddr_storage is plain old data and exists to be a buffer of
    // this shape.
    let mut ss: libc::sockaddr_storage = unsafe { mem::zeroed() };
    let mut len = mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
    // SAFETY: `ss` and `len` are live; getpeername writes at most `len` bytes
    // into `ss`. Ignoring the returned length afterwards is sound only because
    // `ss` started fully zeroed, so any field the kernel did not write is a
    // zero rather than uninitialised memory.
    let r = unsafe { libc::getpeername(fd, std::ptr::from_mut(&mut ss).cast(), &mut len) };
    if r < 0 {
        return Err(io::Error::last_os_error());
    }
    match libc::c_int::from(ss.ss_family) {
        libc::AF_INET => {
            // SAFETY: the family field says this storage holds a sockaddr_in,
            // and sockaddr_storage is sized and aligned to hold one.
            let sa = unsafe { &*std::ptr::from_ref(&ss).cast::<libc::sockaddr_in>() };
            Ok(SocketAddr::V4(SocketAddrV4::new(
                std::net::Ipv4Addr::from(sa.sin_addr.s_addr.to_ne_bytes()),
                u16::from_be(sa.sin_port),
            )))
        }
        libc::AF_INET6 => {
            // SAFETY: as above, for a sockaddr_in6.
            let sa = unsafe { &*std::ptr::from_ref(&ss).cast::<libc::sockaddr_in6>() };
            Ok(SocketAddr::V6(SocketAddrV6::new(
                std::net::Ipv6Addr::from(sa.sin6_addr.s6_addr),
                u16::from_be(sa.sin6_port),
                sa.sin6_flowinfo,
                sa.sin6_scope_id,
            )))
        }
        other => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("socket bound to address family {other}"),
        )),
    }
}

/// Apply `TCP_NODELAY` to an accepted socket.
pub fn set_nodelay(fd: RawFd) -> io::Result<()> {
    set_flag(fd, libc::IPPROTO_TCP, libc::TCP_NODELAY, true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;
    use std::os::fd::AsRawFd;

    #[test]
    fn a_pipe_round_trips_bytes_and_reports_emptiness() {
        let (r, w) = pipe_pair().expect("a pipe");
        // Non-blocking, so an empty pipe answers rather than waiting.
        let mut buf = [0u8; 8];
        let err = read(r.as_raw_fd(), &mut buf).expect_err("nothing to read yet");
        assert_eq!(err.kind(), io::ErrorKind::WouldBlock);

        assert_eq!(write(w.as_raw_fd(), b"hello").expect("a write"), 5);
        assert_eq!(read(r.as_raw_fd(), &mut buf).expect("a read"), 5);
        assert_eq!(&buf[..5], b"hello");
    }

    #[test]
    fn a_connect_to_a_listening_socket_succeeds() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("a listener");
        let addr = listener.local_addr().expect("an address");
        let (fd, connected) = tcp_connect(addr).expect("a connect");
        if !connected {
            // Loopback usually completes at once, but a busy machine can leave
            // it in progress; either way `SO_ERROR` settles it.
            let mut poll_fds = [libc::pollfd {
                fd: fd.as_raw_fd(),
                events: libc::POLLOUT,
                revents: 0,
            }];
            assert!(poll(&mut poll_fds, 2000).expect("a poll") > 0, "connect stalled");
        }
        assert_eq!(socket_error(fd.as_raw_fd()).expect("SO_ERROR"), 0);
    }

    #[test]
    fn a_connect_to_a_closed_port_reports_the_refusal() {
        // Bind then drop, so the port is almost certainly free and refusing.
        let listener = TcpListener::bind("127.0.0.1:0").expect("a listener");
        let addr = listener.local_addr().expect("an address");
        drop(listener);

        let Ok((fd, connected)) = tcp_connect(addr) else {
            return; // refused synchronously, which is also a correct answer
        };
        if !connected {
            let mut poll_fds = [libc::pollfd {
                fd: fd.as_raw_fd(),
                events: libc::POLLOUT,
                revents: 0,
            }];
            poll(&mut poll_fds, 2000).expect("a poll");
        }
        // The refusal shows up here, not in `connect`, which is the whole
        // reason the helper thread exists.
        assert_eq!(
            socket_error(fd.as_raw_fd()).expect("SO_ERROR"),
            libc::ECONNREFUSED
        );
    }

    #[test]
    fn a_fresh_socket_is_nonblocking() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("a listener");
        let (fd, _) = tcp_connect(listener.local_addr().expect("an address")).expect("a connect");
        // SAFETY: reading the flag word of a descriptor we own.
        let flags = unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_GETFL) };
        assert!(flags & libc::O_NONBLOCK != 0, "the reactor requires this");
    }

    #[test]
    fn a_socket_pair_carries_a_descriptor_number() {
        let (a, b) = socket_pair().expect("a pair");
        set_nonblocking(b.as_raw_fd()).expect("non-blocking");
        assert_eq!(write(a.as_raw_fd(), &42i32.to_le_bytes()).expect("write"), 4);
        let mut buf = [0u8; 4];
        assert_eq!(read(b.as_raw_fd(), &mut buf).expect("read"), 4);
        assert_eq!(i32::from_le_bytes(buf), 42);
    }
}
