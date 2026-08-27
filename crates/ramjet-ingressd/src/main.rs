//! `ramjet-ingressd` is the daemon entry point for ramjet-ingress.
//!
//! Argument parsing, config loading, and wiring the controller and proxy
//! together land in a later phase. For now this only reports its version.

fn main() {
    println!("{} {}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"));
}
