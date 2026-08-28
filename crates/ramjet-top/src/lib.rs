//! `ramjet-top`: a live view of a running ramjet-ingress.
//!
//! The daemon exposes what it is doing over an admin port — the compiled route
//! table with per-route counters, the history of generations that produced it,
//! and a Prometheus page. All three are perfectly good to `curl`, and none of
//! them answers the question somebody actually has at the moment they ask it,
//! which is "what is happening right now, and did the config I just pushed make
//! it worse?".
//!
//! This crate is the difference between those. It polls, differences the
//! counters into rates, and draws them.
//!
//! # Layout
//!
//! The split is between what can be tested without a terminal and what cannot,
//! and almost everything is in the first group:
//!
//! - [`contract`] — the admin JSON, as types.
//! - [`prom`] — the handful of series read out of `/metrics`.
//! - [`client`] — the three requests that make one [`Snapshot`](client::Snapshot).
//! - [`model`] — two snapshots differenced into the numbers on the screen.
//! - [`rfc3339`] — enough date handling to say how old a generation is.
//! - [`plain`] — the `--once` output, for scripts and CI.
//! - [`app`] — the state a session accumulates: history, sort, filter.
//! - [`ui`] — drawing, and the only module that needs a terminal.
//! - [`args`] — the command line.

#![deny(missing_docs)]

pub mod app;
pub mod args;
pub mod client;
pub mod contract;
pub mod model;
pub mod plain;
pub mod prom;
pub mod rfc3339;
pub mod ui;

pub use client::{AdminClient, ClientError, Snapshot};
