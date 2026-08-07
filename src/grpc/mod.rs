//! The generated gRPC types, shared by the server and the client.
//!
//! `pb` is compiled from the vendored `spec/a2a.proto` by `build.rs`, and
//! `convert` maps between it and this crate's own [`types`](crate::types).
//! Both live here rather than under [`server`](crate::server) so that a gRPC
//! *client* does not have to enable the server to use them.
//!
//! `convert` is only compiled when something needs it — a build with `grpc`
//! alone gets the generated types and nothing else.

pub mod pb {
    #![allow(clippy::doc_lazy_continuation)]
    tonic::include_proto!("lf.a2a.v1");
}

#[cfg(any(feature = "server", feature = "client"))]
pub(crate) mod convert;
