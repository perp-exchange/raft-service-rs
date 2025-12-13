pub mod application;
pub mod error;
pub mod orchestrator;
pub mod server;
pub mod service;

pub(crate) mod grpc;
pub(crate) mod raft;

pub mod pb {
    pub mod controller {
        tonic::include_proto!("controller");
    }

    pub(crate) mod common {
        tonic::include_proto!("common");
    }

    pub(crate) mod internal {
        tonic::include_proto!("internal");
    }
}

pub use pb::common::Node;
pub use pb::controller;
