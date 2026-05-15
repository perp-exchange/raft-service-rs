use std::io;
use std::net::AddrParseError;

use openraft::ConfigError;
use openraft::error::Fatal;
use thiserror::Error;

use crate::application::ApplicationConfig;
use crate::raft::config::type_config::ClientWriteError;
use crate::raft::config::type_config::InitializeError;
use crate::raft::config::type_config::LinearizableReadError;
use crate::raft::config::type_config::RaftError as OpenRaftError;
use crate::raft::config::type_config::TypeConfig;

#[derive(Error, Debug)]
pub enum RaftError<Config>
where
    Config: ApplicationConfig,
{
    #[error("Io error: {0}")]
    Io(#[from] io::Error),

    #[error("Config error: {0}")]
    Config(#[from] ConfigError),

    #[error("Fatal error: {0}")]
    Fatal(#[from] Fatal<TypeConfig<Config>>),

    #[error("Raft initialize error: {0}")]
    InitializeError(#[from] OpenRaftError<Config, InitializeError<Config>>),

    #[error("Raft client write error: {0}")]
    ClientWriteError(#[from] OpenRaftError<Config, ClientWriteError<Config>>),

    #[error("Raft linearizable read error: {0}")]
    LinearizableReadError(#[from] OpenRaftError<Config, LinearizableReadError<Config>>),

    #[error("Unknown error: {0}")]
    Unknown(Box<dyn std::error::Error + Send + Sync>),
}

#[derive(Error, Debug)]
pub enum RaftOrchestratorError<Config>
where
    Config: ApplicationConfig,
{
    #[error("Parse socket addr err: {0}")]
    ParseSocketAddr(#[from] AddrParseError),

    #[error("Raft err: {0}")]
    Raft(#[from] RaftError<Config>),

    #[error("Orchestration err: {0}")]
    Orchestration(Box<dyn std::error::Error + Send + Sync>),

    #[error("Application err: {0}")]
    Application(Box<dyn std::error::Error + Send + Sync>),
}
