use crate::raft::config::type_config::ClientWriteError;
use crate::raft::config::type_config::LinearizableReadError;
use crate::raft::config::type_config::RaftError;

pub type ReadError<C> = RaftError<C, LinearizableReadError<C>>;
pub type WriteError<C> = RaftError<C, ClientWriteError<C>>;
