use std::fmt::Debug;
use std::marker::PhantomData;

use openraft::RaftTypeConfig;
use openraft::TokioRuntime;
use openraft::impls::OneshotResponder;
use openraft::type_config::alias::*;

use crate::application::ApplicationConfig;
use crate::pb;

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Ord, PartialOrd)]
pub struct TypeConfig<C: ApplicationConfig> {
    _mark: PhantomData<C>,
}

impl<C> RaftTypeConfig for TypeConfig<C>
where
    C: ApplicationConfig,
{
    type D = C::Request;
    type R = C::Response;
    type NodeId = u64;
    type Node = pb::common::Node;
    type Term = u64;
    type LeaderId = pb::internal::LeaderId;
    type Vote = pb::internal::Vote;
    type Entry = pb::internal::Entry;
    type SnapshotData = Vec<u8>;
    type AsyncRuntime = TokioRuntime;
    type Responder<T: Send + 'static> = OneshotResponder<Self, T>;
}

pub(crate) type Raft<C> = openraft::Raft<TypeConfig<C>>;
pub(crate) type LogId<C> = openraft::LogId<TypeConfig<C>>;
pub(crate) type StoredMembership<C> = openraft::StoredMembership<TypeConfig<C>>;
pub(crate) type Snapshot<C> = openraft::Snapshot<TypeConfig<C>>;
pub(crate) type StorageError<C> = openraft::StorageError<TypeConfig<C>>;
pub(crate) type SnapshotMeta<C> = openraft::SnapshotMeta<TypeConfig<C>>;
pub(crate) type Membership<C> = openraft::Membership<TypeConfig<C>>;

pub(crate) type SnapshotData<C> = SnapshotDataOf<TypeConfig<C>>;
pub(crate) type Term<C> = TermOf<TypeConfig<C>>;
pub(crate) type NodeId<C> = NodeIdOf<TypeConfig<C>>;
pub(crate) type Node<C> = NodeOf<TypeConfig<C>>;
pub(crate) type Vote<C> = VoteOf<TypeConfig<C>>;
pub(crate) type LeaderId<C> = LeaderIdOf<TypeConfig<C>>;

pub(crate) type VoteRequest<C> = openraft::raft::VoteRequest<TypeConfig<C>>;
pub(crate) type VoteResponse<C> = openraft::raft::VoteResponse<TypeConfig<C>>;
pub(crate) type AppendEntriesRequest<C> = openraft::raft::AppendEntriesRequest<TypeConfig<C>>;
pub(crate) type AppendEntriesResponse<C> = openraft::raft::AppendEntriesResponse<TypeConfig<C>>;
pub(crate) type SnapshotResponse<C> = openraft::raft::SnapshotResponse<TypeConfig<C>>;
pub(crate) type ClientWriteResponse<C> = openraft::raft::ClientWriteResponse<TypeConfig<C>>;

pub(crate) type RaftError<C, E> = openraft::error::RaftError<TypeConfig<C>, E>;
pub(crate) type RPCError<C> = openraft::error::RPCError<TypeConfig<C>>;
pub(crate) type StreamingError<C> = openraft::error::StreamingError<TypeConfig<C>>;
pub(crate) type ClientWriteError<C> = openraft::error::ClientWriteError<TypeConfig<C>>;
pub(crate) type LinearizableReadError<C> = openraft::error::LinearizableReadError<TypeConfig<C>>;

pub(crate) type EntryPayload<C> = openraft::entry::EntryPayload<TypeConfig<C>>;
