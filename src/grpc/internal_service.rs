use futures_util::StreamExt;
use tonic::Request;
use tonic::Response;
use tonic::Status;
use tonic::Streaming;
use tonic::async_trait;

use crate::application::ApplicationConfig;
use crate::pb::internal::raft_service_server::RaftService;
use crate::pb::internal::*;
use crate::raft::config::type_config::Raft;
use crate::raft::config::type_config::Snapshot;
use crate::raft::config::type_config::SnapshotMeta;
use crate::raft::config::type_config::StoredMembership;

pub struct RaftServiceImpl<C>(Raft<C>)
where
    C: ApplicationConfig;

impl<C> RaftServiceImpl<C>
where
    C: ApplicationConfig,
{
    pub fn new(raft_node: Raft<C>) -> Self {
        RaftServiceImpl(raft_node)
    }
}

#[async_trait]
impl<C> RaftService for RaftServiceImpl<C>
where
    C: ApplicationConfig,
{
    /// Vote handles vote requests between Raft nodes during leader election
    async fn vote(&self, request: Request<VoteRequest>) -> Result<Response<VoteResponse>, Status> {
        let response = self
            .0
            .vote(request.into_inner().into())
            .await
            .map_err(|e| Status::internal(format!("Vote operation failed: {e}")))?;

        Ok(Response::new(response.into()))
    }

    /// `AppendEntries` handles call related to append entries RPC
    async fn append_entries(
        &self,
        request: Request<AppendEntriesRequest>,
    ) -> Result<Response<AppendEntriesResponse>, Status> {
        let response = self
            .0
            .append_entries(request.into_inner().into())
            .await
            .map_err(|e| Status::internal(format!("Append entries operation failed: {e}")))?;

        Ok(Response::new(response.into()))
    }

    /// Snapshot handles install snapshot RPC
    async fn snapshot(
        &self,
        request: Request<Streaming<SnapshotRequest>>,
    ) -> Result<Response<SnapshotResponse>, Status> {
        let mut stream = request.into_inner();

        // Get the first chunk which contains metadata
        let first_chunk = stream
            .next()
            .await
            .ok_or_else(|| Status::invalid_argument("Empty snapshot stream"))??;

        let vote;
        let snapshot_meta;
        {
            let meta = first_chunk
                .into_meta()
                .ok_or_else(|| Status::invalid_argument("First snapshot chunk must be metadata"))?;

            vote = meta.vote.unwrap();

            snapshot_meta = SnapshotMeta {
                last_log_id: meta.last_log_id.map(Into::into),
                last_membership: StoredMembership::new(
                    meta.last_membership_log_id.map(Into::into),
                    meta.last_membership.unwrap().into(),
                ),
                snapshot_id: meta.snapshot_id,
            };
        }

        let mut snapshot_data_bytes = Vec::new();

        while let Some(chunk) = stream.next().await {
            let data = chunk?
                .into_data_chunk()
                .ok_or_else(|| Status::invalid_argument("Snapshot chunk must be data"))?;
            snapshot_data_bytes.extend_from_slice(&data);
        }

        let snapshot = Snapshot {
            meta: snapshot_meta,
            snapshot: snapshot_data_bytes,
        };

        let resp = self
            .0
            .install_full_snapshot(vote, snapshot)
            .await
            .map_err(|e| Status::internal(format!("Snapshot installation failed: {e}")))?;

        Ok(Response::new(SnapshotResponse {
            vote: Some(resp.vote),
        }))
    }
}
