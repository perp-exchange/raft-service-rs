use openraft::AnyError;
use openraft::OptionalSend;
use openraft::error::NetworkError;
use openraft::error::ReplicationClosed;
use openraft::error::Unreachable;
use openraft::network::RPCOption;
use openraft::network::v2::RaftNetworkV2;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;
use tonic::transport::Channel;

use crate::application::ApplicationConfig;
use crate::pb::internal::SnapshotRequest;
use crate::pb::internal::SnapshotRequestMeta;
use crate::pb::internal::raft_service_client::RaftServiceClient;
use crate::pb::internal::snapshot_request::Payload;
use crate::raft::config::type_config::AppendEntriesRequest;
use crate::raft::config::type_config::AppendEntriesResponse;
use crate::raft::config::type_config::Node;
use crate::raft::config::type_config::RPCError;
use crate::raft::config::type_config::Snapshot;
use crate::raft::config::type_config::SnapshotResponse;
use crate::raft::config::type_config::StreamingError;
use crate::raft::config::type_config::TypeConfig;
use crate::raft::config::type_config::Vote;
use crate::raft::config::type_config::VoteRequest;
use crate::raft::config::type_config::VoteResponse;

async fn new_client<C>(rpc_addr: &str) -> Result<RaftServiceClient<Channel>, RPCError<C>>
where
    C: ApplicationConfig,
{
    let channel = match Channel::builder(format!("http://{rpc_addr}").parse().unwrap())
        .connect()
        .await
    {
        Ok(channel) => channel,
        Err(e) => {
            return Err(RPCError::Unreachable(Unreachable::new(&e)));
        }
    };

    Ok(RaftServiceClient::new(channel))
}

pub struct GRPCNetworkConnection<C>
where
    C: ApplicationConfig,
{
    target_node: Node<C>,
}

impl<C> GRPCNetworkConnection<C>
where
    C: ApplicationConfig,
{
    pub fn new(target_node: Node<C>) -> Self {
        Self { target_node }
    }
}

impl<C> RaftNetworkV2<TypeConfig<C>> for GRPCNetworkConnection<C>
where
    C: ApplicationConfig,
{
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<C>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<C>, RPCError<C>> {
        let mut client = new_client(&self.target_node.rpc_addr).await?;

        let request = Request::new(rpc.into());

        let response = client
            .append_entries(request)
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?
            .into_inner();

        Ok(response.into())
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<C>,
        _option: RPCOption,
    ) -> Result<VoteResponse<C>, RPCError<C>> {
        let mut client = new_client(&self.target_node.rpc_addr).await?;

        let request = Request::new(rpc.into());

        // Send the vote request
        let response = client
            .vote(request)
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?
            .into_inner();

        Ok(response.into())
    }

    async fn full_snapshot(
        &mut self,
        vote: Vote<C>,
        snapshot: Snapshot<C>,
        _cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<C>, StreamingError<C>> {
        let mut client = new_client(&self.target_node.rpc_addr).await?;

        let (tx, rx) = tokio::sync::mpsc::channel(1024);
        let strm = ReceiverStream::new(rx);

        let response = client
            .snapshot(strm)
            .await
            .map_err(|e| NetworkError::new(&e))?;

        // 1. Send meta chunk
        {
            let meta = &snapshot.meta;

            let request = SnapshotRequest {
                payload: Some(Payload::Meta(SnapshotRequestMeta {
                    vote: Some(vote),
                    last_log_id: meta.last_log_id.map(Into::into),
                    last_membership_log_id: meta.last_membership.log_id().map(Into::into),
                    last_membership: Some(meta.last_membership.membership().clone().into()),
                    snapshot_id: meta.snapshot_id.clone(),
                })),
            };

            tx.send(request).await.map_err(|e| NetworkError::new(&e))?;
        }

        // 2. Send data chunks
        {
            let chunk_size = 1024 * 1024;
            for chunk in snapshot.snapshot.chunks(chunk_size) {
                let request = SnapshotRequest {
                    payload: Some(Payload::Chunk(chunk.to_vec())),
                };

                tx.send(request).await.map_err(|e| NetworkError::new(&e))?;
            }
        }

        // 3. receive response
        let message = response.into_inner();

        Ok(SnapshotResponse {
            vote: message.vote.ok_or_else(|| {
                NetworkError::new(&AnyError::error("Missing `vote` in snapshot response"))
            })?,
        })
    }
}
