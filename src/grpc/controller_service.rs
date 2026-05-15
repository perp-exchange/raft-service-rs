use std::collections::BTreeMap;

use openraft::async_runtime::WatchReceiver;
use tonic::Request;
use tonic::Response;
use tonic::Status;
use tonic::async_trait;
use tracing::debug;

use crate::application::ApplicationConfig;
use crate::pb::controller::InitRequest;
use crate::pb::controller::raft_controller_service_server::RaftControllerService;
use crate::pb::{self};
use crate::raft::config::type_config::Node;
use crate::raft::config::type_config::Raft;

pub struct RaftControllerServiceImpl<C>
where
    C: ApplicationConfig,
{
    raft_node: Raft<C>,
}

impl<C> RaftControllerServiceImpl<C>
where
    C: ApplicationConfig,
{
    pub fn new(raft_node: Raft<C>) -> Self {
        RaftControllerServiceImpl { raft_node }
    }
}

#[async_trait]
impl<C> RaftControllerService for RaftControllerServiceImpl<C>
where
    C: ApplicationConfig,
{
    /// Initializes a new Raft cluster with the specified nodes
    ///
    /// # Arguments
    /// * `request` - Contains the initial set of nodes for the cluster
    ///
    /// # Returns
    /// * Success response with initialization details
    /// * Error if initialization fails
    async fn init(&self, request: Request<InitRequest>) -> Result<Response<()>, Status> {
        debug!("Initializing Raft cluster");
        let req = request.into_inner();

        // Convert nodes into required format
        let nodes_map: BTreeMap<u64, pb::common::Node> = req
            .nodes
            .into_iter()
            .map(|node| (node.node_id, node))
            .collect();

        // Initialize the cluster
        let result = self
            .raft_node
            .initialize(nodes_map)
            .await
            .map_err(|e| Status::internal(format!("Failed to initialize cluster: {e}")))?;

        debug!("Cluster initialization successful");
        Ok(Response::new(result))
    }

    /// Adds a learner node to the Raft cluster
    ///
    /// # Arguments
    /// * `request` - Contains the node information and blocking preference
    ///
    /// # Returns
    /// * Success response with learner addition details
    /// * Error if the operation fails
    async fn add_learner(
        &self,
        request: Request<pb::controller::AddLearnerRequest>,
    ) -> Result<Response<pb::controller::ClientWriteResponse>, Status> {
        let req = request.into_inner();

        let node = req
            .node
            .ok_or_else(|| Status::internal("Node information is required"))?;

        debug!("Adding learner node {}", node.node_id);

        let raft_node = Node::<C> {
            rpc_addr: node.rpc_addr.clone(),
            node_id: node.node_id,
        };

        let result = self
            .raft_node
            .add_learner(node.node_id, raft_node, true)
            .await
            .map_err(|e| Status::internal(format!("Failed to add learner node: {e}")))?;

        debug!("Successfully added learner node {}", node.node_id);
        Ok(Response::new(result.into()))
    }

    /// Changes the membership of the Raft cluster
    ///
    /// # Arguments
    /// * `request` - Contains the new member set and retention policy
    ///
    /// # Returns
    /// * Success response with membership change details
    /// * Error if the operation fails
    async fn change_membership(
        &self,
        request: Request<pb::controller::ChangeMembershipRequest>,
    ) -> Result<Response<pb::controller::ClientWriteResponse>, Status> {
        let req = request.into_inner();

        debug!(
            "Changing membership. Members: {:?}, Retain: {}",
            req.members, req.retain
        );

        let result = self
            .raft_node
            .change_membership(req.members, req.retain)
            .await
            .map_err(|e| Status::internal(format!("Failed to change membership: {e}")))?;

        debug!("Successfully changed cluster membership");
        Ok(Response::new(result.into()))
    }

    /// Retrieves metrics about the Raft node
    async fn metrics(
        &self,
        _request: Request<()>,
    ) -> Result<Response<pb::controller::MetricsResponse>, Status> {
        debug!("Collecting metrics");
        let metrics = self.raft_node.metrics().borrow_watched().clone();
        let resp = pb::controller::MetricsResponse {
            membership: Some(metrics.membership_config.membership().clone().into()),
            other_metrics: metrics.to_string(),
        };
        Ok(Response::new(resp))
    }
}
