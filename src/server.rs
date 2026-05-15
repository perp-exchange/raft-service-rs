use std::collections::HashMap;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use maplit::btreemap;
use openraft::ChangeMembers;
use serde::Deserialize;
use tokio::sync::RwLock;
use tracing::info;

use crate::application::ApplicationConfig;
use crate::application::ApplicationStateMachine;
use crate::raft::config::type_config::ClientWriteError;
use crate::raft::config::type_config::ClientWriteResponse;
use crate::raft::config::type_config::LinearizableReadError;
use crate::raft::config::type_config::Node;
use crate::raft::config::type_config::NodeId;
use crate::raft::config::type_config::Raft;
use crate::raft::config::type_config::RaftError;
use crate::raft::new_raft;

#[derive(Clone, Debug, Deserialize)]
pub struct RaftServiceConfig {
    pub node_id: u64,
    pub rpc_url: String,
    pub log_path: PathBuf,
}

pub struct RaftDataClient<A>
where
    A: ApplicationStateMachine,
{
    node_id: u64,
    application: Arc<RwLock<A>>,
    raft: Raft<A::C>,
}

impl<A> Clone for RaftDataClient<A>
where
    A: ApplicationStateMachine,
{
    fn clone(&self) -> Self {
        Self {
            node_id: self.node_id,
            application: self.application.clone(),
            raft: self.raft.clone(),
        }
    }
}

impl<A> RaftDataClient<A>
where
    A: ApplicationStateMachine,
{
    pub fn node_id(&self) -> u64 {
        self.node_id
    }

    pub async fn write(
        &self,
        request: <A::C as ApplicationConfig>::Request,
    ) -> Result<ClientWriteResponse<A::C>, RaftError<A::C, ClientWriteError<A::C>>> {
        self.raft.client_write(request).await
    }

    pub async fn read<R>(&self, f: impl FnOnce(&A) -> R) -> R {
        let application_data = self.application.read().await;

        f(&application_data)
    }

    pub async fn read_safe<R>(
        &self,
        f: impl FnOnce(&A) -> R,
    ) -> Result<R, RaftError<A::C, LinearizableReadError<A::C>>> {
        let ret = self
            .raft
            .get_read_linearizer(openraft::ReadPolicy::ReadIndex)
            .await;

        match ret {
            Ok(linearizer) => {
                linearizer.await_ready(&self.raft).await.unwrap();

                let application_data = self.application.read().await;

                Ok(f(&application_data))
            }
            Err(err) => Err(err),
        }
    }
}

pub struct RaftControlClient<C: ApplicationConfig> {
    node_id: u64,
    raft: Raft<C>,
}

impl<C> RaftControlClient<C>
where
    C: ApplicationConfig,
{
    pub fn node_id(&self) -> u64 {
        self.node_id
    }

    pub async fn is_initialized(&self) -> anyhow::Result<bool> {
        Ok(self.raft.is_initialized().await?)
    }

    pub async fn initialize(&self, members: HashMap<NodeId<C>, Node<C>>) -> anyhow::Result<()> {
        self.raft.initialize(members).await?;

        Ok(())
    }

    pub async fn add_learner(&self, id: NodeId<C>, node: Node<C>) -> anyhow::Result<()> {
        self.raft.add_learner(id, node, true).await?;

        Ok(())
    }

    pub async fn add_voter(&self, node: Node<C>) -> anyhow::Result<()> {
        self.raft
            .change_membership(
                ChangeMembers::AddVoters(btreemap! {
                    node.node_id => node
                }),
                false,
            )
            .await?;

        Ok(())
    }
}

pub struct RaftServer<A>
where
    A: ApplicationStateMachine,
{
    pub(crate) node_id: u64,
    pub(crate) application: Arc<RwLock<A>>,
    pub(crate) raft: Raft<A::C>,
}

impl<A> RaftServer<A>
where
    A: ApplicationStateMachine,
{
    pub async fn new_from_config(config: &RaftServiceConfig) -> anyhow::Result<Self> {
        Self::new(config.node_id, &config.log_path).await
    }

    pub async fn new(node_id: u64, log_store_path: &Path) -> anyhow::Result<Self> {
        info!(node_id, ?log_store_path, "Init raft");

        let (application, raft) = new_raft::<A>(node_id, log_store_path).await?;

        Ok(Self {
            node_id,
            application,
            raft,
        })
    }

    pub fn control_client(&self) -> RaftControlClient<A::C> {
        RaftControlClient {
            node_id: self.node_id,
            raft: self.raft.clone(),
        }
    }

    pub fn data_client(&self) -> RaftDataClient<A> {
        RaftDataClient {
            node_id: self.node_id,
            application: self.application.clone(),
            raft: self.raft.clone(),
        }
    }
}
