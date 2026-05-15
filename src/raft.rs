use std::path::Path;
use std::sync::Arc;

use openraft::Config;
use tokio::sync::RwLock;

use crate::application::ApplicationStateMachine;
use crate::error::RaftError;
use crate::raft::config::type_config::NodeId;
use crate::raft::config::type_config::Raft;
use crate::raft::network::grpc::GRPCNetwork;
use crate::raft::store::new_storage;

pub(crate) mod config;

mod network;
mod pb_impl;
mod state_machine;
mod store;

pub async fn new_raft<A>(
    node_id: NodeId<A::Config>,
    path: &Path,
) -> Result<(Arc<RwLock<A>>, Raft<A::Config>), RaftError<A::Config>>
where
    A: ApplicationStateMachine,
{
    let config = Arc::new(
        Config {
            heartbeat_interval: 500,
            election_timeout_min: 1500,
            election_timeout_max: 3000,
            ..Default::default()
        }
        .validate()?,
    );

    let network = GRPCNetwork::default();

    let (log_store, state_machine) = new_storage(path).await?;

    Ok((
        state_machine.state_machine.application_data.clone(),
        openraft::Raft::new(node_id, config, network, log_store, state_machine).await?,
    ))
}

#[cfg(test)]
mod tests {
    use openraft::testing::log::StoreBuilder;
    use openraft::testing::log::Suite;
    use serde::Deserialize;
    use serde::Serialize;
    use tempfile::TempDir;
    use thiserror::Error;
    use tonic::async_trait;

    use crate::application::ApplicationConfig;
    use crate::application::ApplicationStateMachine;
    use crate::raft::config::type_config::StorageError;
    use crate::raft::config::type_config::TypeConfig;
    use crate::raft::state_machine::store::StateMachineStore;
    use crate::raft::store::log::RocksLogStore;
    use crate::raft::store::new_storage;

    #[derive(Error, Debug)]
    enum Error {}

    #[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
    struct MockApplication {}

    #[derive(Debug, derive_more::Display, Serialize, Deserialize)]
    struct Request {}

    #[derive(Debug, Serialize, Deserialize)]
    struct Response;

    #[derive(Clone, Default, Serialize, Deserialize)]
    struct Snapshot;

    impl ApplicationConfig for MockApplication {
        type Request = Request;
        type Response = Response;
        type Snapshot = Snapshot;
    }

    #[async_trait]
    impl ApplicationStateMachine for MockApplication {
        type Config = MockApplication;
        type Error = Error;

        fn export(&self) -> Result<Snapshot, Self::Error> {
            Ok(Snapshot)
        }

        fn import(_snapshot: Snapshot) -> Result<Self, Self::Error> {
            Ok(MockApplication::default())
        }

        async fn apply(&mut self, _request: Request) -> Result<Response, Self::Error> {
            Ok(Response)
        }
    }

    struct RocksStoreBuilder;

    impl
        StoreBuilder<
            TypeConfig<MockApplication>,
            RocksLogStore<TypeConfig<MockApplication>>,
            StateMachineStore<MockApplication>,
            (),
        > for RocksStoreBuilder
    {
        async fn build(
            &self,
        ) -> Result<
            (
                (),
                RocksLogStore<TypeConfig<MockApplication>>,
                StateMachineStore<MockApplication>,
            ),
            StorageError<MockApplication>,
        > {
            let tmp_dir = TempDir::new().map_err(|e| StorageError::read_logs(&e))?;

            let (log_store, state_machine) = new_storage(tmp_dir.path())
                .await
                .map_err(|e| StorageError::read_logs(&e))?;

            Ok(((), log_store, state_machine))
        }
    }

    #[tokio::test]
    async fn test_store_with_rocks() {
        Suite::test_all(RocksStoreBuilder).await.unwrap();
    }
}
