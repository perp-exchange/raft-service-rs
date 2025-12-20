use std::time::Duration;

use raft_service_rs::controller::AddLearnerRequest;
use raft_service_rs::controller::ChangeMembershipRequest;
use raft_service_rs::pb::controller::InitRequest;
use raft_service_rs::pb::controller::raft_controller_service_client::RaftControllerServiceClient;
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;
use tracing::info;
use tracing_subscriber::EnvFilter;

use crate::service::Application;
use crate::service::node_address;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_target(true)
        .with_thread_ids(true)
        .with_level(true)
        .with_ansi(false)
        .with_line_number(true)
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let raft1 = {
        let tmp = TempDir::new()?;
        Application::new(1, node_address(1).rpc_addr, tmp.path().to_path_buf()).await?
    };

    let raft2 = {
        let tmp = TempDir::new()?;
        Application::new(2, node_address(2).rpc_addr, tmp.path().to_path_buf()).await?
    };

    let raft3 = {
        let tmp = TempDir::new()?;
        Application::new(3, node_address(3).rpc_addr, tmp.path().to_path_buf()).await?
    };

    let shutdown = CancellationToken::new();

    let _h1 = tokio::spawn({
        let shutdown = shutdown.clone();
        async move { raft1.run(shutdown).await }
    });
    let _h2 = tokio::spawn({
        let shutdown = shutdown.clone();
        async move { raft2.run(shutdown).await }
    });
    let _h3 = tokio::spawn({
        let shutdown = shutdown.clone();
        async move { raft3.run(shutdown).await }
    });

    // Wait for server to start up.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let mut client1 =
        RaftControllerServiceClient::connect(format!("http://{}", node_address(1).rpc_addr))
            .await?;
    let response = client1
        .init(InitRequest {
            nodes: vec![node_address(1)],
        })
        .await?
        .into_inner();
    info!(?response);
    tokio::time::sleep(Duration::from_millis(500)).await;

    let response = client1
        .add_learner(AddLearnerRequest {
            node: Some(node_address(2)),
        })
        .await?
        .into_inner();
    info!(?response);
    tokio::time::sleep(Duration::from_millis(500)).await;

    let response = client1
        .add_learner(AddLearnerRequest {
            node: Some(node_address(3)),
        })
        .await?
        .into_inner();
    info!(?response);
    tokio::time::sleep(Duration::from_millis(500)).await;

    let response = client1
        .change_membership(ChangeMembershipRequest {
            members: vec![1, 2, 3],
            retain: false,
        })
        .await?
        .into_inner();
    info!(?response);
    tokio::time::sleep(Duration::from_millis(500)).await;

    shutdown.cancelled().await;

    Ok(())
}

mod service {
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::Arc;

    use prost::DecodeError;
    use raft_service_rs::Node;
    use raft_service_rs::application::ApplicationConfig;
    use raft_service_rs::application::ApplicationLayer;
    use raft_service_rs::application::ApplicationStateMachine;
    use raft_service_rs::orchestrator::RaftOrchestrator;
    use raft_service_rs::server::RaftDataClient;
    use raft_service_rs::server::RaftServiceConfig;
    use raft_service_rs::service::LeaderLifecycleServiceBuilder;
    use raft_service_rs::service::StaticLifecycleServiceBuilder;
    use serde::Deserialize;
    use serde::Serialize;
    use tokio_util::sync::CancellationToken;
    use tonic::async_trait;

    use crate::service::reading_service::ReadingServiceBuilder;
    use crate::service::writing_service::WritingServiceBuilder;

    pub fn node_address(id: u64) -> Node {
        Node {
            node_id: id,
            rpc_addr: format!("127.0.0.1:{}", 21000 + id),
        }
    }

    mod writing_service {
        use std::time::Duration;

        use raft_service_rs::server::RaftDataClient;
        use raft_service_rs::service::LeaderLifecycleService;
        use raft_service_rs::service::LeaderLifecycleServiceBuilder;
        use tokio::time::sleep;
        use tokio_util::sync::CancellationToken;
        use tonic::async_trait;
        use tracing::error;
        use tracing::info;

        use crate::service::KeyValueData;
        use crate::service::Request;

        pub struct WritingService {
            raft_client: RaftDataClient<KeyValueData>,
        }

        #[async_trait]
        impl LeaderLifecycleService for WritingService {
            async fn on_leader_start(&self, shutdown: CancellationToken) {
                let raft_client = self.raft_client.clone();

                let mut start = 0;

                while !shutdown.is_cancelled() {
                    let request = Request {
                        key: start.to_string(),
                        value: start.to_string(),
                    };

                    info!(node_id = raft_client.node_id(), ?request);

                    if let Err(e) = raft_client.write(request).await {
                        error!(?e);
                    }

                    sleep(Duration::from_secs(1)).await;
                    start += 1;
                }
            }
        }

        pub struct WritingServiceBuilder {
            raft_client: RaftDataClient<KeyValueData>,
        }

        impl WritingServiceBuilder {
            pub fn new(raft_client: RaftDataClient<KeyValueData>) -> Self {
                WritingServiceBuilder { raft_client }
            }
        }

        impl LeaderLifecycleServiceBuilder for WritingServiceBuilder {
            fn build(&self) -> Box<dyn LeaderLifecycleService> {
                Box::new(WritingService {
                    raft_client: self.raft_client.clone(),
                })
            }
        }
    }

    mod reading_service {
        use std::time::Duration;

        use raft_service_rs::server::RaftDataClient;
        use raft_service_rs::service::LeaderLifecycleService;
        use raft_service_rs::service::LeaderLifecycleServiceBuilder;
        use tokio::time::sleep;
        use tokio_util::sync::CancellationToken;
        use tonic::async_trait;
        use tracing::error;
        use tracing::info;

        use crate::service::KeyValueData;

        pub struct ReadingService {
            raft_client: RaftDataClient<KeyValueData>,
        }

        #[async_trait]
        impl LeaderLifecycleService for ReadingService {
            async fn on_leader_start(&self, shutdown: CancellationToken) {
                while !shutdown.is_cancelled() {
                    match self.raft_client.read_safe(|store| store.map.len()).await {
                        Ok(len) => {
                            info!(node_id = self.raft_client.node_id(), len);
                        }
                        Err(e) => {
                            error!(?e);
                        }
                    }

                    sleep(Duration::from_secs(1)).await;
                }
            }
        }

        pub struct ReadingServiceBuilder {
            raft_client: RaftDataClient<KeyValueData>,
        }

        impl ReadingServiceBuilder {
            pub fn new(raft_client: RaftDataClient<KeyValueData>) -> Self {
                ReadingServiceBuilder { raft_client }
            }
        }

        impl LeaderLifecycleServiceBuilder for ReadingServiceBuilder {
            fn build(&self) -> Box<dyn LeaderLifecycleService> {
                Box::new(ReadingService {
                    raft_client: self.raft_client.clone(),
                })
            }
        }
    }

    #[derive(Debug, derive_more::Display, Serialize, Deserialize)]
    #[display("self")]
    pub struct Request {
        key: String,
        value: String,
    }

    #[derive(Serialize, Deserialize)]
    pub struct Response;

    #[derive(Default, Clone, Serialize, Deserialize)]
    pub struct Snapshot {
        map: HashMap<String, String>,
    }

    #[derive(Debug, Clone, Copy, Default, Ord, PartialOrd, Eq, PartialEq)]
    pub struct KeyValueConfig;

    impl ApplicationConfig for KeyValueConfig {
        type Request = Request;
        type Response = Response;
        type Snapshot = Snapshot;
    }

    #[derive(Default)]
    pub struct KeyValueData {
        map: HashMap<String, String>,
    }

    #[async_trait]
    impl ApplicationStateMachine for KeyValueData {
        type C = KeyValueConfig;

        fn export(&self) -> Snapshot {
            Snapshot {
                map: self.map.clone(),
            }
        }

        fn import(snapshot: Snapshot) -> Result<Self, DecodeError> {
            Ok(KeyValueData { map: snapshot.map })
        }

        async fn apply(&mut self, request: Request) -> anyhow::Result<Response> {
            self.map.insert(request.key, request.value);

            Ok(Response)
        }
    }

    struct KeyValueService {
        writing_service_builder: Arc<WritingServiceBuilder>,
        reading_service_builder: Arc<ReadingServiceBuilder>,
    }

    #[async_trait]
    impl ApplicationLayer for KeyValueService {
        type R = KeyValueData;
        type Config = ();

        async fn new(
            _config: Self::Config,
            raft_client: RaftDataClient<Self::R>,
        ) -> anyhow::Result<Self> {
            Ok(KeyValueService {
                writing_service_builder: Arc::new(WritingServiceBuilder::new(raft_client.clone())),
                reading_service_builder: Arc::new(ReadingServiceBuilder::new(raft_client)),
            })
        }

        fn leader_lifecycle_service_builder(
            &mut self,
        ) -> Vec<(String, Arc<dyn LeaderLifecycleServiceBuilder>)> {
            vec![
                (
                    "writing_service".to_string(),
                    self.writing_service_builder.clone() as Arc<dyn LeaderLifecycleServiceBuilder>,
                ),
                (
                    "reading_service".to_string(),
                    self.reading_service_builder.clone() as Arc<dyn LeaderLifecycleServiceBuilder>,
                ),
            ]
        }

        fn static_lifecycle_service_builder(
            &mut self,
        ) -> Vec<(String, Arc<dyn StaticLifecycleServiceBuilder>)> {
            vec![]
        }

        async fn shutdown(self) -> anyhow::Result<()> {
            Ok(())
        }
    }

    pub struct Application {
        raft_config: RaftServiceConfig,
    }

    impl Application {
        pub async fn new(node_id: u64, rpc_url: String, log_path: PathBuf) -> anyhow::Result<Self> {
            let raft_config = RaftServiceConfig {
                node_id,
                rpc_url,
                log_path,
            };

            Ok(Self { raft_config })
        }

        pub async fn run(self, shutdown: CancellationToken) -> anyhow::Result<()> {
            let orchestrator =
                RaftOrchestrator::<KeyValueService>::new(self.raft_config, ()).await?;

            orchestrator.run(shutdown).await?;

            Ok(())
        }
    }
}
