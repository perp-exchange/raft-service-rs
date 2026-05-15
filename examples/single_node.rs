use std::time::Duration;

use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::info;
use tracing_subscriber::EnvFilter;

use crate::application::Application;

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

    let path = tempfile::tempdir()?;

    {
        info!("Run application with single node first time");

        let application =
            Application::new(1, "127.0.0.1:21000".to_string(), path.path().to_path_buf());
        let shutdown = CancellationToken::new();
        let handle = application.run(shutdown.clone()).await?;
        sleep(Duration::from_secs(60)).await;
        shutdown.cancel();
        handle.await??;
    }

    {
        info!("Run application with single node second time");

        let application =
            Application::new(1, "127.0.0.1:21000".to_string(), path.path().to_path_buf());
        let shutdown = CancellationToken::new();
        let handle = application.run(shutdown.clone()).await?;
        sleep(Duration::from_secs(5)).await;
        shutdown.cancel();
        handle.await??;
    }

    Ok(())
}

mod writing_service {
    use std::time::Duration;

    use raft_service_rs::application::LeaderLifecycleService;
    use raft_service_rs::application::LeaderLifecycleServiceBuilder;
    use raft_service_rs::server::RaftDataClient;
    use tokio::time::sleep;
    use tokio_util::sync::CancellationToken;
    use tonic::async_trait;
    use tracing::error;
    use tracing::info;

    use crate::application::KeyValueData;
    use crate::application::Request;

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
                    key: start % 10,
                    value: vec![0; 4096],
                };

                info!(
                    node_id = raft_client.node_id(),
                    request.key, "Write to raft cluster"
                );

                if let Err(e) = raft_client.write(request).await {
                    error!(?e);
                }

                sleep(Duration::from_millis(5)).await;
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
        fn name(&self) -> &'static str {
            "writing_service"
        }

        fn build(&self) -> Box<dyn LeaderLifecycleService> {
            Box::new(WritingService {
                raft_client: self.raft_client.clone(),
            })
        }
    }
}

mod application {
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::Arc;

    use raft_service_rs::Node;
    use raft_service_rs::application::ApplicationConfig;
    use raft_service_rs::application::ApplicationLayer;
    use raft_service_rs::application::ApplicationStateMachine;
    use raft_service_rs::application::LeaderLifecycleServiceBuilder;
    use raft_service_rs::application::StaticLifecycleServiceBuilder;
    use raft_service_rs::error::RaftOrchestratorError;
    use raft_service_rs::orchestrator::RaftOrchestrator;
    use raft_service_rs::server::RaftDataClient;
    use raft_service_rs::server::RaftServiceConfig;
    use serde::Deserialize;
    use serde::Serialize;
    use thiserror::Error;
    use tokio::task::JoinHandle;
    use tokio_util::sync::CancellationToken;
    use tonic::async_trait;
    use tracing::info;

    use crate::writing_service::WritingServiceBuilder;

    #[derive(Debug, Clone, Copy, Default, Ord, PartialOrd, Eq, PartialEq)]
    pub struct KeyValueConfig;

    #[derive(Debug, derive_more::Display, Serialize, Deserialize)]
    #[display("self")]
    pub struct Request {
        pub key: u64,
        pub value: Vec<u8>,
    }

    #[derive(Serialize, Deserialize)]
    pub struct Response;

    #[derive(Default, Clone, Serialize, Deserialize)]
    pub struct Snapshot {
        map: HashMap<u64, Vec<u8>>,
    }

    impl ApplicationConfig for KeyValueConfig {
        type Request = Request;
        type Response = Response;
        type Snapshot = Snapshot;
    }

    #[derive(Default)]
    pub struct KeyValueData {
        map: HashMap<u64, Vec<u8>>,
    }

    #[derive(Error, Debug)]
    pub enum StateMachineError {}

    #[async_trait]
    impl ApplicationStateMachine for KeyValueData {
        type Config = KeyValueConfig;
        type Error = StateMachineError;

        fn export(&self) -> Result<Snapshot, Self::Error> {
            Ok(Snapshot {
                map: self.map.clone(),
            })
        }

        fn import(snapshot: Snapshot) -> Result<Self, Self::Error> {
            Ok(KeyValueData { map: snapshot.map })
        }

        async fn apply(&mut self, request: Request) -> Result<Response, Self::Error> {
            self.map.insert(request.key, request.value);

            Ok(Response)
        }
    }

    struct KeyValueService {
        writing_service_builder: Arc<WritingServiceBuilder>,
    }

    #[derive(Error, Debug)]
    enum Error {}

    #[async_trait]
    impl ApplicationLayer for KeyValueService {
        type Config = ();
        type StateMachine = KeyValueData;
        type Error = Error;

        async fn new(
            _config: Self::Config,
            raft_client: RaftDataClient<Self::StateMachine>,
        ) -> Result<Self, Self::Error> {
            Ok(KeyValueService {
                writing_service_builder: Arc::new(WritingServiceBuilder::new(raft_client.clone())),
            })
        }

        fn leader_lifecycle_service_builder(&self) -> Vec<Arc<dyn LeaderLifecycleServiceBuilder>> {
            vec![self.writing_service_builder.clone() as Arc<dyn LeaderLifecycleServiceBuilder>]
        }

        fn static_lifecycle_service_builder(&self) -> Vec<Arc<dyn StaticLifecycleServiceBuilder>> {
            vec![]
        }

        async fn shutdown(self) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    pub struct Application {
        raft_config: RaftServiceConfig,
    }

    impl Application {
        pub fn new(node_id: u64, rpc_url: String, log_path: PathBuf) -> Self {
            let raft_config = RaftServiceConfig {
                node_id,
                rpc_url,
                log_path,
            };

            Self { raft_config }
        }

        pub async fn run(
            self,
            shutdown: CancellationToken,
        ) -> anyhow::Result<JoinHandle<Result<(), RaftOrchestratorError<KeyValueConfig>>>> {
            let orchestrator =
                RaftOrchestrator::<KeyValueService>::new(self.raft_config.clone(), ()).await?;

            let controller_client = orchestrator.get_controller_client();

            let handle = tokio::spawn(orchestrator.run(shutdown));

            if !controller_client.is_initialized().await? {
                info!("Initialize cluster with single node");
                controller_client
                    .initialize(HashMap::from([(
                        self.raft_config.node_id,
                        Node {
                            node_id: self.raft_config.node_id,
                            rpc_addr: self.raft_config.rpc_url,
                        },
                    )]))
                    .await?;
            }

            Ok(handle)
        }
    }
}
