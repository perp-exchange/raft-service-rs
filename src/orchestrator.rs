use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::Context;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;
use tracing::debug;

use crate::application::ApplicationLayer;
use crate::application::ApplicationStateMachine;
use crate::grpc::controller_service::RaftControllerServiceImpl;
use crate::grpc::internal_service::RaftServiceImpl;
use crate::pb::controller::raft_controller_service_server::RaftControllerServiceServer;
use crate::pb::internal::raft_service_server::RaftServiceServer;
use crate::server::RaftControlClient;
use crate::server::RaftServer;
use crate::server::RaftServiceConfig;

async fn leader_services_shutdown(
    leader_lifecycle_services: Arc<
        Mutex<Option<(CancellationToken, BTreeMap<&'static str, JoinHandle<()>>)>>,
    >,
) {
    if let Some((shutdown, handles)) = leader_lifecycle_services.lock().await.take() {
        debug!("Shutting down leader lifecycle services");
        shutdown.cancel();

        for (name, handle) in handles {
            handle.await.unwrap();
            debug!(name, "Leader lifecycle service stopped");
        }
    }
}

pub struct RaftOrchestrator<A: ApplicationLayer> {
    application_config: A::Config,
    raft_config: RaftServiceConfig,
    raft_server: RaftServer<A::R>,
}

impl<A> RaftOrchestrator<A>
where
    A: ApplicationLayer,
{
    pub async fn new(
        raft_config: RaftServiceConfig,
        application_config: A::Config,
    ) -> anyhow::Result<Self> {
        let raft_server = RaftServer::new_from_config(&raft_config)
            .await
            .context("Failed to create raft_server")?;

        Ok(RaftOrchestrator {
            application_config,
            raft_config,
            raft_server,
        })
    }

    pub async fn get_controller_client(
        &self,
    ) -> RaftControlClient<<A::R as ApplicationStateMachine>::C> {
        self.raft_server.control_client()
    }

    pub async fn run(self, shutdown_token: CancellationToken) -> anyhow::Result<()> {
        let data_client = self.raft_server.data_client();
        let raft = self.raft_server.raft;

        let application = A::new(self.application_config, data_client).await?;

        let mut static_lifecycle_services = JoinSet::new();
        for builder in application.static_lifecycle_service_builder() {
            let mut svc = builder.build();
            let shutdown_token = shutdown_token.clone();
            static_lifecycle_services.spawn(async move { svc.start(shutdown_token).await });

            debug!(name = builder.name(), "Static lifecycle service started");
        }

        let leader_lifecycle_services = Arc::new(Mutex::new(None));

        let mut handle = {
            let leader_lifecycle_services = leader_lifecycle_services.clone();

            raft.on_leader_change(
                {
                    let leader_lifecycle_services = leader_lifecycle_services.clone();
                    let leader_lifecycle_service_builder =
                        application.leader_lifecycle_service_builder();

                    move |leader_id| {
                        let leader_lifecycle_services = leader_lifecycle_services.clone();
                        let leader_lifecycle_service_builder =
                            leader_lifecycle_service_builder.clone();

                        async move {
                            debug!(?leader_id, "Became leader");

                            let shutdown = CancellationToken::new();
                            let mut handles = BTreeMap::new();

                            for builder in &leader_lifecycle_service_builder {
                                let name = builder.name();
                                let builder = builder.clone();
                                let svc = builder.build();
                                let shutdown = shutdown.clone();
                                handles.insert(
                                    name,
                                    tokio::spawn(
                                        async move { svc.on_leader_start(shutdown).await },
                                    ),
                                );
                                debug!(name, "Leader lifecycle service started");
                            }

                            *leader_lifecycle_services.lock().await = Some((shutdown, handles));
                        }
                    }
                },
                {
                    move |old_leader_id| {
                        let leader_lifecycle_services = leader_lifecycle_services.clone();

                        async move {
                            debug!(?old_leader_id, "Stepped down from leader");

                            leader_services_shutdown(leader_lifecycle_services).await;
                        }
                    }
                },
            )
        };

        // Raft internal service and control service
        let internal = tokio::spawn({
            let shutdown_token = shutdown_token.clone();
            let addr = self.raft_config.rpc_url.parse()?;
            let raft = raft.clone();
            async move {
                Server::builder()
                    .add_service(RaftServiceServer::new(RaftServiceImpl::new(raft.clone())))
                    .add_service(RaftControllerServiceServer::new(
                        RaftControllerServiceImpl::new(raft),
                    ))
                    .serve_with_shutdown(addr, shutdown_token.cancelled())
                    .await?;

                anyhow::Ok(())
            }
        });

        shutdown_token.cancelled().await;

        // Shutdown leader lifecycle services
        leader_services_shutdown(leader_lifecycle_services).await;
        handle.close().await;

        // Shutdown static lifecycle services
        while let Some(res) = static_lifecycle_services.join_next().await {
            if let Err(e) = res {
                debug!("Static lifecycle service failed: {:?}", e);
            }
        }

        application.shutdown().await?;

        internal.await??;
        raft.shutdown().await?;

        Ok(())
    }
}
