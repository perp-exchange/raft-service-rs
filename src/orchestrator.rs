use std::sync::Arc;
use std::sync::Mutex;

use anyhow::Context;
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

        let mut application = A::new(self.application_config, data_client).await?;

        let mut static_lifecycle_services = vec![];

        for (name, builder) in application.static_lifecycle_service_builder() {
            let mut svc = builder.build();
            svc.on_start().await;
            static_lifecycle_services.push((name.clone(), svc));

            debug!(name, "Static lifecycle service started");
        }

        let mut handle = {
            let leader_lifecycle_services = Arc::new(Mutex::new(vec![]));
            let leader_lifecycle_service_builder = application.leader_lifecycle_service_builder();

            raft.on_leader_change(
                {
                    let leader_lifecycle_services = leader_lifecycle_services.clone();

                    move |leader_id| {
                        debug!(?leader_id, "Became leader");

                        let mut leader_lifecycle_services =
                            leader_lifecycle_services.lock().unwrap();

                        for (name, builder) in &leader_lifecycle_service_builder {
                            let mut svc = builder.build();
                            svc.on_leader_start();
                            leader_lifecycle_services.push((name.clone(), svc));

                            debug!(name, "Leader lifecycle service started");
                        }
                    }
                },
                {
                    move |old_leader_id| {
                        debug!(?old_leader_id, "Stepped down from leader");

                        let mut leader_lifecycle_services =
                            leader_lifecycle_services.lock().unwrap();

                        for (name, mut svc) in leader_lifecycle_services.drain(..) {
                            svc.on_leader_stop();

                            debug!(name, "Leader lifecycle service stopped");
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
        handle.close().await;

        // Shutdown static lifecycle services
        for (name, mut svc) in static_lifecycle_services {
            svc.on_shutdown().await;
            debug!(name, "Static lifecycle service stopped");
        }

        application.shutdown().await?;

        internal.await??;
        raft.shutdown().await?;

        Ok(())
    }
}
