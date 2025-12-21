use std::fmt::Debug;
use std::fmt::Display;
use std::sync::Arc;

use prost::DecodeError;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tonic::async_trait;

use crate::server::RaftDataClient;
use crate::service::LeaderLifecycleServiceBuilder;
use crate::service::StaticLifecycleServiceBuilder;

pub trait ApplicationConfig: Debug + Copy + Default + Ord + Send + Sync + 'static {
    type Request: Debug + Display + Serialize + DeserializeOwned + Send + Sync + 'static;
    type Response: Serialize + DeserializeOwned + Send + Sync + 'static;
    type Snapshot: Serialize + DeserializeOwned;
}

#[async_trait]
pub trait ApplicationStateMachine: Default + Send + Sync + 'static {
    type C: ApplicationConfig;

    fn export(&self) -> <Self::C as ApplicationConfig>::Snapshot;
    fn import(snapshot: <Self::C as ApplicationConfig>::Snapshot) -> Result<Self, DecodeError>;
    async fn apply(
        &mut self,
        request: <Self::C as ApplicationConfig>::Request,
    ) -> anyhow::Result<<Self::C as ApplicationConfig>::Response>;
}

#[async_trait]
pub trait ApplicationLayer: Sized + Send + Sync + 'static {
    type R: ApplicationStateMachine;
    type Config;

    async fn new(config: Self::Config, sm: RaftDataClient<Self::R>) -> anyhow::Result<Self>;

    fn leader_lifecycle_service_builder(&self) -> Vec<Arc<dyn LeaderLifecycleServiceBuilder>>;

    fn static_lifecycle_service_builder(&self) -> Vec<Arc<dyn StaticLifecycleServiceBuilder>>;

    async fn shutdown(self) -> anyhow::Result<()>;
}
