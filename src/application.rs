use std::fmt::Debug;
use std::fmt::Display;
use std::sync::Arc;

use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio_util::sync::CancellationToken;
use tonic::async_trait;

use crate::server::RaftDataClient;

pub trait ApplicationConfig: Debug + Copy + Default + Ord + Send + Sync + 'static {
    type Request: Debug + Display + Serialize + DeserializeOwned + Send + Sync + 'static;
    type Response: Serialize + DeserializeOwned + Send + Sync + 'static;
    type Snapshot: Serialize + DeserializeOwned;
}

#[async_trait]
pub trait ApplicationStateMachine: Default + Send + Sync + 'static {
    type Config: ApplicationConfig;
    type Error: std::error::Error + Send + Sync;

    fn export(&self) -> Result<<Self::Config as ApplicationConfig>::Snapshot, Self::Error>;

    fn import(snapshot: <Self::Config as ApplicationConfig>::Snapshot)
    -> Result<Self, Self::Error>;

    async fn apply(
        &mut self,
        request: <Self::Config as ApplicationConfig>::Request,
    ) -> Result<<Self::Config as ApplicationConfig>::Response, Self::Error>;
}

#[async_trait]
pub trait LeaderLifecycleService: Send {
    async fn on_leader_start(&self, shutdown: CancellationToken);
}

#[async_trait]
pub trait StaticLifecycleService: Send {
    async fn start(&mut self, shutdown: CancellationToken);
}

pub trait LeaderLifecycleServiceBuilder: Send + Sync {
    fn name(&self) -> &'static str;
    fn build(&self) -> Box<dyn LeaderLifecycleService>;
}

pub trait StaticLifecycleServiceBuilder: Send + Sync {
    fn name(&self) -> &'static str;
    fn build(&self) -> Box<dyn StaticLifecycleService>;
}

#[async_trait]
pub trait ApplicationLayer: Sized + Send + Sync + 'static {
    type Config;
    type StateMachine: ApplicationStateMachine;
    type Error: std::error::Error + Send + Sync;

    async fn new(
        config: Self::Config,
        sm: RaftDataClient<Self::StateMachine>,
    ) -> Result<Self, Self::Error>;

    fn leader_lifecycle_service_builder(&self) -> Vec<Arc<dyn LeaderLifecycleServiceBuilder>>;

    fn static_lifecycle_service_builder(&self) -> Vec<Arc<dyn StaticLifecycleServiceBuilder>>;

    async fn shutdown(self) -> Result<(), Self::Error>;
}
