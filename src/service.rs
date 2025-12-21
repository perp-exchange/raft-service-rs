use tokio_util::sync::CancellationToken;
use tonic::async_trait;

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
