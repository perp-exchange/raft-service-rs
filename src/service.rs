use std::sync::Arc;

use tokio_util::sync::CancellationToken;
use tonic::async_trait;

#[async_trait]
pub trait LeaderLifecycleService: Send {
    async fn on_leader_start(&self, shutdown: Arc<CancellationToken>);
}

pub trait StaticLifecycleService: Send {
    fn on_start(&mut self);

    fn on_shutdown(&mut self);
}

pub trait LeaderLifecycleServiceBuilder: Send + Sync {
    fn build(&self) -> Box<dyn LeaderLifecycleService>;
}

pub trait StaticLifecycleServiceBuilder: Send + Sync {
    fn build(&self) -> Box<dyn StaticLifecycleService>;
}
