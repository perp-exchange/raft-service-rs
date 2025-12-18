pub trait LeaderLifecycleService: Send {
    fn on_leader_start(&mut self);

    fn on_leader_stop(&mut self);
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
