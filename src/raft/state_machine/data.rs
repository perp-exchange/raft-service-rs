use std::sync::Arc;

use tokio::sync::RwLock;

use crate::application::ApplicationStateMachine;
use crate::raft::config::type_config::LogId;
use crate::raft::config::type_config::StoredMembership;

pub struct StateMachineData<A>
where
    A: ApplicationStateMachine,
{
    pub(crate) last_applied_log: Option<LogId<A::C>>,
    pub(crate) last_membership: StoredMembership<A::C>,
    pub(crate) application_data: Arc<RwLock<A>>,
}

impl<A> Clone for StateMachineData<A>
where
    A: ApplicationStateMachine,
{
    fn clone(&self) -> Self {
        Self {
            last_applied_log: self.last_applied_log.clone(),
            last_membership: self.last_membership.clone(),
            application_data: self.application_data.clone(),
        }
    }
}
