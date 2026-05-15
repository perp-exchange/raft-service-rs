use std::io;

use openraft::RaftSnapshotBuilder;
use serde::Deserialize;
use serde::Serialize;

use crate::application::ApplicationConfig;
use crate::application::ApplicationStateMachine;
use crate::raft::config::type_config::Snapshot;
use crate::raft::config::type_config::SnapshotData;
use crate::raft::config::type_config::SnapshotMeta;
use crate::raft::config::type_config::TypeConfig;
use crate::raft::state_machine::store::StateMachineStore;

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(bound = "")]
pub struct StoredSnapshot<C>
where
    C: ApplicationConfig,
{
    pub(crate) meta: SnapshotMeta<C>,
    pub(crate) data: SnapshotData<C>,
}

impl<A> RaftSnapshotBuilder<TypeConfig<A::C>> for StateMachineStore<A>
where
    A: ApplicationStateMachine,
{
    async fn build_snapshot(&mut self) -> Result<Snapshot<A::C>, io::Error> {
        let data = serde_json::to_vec(&&self.state_machine.application_data.read().await.export())?;
        let last_applied_log = self.state_machine.last_applied_log;
        let last_membership = self.state_machine.last_membership.clone();

        let snapshot_id = if let Some(last) = last_applied_log {
            format!(
                "{}-{}-{}",
                last.committed_leader_id(),
                last.index(),
                self.snapshot_idx
            )
        } else {
            format!("--{}", self.snapshot_idx)
        };

        let meta = SnapshotMeta {
            last_log_id: last_applied_log,
            last_membership,
            snapshot_id,
        };

        let snapshot = StoredSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        };

        self.set_current_snapshot_(snapshot)?;

        Ok(Snapshot {
            meta,
            snapshot: data,
        })
    }
}
