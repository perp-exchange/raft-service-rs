use std::io;
use std::sync::Arc;

use futures_util::Stream;
use futures_util::TryStreamExt;
use openraft::AnyError;
use openraft::OptionalSend;
use openraft::entry::RaftEntry;
use openraft::storage::EntryResponder;
use openraft::storage::RaftStateMachine;
use rocksdb::ColumnFamily;
use rocksdb::DB;

use crate::application::ApplicationConfig;
use crate::application::ApplicationStateMachine;
use crate::raft::config::type_config::LogId;
use crate::raft::config::type_config::Snapshot;
use crate::raft::config::type_config::SnapshotData;
use crate::raft::config::type_config::SnapshotMeta;
use crate::raft::config::type_config::StorageError;
use crate::raft::config::type_config::StoredMembership;
use crate::raft::config::type_config::TypeConfig;
use crate::raft::state_machine::data::StateMachineData;
use crate::raft::state_machine::snapshot::StoredSnapshot;
use crate::raft::store::log::STORE_COLUMN;

pub struct StateMachineStore<A>
where
    A: ApplicationStateMachine,
{
    pub(crate) state_machine: StateMachineData<A>,
    pub(crate) snapshot_idx: u64,
    pub(crate) db: Arc<DB>,
}

impl<A> Clone for StateMachineStore<A>
where
    A: ApplicationStateMachine,
{
    fn clone(&self) -> Self {
        Self {
            state_machine: self.state_machine.clone(),
            snapshot_idx: self.snapshot_idx,
            db: self.db.clone(),
        }
    }
}

impl<A> StateMachineStore<A>
where
    A: ApplicationStateMachine,
{
    pub async fn new(db: Arc<DB>) -> Result<Self, io::Error> {
        let mut sm = Self {
            state_machine: StateMachineData {
                last_applied_log: None,
                last_membership: Default::default(),
                application_data: Arc::new(Default::default()),
            },
            snapshot_idx: 0,
            db,
        };

        let snapshot = sm.get_current_snapshot_()?;
        if let Some(snap) = snapshot {
            sm.update_state_machine_(snap).await?;
        }

        Ok(sm)
    }

    async fn update_state_machine_(
        &mut self,
        snapshot: StoredSnapshot<A::C>,
    ) -> Result<(), io::Error> {
        self.state_machine.last_applied_log = snapshot.meta.last_log_id;
        self.state_machine.last_membership = snapshot.meta.last_membership.clone();
        {
            let data: <A::C as ApplicationConfig>::Snapshot =
                serde_json::from_slice(&snapshot.data)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            let data =
                A::import(data).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

            let mut application_data = self.state_machine.application_data.write().await;
            *application_data = data;
        }

        Ok(())
    }

    fn get_current_snapshot_(&self) -> Result<Option<StoredSnapshot<A::C>>, io::Error> {
        Ok(self
            .db
            .get_cf(self.store(), b"snapshot")
            .map_err(io::Error::other)?
            .and_then(|v| serde_json::from_slice(&v).ok()))
    }

    pub fn set_current_snapshot_(&self, snap: StoredSnapshot<A::C>) -> Result<(), io::Error> {
        self.db
            .put_cf(
                self.store(),
                b"snapshot",
                serde_json::to_vec(&snap).unwrap().as_slice(),
            )
            .map_err(io::Error::other)?;
        self.db.flush_wal(true).map_err(io::Error::other)?;
        Ok(())
    }

    fn store(&self) -> &ColumnFamily {
        self.db.cf_handle(STORE_COLUMN).unwrap()
    }
}

impl<A> RaftStateMachine<TypeConfig<A::C>> for StateMachineStore<A>
where
    A: ApplicationStateMachine,
{
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<A::C>>, StoredMembership<A::C>), io::Error> {
        Ok((
            self.state_machine.last_applied_log,
            self.state_machine.last_membership.clone(),
        ))
    }

    async fn apply<Strm>(&mut self, mut entries: Strm) -> Result<(), io::Error>
    where
        Strm: Stream<Item = Result<EntryResponder<TypeConfig<A::C>>, io::Error>>
            + Unpin
            + OptionalSend,
    {
        while let Some((entry, response)) = entries.try_next().await? {
            let log_id = entry.log_id();

            self.state_machine.last_applied_log = Some(log_id);

            let value = if let Some(req) = entry.app_data {
                let req = serde_json::from_slice(req.as_slice())
                    .map_err(|e| StorageError::apply(log_id, &e))?;

                let response = self
                    .state_machine
                    .application_data
                    .write()
                    .await
                    .apply(req)
                    .await
                    .map_err(|e| StorageError::apply(log_id, AnyError::error(e.to_string())))?;

                Some(response)
            } else if let Some(membership) = entry.membership {
                self.state_machine.last_membership =
                    StoredMembership::new(Some(log_id), membership.into());

                None
            } else {
                None
            };

            if let Some(response) = response
                && let Some(value) = value
            {
                response.send(value);
            }
        }

        Ok(())
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.snapshot_idx += 1;
        self.clone()
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<SnapshotData<A::C>, io::Error> {
        Ok(Default::default())
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<A::C>,
        snapshot: SnapshotData<A::C>,
    ) -> Result<(), io::Error> {
        let new_snapshot = StoredSnapshot {
            meta: meta.clone(),
            data: snapshot,
        };

        self.update_state_machine_(new_snapshot.clone()).await?;

        self.set_current_snapshot_(new_snapshot)?;

        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<A::C>>, io::Error> {
        let x = self.get_current_snapshot_()?;
        Ok(x.map(|s| Snapshot {
            meta: s.meta.clone(),
            snapshot: s.data.clone(),
        }))
    }
}
