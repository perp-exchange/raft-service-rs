use std::io;
use std::path::Path;
use std::sync::Arc;

use rocksdb::ColumnFamilyDescriptor;
use rocksdb::DB;
use rocksdb::Options;

use crate::application::ApplicationStateMachine;
use crate::raft::config::type_config::TypeConfig;
use crate::raft::state_machine::store::StateMachineStore;
use crate::raft::store::log::RocksLogStore;

pub(crate) mod log;

pub const STORE_COLUMN: &str = "store";
pub const META_COLUMN: &str = "meta";
pub const LOGS_COLUMN: &str = "logs";

pub async fn new_storage<SM, P>(
    path: P,
) -> Result<(RocksLogStore<TypeConfig<SM::Config>>, StateMachineStore<SM>), io::Error>
where
    SM: ApplicationStateMachine,
    P: AsRef<Path>,
{
    let mut opts = Options::default();
    opts.create_missing_column_families(true);
    opts.create_if_missing(true);

    let cfs = vec![
        ColumnFamilyDescriptor::new(STORE_COLUMN, Options::default()),
        ColumnFamilyDescriptor::new(META_COLUMN, Options::default()),
        ColumnFamilyDescriptor::new(LOGS_COLUMN, Options::default()),
    ];

    let db = Arc::new(DB::open_cf_descriptors(&opts, path, cfs).map_err(io::Error::other)?);

    let rocks_log_store = RocksLogStore::new(db.clone());
    let state_machine_store = StateMachineStore::new(db.clone()).await?;

    Ok((rocks_log_store, state_machine_store))
}
