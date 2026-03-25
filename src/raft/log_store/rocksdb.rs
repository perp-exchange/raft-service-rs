use std::error::Error;
use std::fmt::Debug;
use std::io;
use std::marker::PhantomData;
use std::ops::RangeBounds;
use std::path::Path;
use std::sync::Arc;

use byteorder::BigEndian;
use byteorder::ReadBytesExt;
use byteorder::WriteBytesExt;
use openraft::LogState;
use openraft::OptionalSend;
use openraft::RaftLogReader;
use openraft::RaftTypeConfig;
use openraft::entry::RaftEntry;
use openraft::storage::IOFlushed;
use openraft::storage::RaftLogStorage;
use openraft::type_config::alias::EntryOf;
use openraft::type_config::alias::LogIdOf;
use openraft::type_config::alias::VoteOf;
use rocksdb::ColumnFamily;
use rocksdb::ColumnFamilyDescriptor;
use rocksdb::DB;
use rocksdb::Direction;
use rocksdb::Options;
use tokio::task::spawn_blocking;

use crate::raft::log_store::rocksdb::meta::StoreMeta;

const META_COLUMN: &str = "meta";
const LOGS_COLUMN: &str = "logs";

#[derive(Clone)]
pub(in crate::raft) struct RocksLogStore<C> {
    db: Arc<DB>,
    _mark: PhantomData<C>,
}

impl<C: RaftTypeConfig> RocksLogStore<C> {
    pub(in crate::raft) fn new<P: AsRef<Path>>(path: P) -> Result<Self, io::Error> {
        if path.as_ref().exists() {
            println!("Opening existing RocksDB at {:?}", path.as_ref());
        } else {
            println!("Creating new RocksDB at {:?}", path.as_ref());
        }

        let mut opts = Options::default();
        opts.create_missing_column_families(true);
        opts.create_if_missing(true);

        let cfs = vec![
            ColumnFamilyDescriptor::new(META_COLUMN, Options::default()),
            ColumnFamilyDescriptor::new(LOGS_COLUMN, Options::default()),
        ];

        let db = Arc::new(DB::open_cf_descriptors(&opts, path, cfs).map_err(io::Error::other)?);

        Ok(RocksLogStore {
            db,
            _mark: PhantomData,
        })
    }

    fn cf_meta(&self) -> &ColumnFamily {
        self.db.cf_handle(META_COLUMN).unwrap()
    }

    fn cf_logs(&self) -> &ColumnFamily {
        self.db.cf_handle(LOGS_COLUMN).unwrap()
    }

    fn get_meta<M: StoreMeta<C>>(&self) -> Result<Option<M::Value>, io::Error> {
        let bytes = self
            .db
            .get_cf(self.cf_meta(), M::KEY)
            .map_err(|e| io::Error::other(e.to_string()))?;

        let Some(bytes) = bytes else {
            return Ok(None);
        };

        let t = serde_json::from_slice(&bytes)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        Ok(Some(t))
    }

    fn put_meta<M: StoreMeta<C>>(&self, value: &M::Value) -> Result<(), io::Error> {
        let json_value =
            serde_json::to_vec(value).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        self.db
            .put_cf(self.cf_meta(), M::KEY, json_value)
            .map_err(|e| io::Error::other(e.to_string()))?;

        Ok(())
    }
}

impl<C: RaftTypeConfig> RaftLogReader<C> for RocksLogStore<C> {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<EntryOf<C>>, io::Error> {
        let start = match range.start_bound() {
            std::ops::Bound::Included(x) => id_to_bin(*x),
            std::ops::Bound::Excluded(x) => id_to_bin(*x + 1),
            std::ops::Bound::Unbounded => id_to_bin(0),
        };

        let mut res = Vec::new();

        let it = self.db.iterator_cf(
            self.cf_logs(),
            rocksdb::IteratorMode::From(&start, Direction::Forward),
        );
        for iter_res in it {
            let (id, val) = iter_res.map_err(read_logs_err)?;

            let id = bin_to_id(&id);
            if !range.contains(&id) {
                break;
            }

            let entry: EntryOf<C> = serde_json::from_slice(&val).map_err(read_logs_err)?;

            assert_eq!(id, entry.index());

            res.push(entry);
        }

        Ok(res)
    }

    async fn read_vote(&mut self) -> Result<Option<VoteOf<C>>, io::Error> {
        self.get_meta::<meta::Vote>()
    }
}

impl<C: RaftTypeConfig> RaftLogStorage<C> for RocksLogStore<C> {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<C>, io::Error> {
        let last = self
            .db
            .iterator_cf(self.cf_logs(), rocksdb::IteratorMode::End)
            .next();

        let last_log_id = match last {
            None => None,
            Some(res) => {
                let (_log_index, entry_bytes) = res.map_err(read_logs_err)?;
                let ent =
                    serde_json::from_slice::<EntryOf<C>>(&entry_bytes).map_err(read_logs_err)?;
                Some(ent.log_id())
            }
        };

        let last_purged_log_id = self.get_meta::<meta::LastPurged>()?;

        let last_log_id = match last_log_id {
            None => last_purged_log_id.clone(),
            Some(x) => Some(x),
        };

        Ok(LogState {
            last_purged_log_id,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &VoteOf<C>) -> Result<(), io::Error> {
        self.put_meta::<meta::Vote>(vote)?;

        let db = self.db.clone();
        spawn_blocking(move || db.flush_wal(true))
            .await
            .map_err(|e| io::Error::other(e.to_string()))?
            .map_err(|e| io::Error::other(e.to_string()))?;

        Ok(())
    }

    async fn append<I>(&mut self, entries: I, callback: IOFlushed<C>) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = EntryOf<C>> + Send,
    {
        for entry in entries {
            let id = id_to_bin(entry.index());
            self.db
                .put_cf(
                    self.cf_logs(),
                    id,
                    serde_json::to_vec(&entry)
                        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?,
                )
                .map_err(|e| io::Error::other(e.to_string()))?;
        }

        let db = self.db.clone();
        let handle = spawn_blocking(move || {
            let res = db.flush_wal(true).map_err(io::Error::other);
            callback.io_completed(res);
        });
        drop(handle);

        Ok(())
    }

    async fn truncate(&mut self, log_id: LogIdOf<C>) -> Result<(), io::Error> {
        let from = id_to_bin(log_id.index());
        let to = id_to_bin(u64::MAX);
        self.db
            .delete_range_cf(self.cf_logs(), &from, &to)
            .map_err(|e| io::Error::other(e.to_string()))?;

        Ok(())
    }

    async fn purge(&mut self, log_id: LogIdOf<C>) -> Result<(), io::Error> {
        self.put_meta::<meta::LastPurged>(&log_id)?;

        let from = id_to_bin(0);
        let to = id_to_bin(log_id.index() + 1);
        self.db
            .delete_range_cf(self.cf_logs(), &from, &to)
            .map_err(|e| io::Error::other(e.to_string()))?;

        Ok(())
    }
}

mod meta {
    use openraft::RaftTypeConfig;
    use openraft::type_config::alias::LogIdOf;
    use openraft::type_config::alias::VoteOf;
    use serde::Serialize;
    use serde::de::DeserializeOwned;

    pub trait StoreMeta<C: RaftTypeConfig> {
        const KEY: &'static str;

        type Value: Serialize + DeserializeOwned;
    }

    pub struct LastPurged {}
    pub struct Vote {}

    impl<C> StoreMeta<C> for LastPurged
    where
        C: RaftTypeConfig,
    {
        const KEY: &'static str = "last_purged_log_id";
        type Value = LogIdOf<C>;
    }

    impl<C> StoreMeta<C> for Vote
    where
        C: RaftTypeConfig,
    {
        const KEY: &'static str = "vote";
        type Value = VoteOf<C>;
    }
}

fn id_to_bin(id: u64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(8);
    buf.write_u64::<BigEndian>(id).unwrap();
    buf
}

fn bin_to_id(buf: &[u8]) -> u64 {
    (&buf[0..8]).read_u64::<BigEndian>().unwrap()
}

fn read_logs_err(e: impl Error + 'static) -> io::Error {
    io::Error::other(e.to_string())
}
