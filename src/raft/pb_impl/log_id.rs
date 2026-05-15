use crate::application::ApplicationConfig;
use crate::pb::common::LogId as PbLogId;
use crate::raft::config::type_config::LogId;

impl<C> From<LogId<C>> for PbLogId
where
    C: ApplicationConfig,
{
    fn from(log_id: LogId<C>) -> Self {
        Self {
            term: *log_id.committed_leader_id(),
            index: log_id.index,
        }
    }
}

impl<C> From<PbLogId> for LogId<C>
where
    C: ApplicationConfig,
{
    fn from(log_id: PbLogId) -> Self {
        Self {
            leader_id: log_id.term,
            index: log_id.index,
        }
    }
}
