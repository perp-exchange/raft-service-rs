use crate::application::ApplicationConfig;
use crate::pb::internal::AppendEntriesRequest as PbAppendEntriesRequest;
use crate::raft::config::type_config::AppendEntriesRequest;

impl<C> From<AppendEntriesRequest<C>> for PbAppendEntriesRequest
where
    C: ApplicationConfig,
{
    fn from(req: AppendEntriesRequest<C>) -> Self {
        Self {
            vote: Some(req.vote),
            prev_log_id: req.prev_log_id.map(Into::into),
            entries: req.entries,
            leader_commit: req.leader_commit.map(Into::into),
        }
    }
}

impl<C> From<PbAppendEntriesRequest> for AppendEntriesRequest<C>
where
    C: ApplicationConfig,
{
    fn from(req: PbAppendEntriesRequest) -> Self {
        Self {
            vote: req.vote.unwrap(),
            prev_log_id: req.prev_log_id.map(Into::into),
            entries: req.entries,
            leader_commit: req.leader_commit.map(Into::into),
        }
    }
}
