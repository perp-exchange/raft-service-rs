use crate::application::ApplicationConfig;
use crate::pb::internal::AppendEntriesResponse as PbAppendEntriesResponse;
use crate::raft::config::type_config::AppendEntriesResponse;

impl<C> From<PbAppendEntriesResponse> for AppendEntriesResponse<C>
where
    C: ApplicationConfig,
{
    fn from(resp: PbAppendEntriesResponse) -> Self {
        if let Some(higher) = resp.rejected_by {
            return AppendEntriesResponse::HigherVote(higher);
        }

        if resp.conflict {
            return AppendEntriesResponse::Conflict;
        }

        if let Some(log_id) = resp.last_log_id {
            AppendEntriesResponse::PartialSuccess(Some(log_id.into()))
        } else {
            AppendEntriesResponse::Success
        }
    }
}

impl<C> From<AppendEntriesResponse<C>> for PbAppendEntriesResponse
where
    C: ApplicationConfig,
{
    fn from(resp: AppendEntriesResponse<C>) -> Self {
        match resp {
            AppendEntriesResponse::Success => Self {
                rejected_by: None,
                conflict: false,
                last_log_id: None,
            },
            AppendEntriesResponse::PartialSuccess(log_id) => Self {
                rejected_by: None,
                conflict: false,
                last_log_id: log_id.map(Into::into),
            },
            AppendEntriesResponse::Conflict => Self {
                rejected_by: None,
                conflict: true,
                last_log_id: None,
            },
            AppendEntriesResponse::HigherVote(vote) => Self {
                rejected_by: Some(vote),
                conflict: false,
                last_log_id: None,
            },
        }
    }
}
