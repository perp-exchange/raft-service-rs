use crate::application::ApplicationConfig;
use crate::pb::internal::VoteResponse as PbVoteResponse;
use crate::raft::config::type_config::VoteResponse;

impl<C> From<PbVoteResponse> for VoteResponse<C>
where
    C: ApplicationConfig,
{
    fn from(resp: PbVoteResponse) -> Self {
        VoteResponse::new(
            resp.vote.unwrap(),
            resp.last_log_id.map(Into::into),
            resp.vote_granted,
        )
    }
}

impl<C> From<VoteResponse<C>> for PbVoteResponse
where
    C: ApplicationConfig,
{
    fn from(resp: VoteResponse<C>) -> Self {
        Self {
            vote: Some(resp.vote),
            vote_granted: resp.vote_granted,
            last_log_id: resp.last_log_id.map(Into::into),
        }
    }
}
