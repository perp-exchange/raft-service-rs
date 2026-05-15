use crate::application::ApplicationConfig;
use crate::pb::controller::ClientWriteResponse as PbClientWriteResponse;
use crate::raft::config::type_config::ClientWriteResponse;

impl<C> From<ClientWriteResponse<C>> for PbClientWriteResponse
where
    C: ApplicationConfig,
{
    fn from(resp: ClientWriteResponse<C>) -> Self {
        PbClientWriteResponse {
            log_id: Some(resp.log_id.into()),
            membership: resp.membership.map(Into::into),
        }
    }
}
