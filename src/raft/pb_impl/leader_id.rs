use std::cmp::Ordering;
use std::fmt;

use openraft::vote::RaftLeaderId;

use crate::application::ApplicationConfig;
use crate::pb::internal::LeaderId;
use crate::raft::config::type_config::NodeId;
use crate::raft::config::type_config::Term;
use crate::raft::config::type_config::TypeConfig;

impl PartialOrd for LeaderId {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        // LeaderIdCompare::<TypeConfig<C>>::std(self, other)
        match self.term.cmp(&other.term) {
            Ordering::Equal => {
                if self.node_id == other.node_id {
                    Some(Ordering::Equal)
                } else {
                    None
                }
            }
            cmp => Some(cmp),
        }
    }
}

impl PartialEq<u64> for LeaderId {
    fn eq(&self, _other: &u64) -> bool {
        false
    }
}

impl PartialOrd<u64> for LeaderId {
    fn partial_cmp(&self, other: &u64) -> Option<Ordering> {
        self.term.partial_cmp(other)
    }
}

impl fmt::Display for LeaderId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "T{}-N{}", self.term, self.node_id)
    }
}

impl<C> RaftLeaderId<TypeConfig<C>> for LeaderId
where
    C: ApplicationConfig,
{
    type Committed = u64;

    fn new(term: Term<C>, node_id: NodeId<C>) -> Self {
        Self { term, node_id }
    }

    fn term(&self) -> Term<C> {
        self.term
    }

    fn node_id(&self) -> &NodeId<C> {
        &self.node_id
    }

    fn to_committed(&self) -> Self::Committed {
        self.term
    }
}
