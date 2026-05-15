use std::collections::BTreeMap;

use crate::application::ApplicationConfig;
use crate::pb::common::Membership as PbMembership;
use crate::pb::common::NodeIdSet;
use crate::raft::config::type_config::Membership;

impl<C> From<PbMembership> for Membership<C>
where
    C: ApplicationConfig,
{
    fn from(membership: PbMembership) -> Self {
        let mut configs = vec![];
        for c in membership.configs {
            let config = c.node_ids.into_keys().collect();
            configs.push(config);
        }

        Membership::new(configs, membership.nodes).unwrap()
    }
}

impl<C> From<Membership<C>> for PbMembership
where
    C: ApplicationConfig,
{
    fn from(membership: Membership<C>) -> Self {
        let mut configs = vec![];
        for c in membership.get_joint_config() {
            let mut node_ids = BTreeMap::new();
            for nid in c.iter() {
                node_ids.insert(*nid, ());
            }
            configs.push(NodeIdSet { node_ids });
        }

        let nodes = membership
            .nodes()
            .map(|(nid, n)| (*nid, n.clone()))
            .collect();

        PbMembership { configs, nodes }
    }
}
