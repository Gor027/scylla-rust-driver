use super::{FallbackPlan, LoadBalancingPolicy, NodeRef, RoutingInfo};
use crate::{
    routing::Token,
    transport::{cluster::ClusterData, locator::ReplicaSet, node::Node, topology::Strategy},
};
use itertools::{Either, Itertools};
use rand::{prelude::SliceRandom, thread_rng, Rng};
use scylla_cql::{frame::types::SerialConsistency, Consistency};
use std::sync::Arc;
use tracing::warn;

// TODO: LWT optimisation
/// This is the default load balancing policy.
/// It can be configured to be datacenter-aware and token-aware.
/// Datacenter failover for queries with non local consistency mode is also supported.
#[derive(Debug)]
pub struct DefaultPolicy {
    preferred_datacenter: Option<String>,
    is_token_aware: bool,
    permit_dc_failover: bool,
}

impl LoadBalancingPolicy for DefaultPolicy {
    fn pick<'a>(&'a self, query: &'a RoutingInfo, cluster: &'a ClusterData) -> Option<NodeRef<'a>> {
        let routing_info = self.routing_info(query, cluster);
        if let Some(ref token_with_strategy) = routing_info.token_with_strategy {
            if self.preferred_datacenter.is_some()
                && !self.permit_dc_failover
                && matches!(
                    token_with_strategy.strategy,
                    Strategy::SimpleStrategy { .. }
                )
            {
                warn!("\
Combining SimpleStrategy with preferred_datacenter set to Some and disabled datacenter failover may lead to empty query plans for some tokens.\
It is better to give up using one of them: either operate in a keyspace with NetworkTopologyStrategy, which explicitly states\
how many replicas there are in each datacenter (you probably want at least 1 to avoid empty plans while preferring that datacenter), \
or refrain from preferring datacenters (which may ban all other datacenters, if datacenter failover happens to be not possible)."
                );
            }
        }
        if let Some(ts) = &routing_info.token_with_strategy {
            // Try to pick some alive local random replica.
            // If preferred datacenter is not specified, all replicas are treated as local.
            let picked = self.pick_replica(ts, true, Self::is_alive, cluster);
            if let Some(alive_local_replica) = picked {
                return Some(alive_local_replica);
            }

            // If datacenter failover is possible, loosen restriction about locality.
            if self.is_datacenter_failover_possible(&routing_info) {
                let picked = self.pick_replica(ts, false, Self::is_alive, cluster);
                if let Some(alive_remote_replica) = picked {
                    return Some(alive_remote_replica);
                }
            }
        };

        // If no token was available (or all the replicas for that token are down), try to pick
        // some alive local node.
        // If there was no preferred datacenter specified, all nodes are treated as local.
        let nodes = self.preferred_node_set(cluster);
        let picked = Self::pick_node(nodes, Self::is_alive);
        if let Some(alive_local) = picked {
            return Some(alive_local);
        }

        let all_nodes = cluster.replica_locator().unique_nodes_in_global_ring();
        // If a datacenter failover is possible, loosen restriction about locality.
        if self.is_datacenter_failover_possible(&routing_info) {
            let picked = Self::pick_node(all_nodes, Self::is_alive);
            if let Some(alive_maybe_remote) = picked {
                return Some(alive_maybe_remote);
            }
        }

        // Previous checks imply that every node we could have selected is down.
        // Let's try to return a down node that wasn't disabled.
        let picked = Self::pick_node(nodes, |node| node.is_enabled());
        if let Some(down_but_enabled_local_node) = picked {
            return Some(down_but_enabled_local_node);
        }

        // If a datacenter failover is possible, loosen restriction about locality.
        if self.is_datacenter_failover_possible(&routing_info) {
            let picked = Self::pick_node(all_nodes, |node| node.is_enabled());
            if let Some(down_but_enabled_maybe_remote_node) = picked {
                return Some(down_but_enabled_maybe_remote_node);
            }
        }

        // Every node is disabled. This could be due to a bad host filter - configuration error.
        nodes.first()
    }

    fn fallback<'a>(
        &'a self,
        query: &'a RoutingInfo,
        cluster: &'a ClusterData,
    ) -> FallbackPlan<'a> {
        let routing_info = self.routing_info(query, cluster);

        // If token is available, get a shuffled list of alive replicas.
        let maybe_replicas = if let Some(ts) = &routing_info.token_with_strategy {
            let local_replicas = self.shuffled_replicas(ts, true, Self::is_alive, cluster);

            // If a datacenter failover is possible, loosen restriction about locality.
            let maybe_remote_replicas = if self.is_datacenter_failover_possible(&routing_info) {
                let remote_replicas = self.shuffled_replicas(ts, false, Self::is_alive, cluster);
                Either::Left(remote_replicas)
            } else {
                Either::Right(std::iter::empty())
            };

            // Produce an iterator, prioritizes local replicas.
            // If preferred datacenter is not specified, every replica is treated as a local one.
            Either::Left(local_replicas.chain(maybe_remote_replicas))
        } else {
            Either::Right(std::iter::empty::<NodeRef<'a>>())
        };

        // Get a list of all local alive nodes, and apply a round robin to it
        let local_nodes = self.preferred_node_set(cluster);
        let robined_local_nodes = Self::round_robin_nodes(local_nodes, Self::is_alive);

        let all_nodes = cluster.replica_locator().unique_nodes_in_global_ring();

        // If a datacenter failover is possible, loosen restriction about locality.
        let maybe_remote_nodes = if self.is_datacenter_failover_possible(&routing_info) {
            let robined_all_nodes = Self::round_robin_nodes(all_nodes, Self::is_alive);

            Either::Left(robined_all_nodes)
        } else {
            Either::Right(std::iter::empty::<NodeRef<'a>>())
        };

        // Even if we consider some enabled nodes to be down, we should try contacting them in the last resort.
        let maybe_down_local_nodes = local_nodes.iter().filter(|node| node.is_enabled());

        // If a datacenter failover is possible, loosen restriction about locality.
        let maybe_down_nodes = if self.is_datacenter_failover_possible(&routing_info) {
            Either::Left(all_nodes.iter().filter(|node| node.is_enabled()))
        } else {
            Either::Right(std::iter::empty())
        };

        // Construct a fallback plan as a composition of replicas, local nodes and remote nodes.
        let plan = maybe_replicas
            .chain(robined_local_nodes)
            .chain(maybe_remote_nodes)
            .chain(maybe_down_local_nodes)
            .chain(maybe_down_nodes)
            .unique();

        Box::new(plan)
    }

    fn name(&self) -> String {
        "DefaultPolicy".to_string()
    }
}

impl DefaultPolicy {
    pub fn builder() -> DefaultPolicyBuilder {
        DefaultPolicyBuilder {
            preferred_datacenter: None,
            is_token_aware: true,
            permit_dc_failover: false,
        }
    }

    fn routing_info<'a>(
        &'a self,
        query: &'a RoutingInfo,
        cluster: &'a ClusterData,
    ) -> ProcessedRoutingInfo<'a> {
        let mut routing_info = ProcessedRoutingInfo::new(query, cluster);

        if !self.is_token_aware {
            routing_info.token_with_strategy = None;
        }

        routing_info
    }

    fn preferred_node_set<'a>(&'a self, cluster: &'a ClusterData) -> &'a [Arc<Node>] {
        if let Some(preferred_datacenter) = &self.preferred_datacenter {
            if let Some(nodes) = cluster
                .replica_locator()
                .unique_nodes_in_datacenter_ring(preferred_datacenter.as_str())
            {
                return nodes;
            } else {
                tracing::warn!(
                    "Datacenter specified as the preferred one ({}) does not exist!",
                    preferred_datacenter
                );
                // We won't guess any DC, as it could lead to possible violation dc failover ban.
                return &[];
            }
        }

        cluster.replica_locator().unique_nodes_in_global_ring()
    }

    fn nonfiltered_replica_set<'a>(
        &'a self,
        ts: &TokenWithStrategy<'a>,
        should_be_local: bool,
        cluster: &'a ClusterData,
    ) -> ReplicaSet<'a> {
        let datacenter = should_be_local
            .then_some(self.preferred_datacenter.as_deref())
            .flatten();

        cluster
            .replica_locator()
            .replicas_for_token(ts.token, ts.strategy, datacenter)
    }

    fn replicas<'a>(
        &'a self,
        ts: &TokenWithStrategy<'a>,
        should_be_local: bool,
        predicate: impl Fn(&NodeRef<'a>) -> bool,
        cluster: &'a ClusterData,
    ) -> impl Iterator<Item = NodeRef<'a>> {
        self.nonfiltered_replica_set(ts, should_be_local, cluster)
            .into_iter()
            .filter(predicate)
    }

    fn pick_replica<'a>(
        &'a self,
        ts: &TokenWithStrategy<'a>,
        should_be_local: bool,
        predicate: impl Fn(&NodeRef<'a>) -> bool,
        cluster: &'a ClusterData,
    ) -> Option<NodeRef<'a>> {
        self.nonfiltered_replica_set(ts, should_be_local, cluster)
            .choose_filtered(&mut thread_rng(), |node| predicate(&node))
    }

    fn shuffled_replicas<'a>(
        &'a self,
        ts: &TokenWithStrategy<'a>,
        should_be_local: bool,
        predicate: impl Fn(&NodeRef<'a>) -> bool,
        cluster: &'a ClusterData,
    ) -> impl Iterator<Item = NodeRef<'a>> {
        let replicas = self.replicas(ts, should_be_local, predicate, cluster);

        Self::shuffle(replicas)
    }

    fn randomly_rotated_nodes(nodes: &[Arc<Node>]) -> impl Iterator<Item = NodeRef<'_>> {
        // Create a randomly rotated slice view
        let nodes_len = nodes.len();
        if nodes_len > 0 {
            let index = rand::thread_rng().gen_range(0..nodes_len); // gen_range() panics when range is empty!
            Either::Left(
                nodes[index..]
                    .iter()
                    .chain(nodes[..index].iter())
                    .take(nodes.len()),
            )
        } else {
            Either::Right(std::iter::empty())
        }
    }

    fn pick_node<'a>(
        nodes: &'a [Arc<Node>],
        predicate: impl Fn(&NodeRef<'a>) -> bool,
    ) -> Option<NodeRef<'a>> {
        // Select the first node that matches the predicate
        Self::randomly_rotated_nodes(nodes).find(predicate)
    }

    fn round_robin_nodes<'a>(
        nodes: &'a [Arc<Node>],
        predicate: impl Fn(&NodeRef<'a>) -> bool,
    ) -> impl Iterator<Item = NodeRef<'a>> {
        Self::randomly_rotated_nodes(nodes).filter(predicate)
    }

    fn shuffle<'a>(iter: impl Iterator<Item = NodeRef<'a>>) -> impl Iterator<Item = NodeRef<'a>> {
        let mut vec: Vec<NodeRef<'a>> = iter.collect();

        let mut rng = thread_rng();
        vec.shuffle(&mut rng);

        vec.into_iter()
    }

    fn is_alive(node: &NodeRef<'_>) -> bool {
        // For now, we leave this as stub, until we have time to improve node events.
        // node.is_enabled() && !node.is_down()
        node.is_enabled()
    }

    fn is_datacenter_failover_possible(&self, routing_info: &ProcessedRoutingInfo) -> bool {
        self.preferred_datacenter.is_some()
            && self.permit_dc_failover
            && !routing_info.local_consistency
    }
}

impl Default for DefaultPolicy {
    fn default() -> Self {
        Self {
            preferred_datacenter: None,
            is_token_aware: true,
            permit_dc_failover: false,
        }
    }
}

#[derive(Clone, Debug)]
pub struct DefaultPolicyBuilder {
    preferred_datacenter: Option<String>,
    is_token_aware: bool,
    permit_dc_failover: bool,
}

impl DefaultPolicyBuilder {
    pub fn build(self) -> Arc<dyn LoadBalancingPolicy> {
        Arc::new(DefaultPolicy {
            preferred_datacenter: self.preferred_datacenter,
            is_token_aware: self.is_token_aware,
            permit_dc_failover: self.permit_dc_failover,
        })
    }

    pub fn prefer_datacenter(mut self, datacenter_name: String) -> Self {
        self.preferred_datacenter = Some(datacenter_name);
        self
    }

    pub fn token_aware(mut self, is_token_aware: bool) -> Self {
        self.is_token_aware = is_token_aware;
        self
    }

    pub fn permit_dc_failover(mut self, permit: bool) -> Self {
        self.permit_dc_failover = permit;
        self
    }
}

struct ProcessedRoutingInfo<'a> {
    token_with_strategy: Option<TokenWithStrategy<'a>>,

    // True if one of LOCAL_ONE, LOCAL_QUORUM, LOCAL_SERIAL was requested
    local_consistency: bool,
}

impl<'a> ProcessedRoutingInfo<'a> {
    fn new(query: &'a RoutingInfo, cluster: &'a ClusterData) -> ProcessedRoutingInfo<'a> {
        let local_consistency = matches!(
            (query.consistency, query.serial_consistency),
            (Consistency::LocalQuorum, _)
                | (Consistency::LocalOne, _)
                | (_, Some(SerialConsistency::LocalSerial))
        );

        Self {
            token_with_strategy: TokenWithStrategy::new(query, cluster),
            local_consistency,
        }
    }
}

struct TokenWithStrategy<'a> {
    strategy: &'a Strategy,
    token: Token,
}

impl<'a> TokenWithStrategy<'a> {
    fn new(query: &'a RoutingInfo, cluster: &'a ClusterData) -> Option<TokenWithStrategy<'a>> {
        let token = query.token?;
        let keyspace_name = query.keyspace?;
        let keyspace = cluster.get_keyspace_info().get(keyspace_name)?;
        let strategy = &keyspace.strategy;
        Some(TokenWithStrategy { strategy, token })
    }
}

#[cfg(test)]
mod tests {
    pub(crate) mod framework {
        use std::collections::{HashMap, HashSet};

        use uuid::Uuid;

        use crate::{
            load_balancing::{LoadBalancingPolicy, Plan, RoutingInfo},
            routing::Token,
            transport::{
                locator::test::{id_to_invalid_addr, mock_metadata_for_token_aware_tests},
                topology::{Metadata, Peer},
                ClusterData,
            },
        };
        pub(crate) struct ExpectedGroupsBuilder {
            groups: Vec<HashSet<u16>>,
        }

        impl ExpectedGroupsBuilder {
            pub(crate) fn new() -> Self {
                Self { groups: Vec::new() }
            }
            pub(crate) fn group(mut self, group: impl IntoIterator<Item = u16>) -> Self {
                self.groups.push(group.into_iter().collect());
                self
            }
            pub(crate) fn build(self) -> Vec<HashSet<u16>> {
                self.groups
            }
        }

        pub(crate) fn assert_proper_grouping_in_plan(
            got: &Vec<u16>,
            expected_groups: &Vec<HashSet<u16>>,
        ) {
            // First, make sure that `got` has the right number of items,
            // equal to the sum of sizes of all expected groups
            let combined_groups_len = expected_groups.iter().map(|s| s.len()).sum();
            assert_eq!(got.len(), combined_groups_len);

            // Now, split `got` into groups of expected sizes
            // and just `assert_eq` them
            let mut got = got.iter();
            let got_groups = expected_groups
                .iter()
                .map(|s| (&mut got).take(s.len()).copied().collect::<HashSet<u16>>())
                .collect::<Vec<_>>();

            assert_eq!(&got_groups, expected_groups);
        }

        #[test]
        fn test_assert_proper_grouping_in_plan_good() {
            let got = vec![1u16, 2, 3, 4, 5];
            let expected_groups = ExpectedGroupsBuilder::new()
                .group([1])
                .group([3, 2, 4])
                .group([5])
                .build();

            assert_proper_grouping_in_plan(&got, &expected_groups);
        }

        #[test]
        #[should_panic]
        fn test_assert_proper_grouping_in_plan_too_many_nodes_in_the_end() {
            let got = vec![1u16, 2, 3, 4, 5, 6];
            let expected_groups = ExpectedGroupsBuilder::new()
                .group([1])
                .group([3, 2, 4])
                .group([5])
                .build();

            assert_proper_grouping_in_plan(&got, &expected_groups);
        }

        #[test]
        #[should_panic]
        fn test_assert_proper_grouping_in_plan_too_many_nodes_in_the_middle() {
            let got = vec![1u16, 2, 6, 3, 4, 5];
            let expected_groups = ExpectedGroupsBuilder::new()
                .group([1])
                .group([3, 2, 4])
                .group([5])
                .build();

            assert_proper_grouping_in_plan(&got, &expected_groups);
        }

        #[test]
        #[should_panic]
        fn test_assert_proper_grouping_in_plan_missing_node() {
            let got = vec![1u16, 2, 3, 4];
            let expected_groups = ExpectedGroupsBuilder::new()
                .group([1])
                .group([3, 2, 4])
                .group([5])
                .build();

            assert_proper_grouping_in_plan(&got, &expected_groups);
        }

        // based on locator mock cluster
        pub(crate) async fn mock_cluster_data_for_token_aware_tests() -> ClusterData {
            let metadata = mock_metadata_for_token_aware_tests();
            ClusterData::new(metadata, &Default::default(), &HashMap::new(), &None, None).await
        }

        // creates ClusterData with info about 5 nodes living in 2 different datacenters
        // ring field is minimal, not intended to influence the tests
        pub(crate) async fn mock_cluster_data_for_token_unaware_tests() -> ClusterData {
            let peers = [("eu", 1), ("eu", 2), ("eu", 3), ("us", 4), ("us", 5)]
                .iter()
                .map(|(dc, id)| Peer {
                    datacenter: Some(dc.to_string()),
                    rack: None,
                    address: id_to_invalid_addr(*id),
                    tokens: vec![Token {
                        value: *id as i64 * 100,
                    }],
                    host_id: Uuid::new_v4(),
                })
                .collect::<Vec<_>>();

            let info = Metadata {
                peers,
                keyspaces: HashMap::new(),
            };

            ClusterData::new(info, &Default::default(), &HashMap::new(), &None, None).await
        }

        pub(crate) fn get_plan_and_collect_node_identifiers(
            policy: &impl LoadBalancingPolicy,
            query_info: &RoutingInfo,
            cluster: &ClusterData,
        ) -> Vec<u16> {
            let plan = Plan::new(policy, query_info, cluster);
            plan.map(|node| node.address.port()).collect::<Vec<_>>()
        }
    }
}
