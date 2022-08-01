//! This module is responsible for precomputing all possible replica lists for given replication strategies.
//! Having the replicas precomputed allows to avoid costly calculations each time a request is sent.
//! To get a replica list for a given token the driver can simply do a lookup in the precomputed data.
//! Precomputing is realized in an efficient manner, we are leveraging an advantageous property
//! of `SimpleStrategy` and datacenter-local `NetworkTopologyStrategy` (with some caveats described
//! below) - replica lists for replication factor `n` are prefixes of replica lists for replication
//! factor `n + 1`. This enables us to only compute replica lists for maximal replication factor
//! found in replication strategies (in `NTS`, we are interested in maximal `rf`
//! specified for each datacenter). It is a big optimization - other solution is
//! to compute those lists for each strategy used in cluster.
//!
//! Notes on Network Topology Strategy precomputation:
//! The optimization mentioned above works ony if requested `replication factor` is <= `rack count`.

use super::replication_info::ReplicationInfo;
use super::TokenRing;
use crate::routing::Token;
use crate::transport::node::Node;
use crate::transport::topology::Strategy;

use std::cmp;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::sync::Arc;

type Replicas = Vec<Arc<Node>>;

/// Takes care of precomputing all possible replica lists for given replication strategies.
/// Uses `ReplicationInfo` to calculate the replica lists for all tokens present in the ring,
/// and stores them for future use.
#[derive(Clone)]
pub(crate) struct PrecomputedReplicas {
    /// Precomputed replicas for SimpleStrategy, kept in a ring for efficient access.
    global_replicas: PrecomputedReplicasRing,

    /// Precomputed replicas for each datacenter, used in NetworkTopologyStrategy.
    datacenter_replicas: HashMap<String, DatacenterPrecomputedReplicas>,
}

/// Keeps a precomputed replica list for each token present in the ring.
#[derive(Clone)]
struct PrecomputedReplicasRing {
    /// Replica lists are kept in `TokenRing` to allow efficient access with a token as a key.
    replicas_for_token: TokenRing<Replicas>,

    /// Maximal replication factor up to which replica lists are computed.
    max_rep_factor: usize,
}

#[derive(Clone)]
struct DatacenterPrecomputedReplicas {
    /// Holds replica lists that were computed using `replication factor` <= `rack count`.
    /// Replica lists computed for `rf` = `n` are prefixes of replica lists computed for
    /// `rf` = `rack count` (if `n` <= `rack count`), so they can be "compressed".
    /// This compression relies on computing the longest lists only.
    compressed_replica_ring: Option<PrecomputedReplicasRing>,

    /// Holds the replica list computed for `replication factors` > `rack count`.
    above_rack_count_replica_rings: HashMap<usize, TokenRing<Replicas>>,
}

impl DatacenterPrecomputedReplicas {
    fn get_replica_ring_for_rf(&self, replication_factor: usize) -> Option<&TokenRing<Replicas>> {
        if let Some(compressed) = &self.compressed_replica_ring {
            if compressed.max_rep_factor >= replication_factor {
                return Some(&compressed.replicas_for_token);
            }
        }

        self.above_rack_count_replica_rings.get(&replication_factor)
    }
}

impl PrecomputedReplicas {
    /// Performs the replica precomputation and creates an instance of `PrecomputedReplicas`.
    /// It extracts the maximal replication factor for which to compute from the given keyspace strategies.
    /// The replicas are first precomputed for the global ring and then individually for each datacenter.
    /// Uses the optimization specified in module description.
    /// The computation could potentially take some time so it shouldn't be done in an async function.
    pub(crate) fn compute<'a>(
        replication_data: &ReplicationInfo,
        keyspace_strategies: impl Iterator<Item = &'a Strategy>,
    ) -> PrecomputedReplicas {
        // Each ring will precompute for at least this RF
        let min_precomputed_rep_factor: usize = 1;

        let mut max_global_repfactor: usize = min_precomputed_rep_factor;
        let mut dc_repfactors: HashMap<&'a str, BTreeSet<usize>> = HashMap::new();

        for strategy in keyspace_strategies {
            match strategy {
                Strategy::SimpleStrategy { replication_factor } => {
                    max_global_repfactor = cmp::max(max_global_repfactor, *replication_factor)
                }
                Strategy::NetworkTopologyStrategy {
                    datacenter_repfactors,
                } => {
                    for (dc_name, dc_repfactor) in datacenter_repfactors {
                        let repfactors: &mut BTreeSet<usize> =
                            dc_repfactors.entry(dc_name).or_default();

                        repfactors.insert(*dc_repfactor);
                    }
                }
                Strategy::LocalStrategy => {} // RF=1
                Strategy::Other { .. } => {}  // Can't precompute for custom strategies
            }
        }

        let global_replicas_iter = replication_data.get_global_ring().iter().map(|(token, _)| {
            let cur_replicas: Replicas = replication_data
                .simple_strategy_replicas(*token, max_global_repfactor)
                .cloned()
                .collect();
            (*token, cur_replicas)
        });
        let global_replicas = PrecomputedReplicasRing {
            replicas_for_token: TokenRing::new(global_replicas_iter),
            max_rep_factor: max_global_repfactor,
        };

        let mut datacenter_replicas: HashMap<String, DatacenterPrecomputedReplicas> =
            HashMap::new();
        for (dc_name, repfactors) in dc_repfactors {
            let dc_rep_data = match replication_data.get_datacenters().get(dc_name) {
                Some(dc_rep_data) => dc_rep_data,
                None => continue,
            };

            let rack_count = dc_rep_data.get_rack_count();
            let compressed_replica_ring_rf = repfactors.range(..=rack_count).next_back();
            let replica_ring_rf_above_rack_count = repfactors.range((rack_count + 1)..);

            let produce_replica_ring_iter = |rf| {
                let ring_iter = dc_rep_data.get_dc_ring().iter().map(|(token, _)| {
                    let cur_replicas: Replicas = replication_data
                        .nts_replicas_in_datacenter(*token, dc_name, rf)
                        .cloned()
                        .collect();
                    (*token, cur_replicas)
                });

                TokenRing::new(ring_iter)
            };

            let compressed_replica_ring =
                compressed_replica_ring_rf.map(|rf| PrecomputedReplicasRing {
                    replicas_for_token: produce_replica_ring_iter(*rf),
                    max_rep_factor: *rf,
                });

            let above_rack_count_replica_rings = replica_ring_rf_above_rack_count
                .map(|rf| (*rf, produce_replica_ring_iter(*rf)))
                .collect();

            let dc_precomputed_replicas = DatacenterPrecomputedReplicas {
                compressed_replica_ring,
                above_rack_count_replica_rings,
            };

            datacenter_replicas.insert(dc_name.to_string(), dc_precomputed_replicas);
        }

        PrecomputedReplicas {
            global_replicas,
            datacenter_replicas,
        }
    }

    /// Gets the precomputed replica list for a given SimpleStrategy.
    /// When requested replication factor is larger than the maximal one (detected during the
    /// precomputation stage), `None` is returned.
    pub(crate) fn get_precomputed_simple_strategy_replicas(
        &self,
        token: Token,
        replication_factor: usize,
    ) -> Option<&[Arc<Node>]> {
        if replication_factor > self.global_replicas.max_rep_factor {
            return None;
        }

        let precomputed_token_replicas = self
            .global_replicas
            .replicas_for_token
            .get_elem_for_token(token)?;
        let result_len: usize = cmp::min(precomputed_token_replicas.len(), replication_factor);
        Some(&precomputed_token_replicas[..result_len])
    }

    /// Gets the precomputed replica list for a given NetworkTopologyStrategy.
    /// When requested replication factor is larger than the maximal one (detected during the
    /// precomputation stage), `None` is returned.
    /// If a provided datacenter does not exist, `None` is returned.
    pub(crate) fn get_precomputed_network_strategy_replicas(
        &self,
        token: Token,
        dc_name: &str,
        dc_replication_factor: usize,
    ) -> Option<&[Arc<Node>]> {
        let precomputed_replicas_ring = self
            .datacenter_replicas
            .get(dc_name)?
            .get_replica_ring_for_rf(dc_replication_factor)?;

        let precomputed_replicas = precomputed_replicas_ring.get_elem_for_token(token)?;
        let result_len: usize = cmp::min(precomputed_replicas.len(), dc_replication_factor);

        Some(&precomputed_replicas[..result_len])
    }
}
