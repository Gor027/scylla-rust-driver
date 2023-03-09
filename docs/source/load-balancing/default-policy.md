# DefaultPolicy

`DefaultPolicy` is the default load balancing policy in Scylla Rust Driver. It
can be configured to be datacenter-aware and token-aware. Datacenter failover
for queries with non-local consistency mode is also supported.

## Creating a DefaultPolicy

`DefaultPolicy` can be created only using `DefaultPolicyBuilder`. The
`builder()` method of `DefaultPolicy` returns a new instance of
`DefaultPolicyBuilder` with the following default values:

- `preferred_datacenter`: `None`
- `is_token_aware`: `true`
- `permit_dc_failover`: `false`
- `latency_awareness`: `None`

You can use the builder methods to configure the desired settings and create a
`DefaultPolicy` instance:

```rust
# extern crate scylla;
# fn test_if_compiles() {
use scylla::load_balancing::DefaultPolicy;

let default_policy = DefaultPolicy::builder()
        .prefer_datacenter("dc1".to_string())
        .token_aware(true)
        .permit_dc_failover(true)
        .build();
# }
```

### Semantics of `DefaultPolicy`

#### Preferred Datacenter

The `preferred_datacenter` field in `DefaultPolicy` allows the load balancing
policy to prioritize nodes based on their location. When a preferred datacenter
is set, the policy will treat nodes in that datacenter as "local" nodes, and
nodes in other datacenters as "remote" nodes. This affects the order in which
nodes are returned by the policy when selecting replicas for read or write
operations. If no preferred datacenter is specified, the policy will treat all
nodes as local nodes.

When datacenter failover is disabled (`permit_dc_failover` is set to
false), the default policy will only include local nodes in load balancing
plans. Remote nodes will be excluded, even if they are alive and available to
serve requests.

#### Datacenter Failover

In the event of a datacenter outage or network failure, the nodes in that
datacenter may become unavailable, and clients may no longer be able to access
the data stored on those nodes. To address this, the `DefaultPolicy` supports datacenter
failover, which allows to route requests to nodes in other datacenters if the
local nodes are unavailable.

Datacenter failover can be enabled in `DefaultPolicy` by `permit_dc_failover`
setting in the builder. When this flag is set, the policy will prefer to return
alive remote replicas if datacenter failover is permitted and possible due to
consistency constraints.

#### Token awareness

Token awareness refers to a mechanism by which the driver is aware of the token
range assigned to each node in the cluster. Tokens are assigned to nodes to
partition the data and distribute it across the cluster.

When a user wants to read or write data, the driver can use token awareness to
route the request to the correct node based on the token range of the data
being accessed. This can help to minimize network traffic and improve
performance by ensuring that the data is accessed locally as much as possible.

In the case of `DefaultPolicy`, token awareness is enabled by default, meaning
that the policy will prefer to return alive local replicas if the token is
available. This means that if the client is requesting data that falls within
the token range of a particular node, the policy will try to route the request
to that node first, assuming it is alive and responsive.

Token awareness can significantly improve the performance and scalability of
applications built on Scylla. By using token awareness, users can ensure that
data is accessed locally as much as possible, reducing network overhead and
improving throughput.



### Node order in produced plans

The DefaultPolicy prefers to return nodes in the following order:

1. Alive local replicas (if token is available & token awareness is enabled)
2. Alive remote replicas (if datacenter failover is permitted & possible due to consistency constraints)
3. Alive local nodes
4. Alive remote nodes (if datacenter failover is permitted & possible due to consistency constraints)
5. Enabled down nodes

If no preferred datacenter is specified, all nodes are treated as local ones.

Replicas in the same priority groups are shuffled. Non-replicas are randomly
rotated (similarly to a round robin with a random index).
