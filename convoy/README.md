# Overview

Convoy is an information dissemination library - built on the SWIM [1] membership protocol. It extends the protocol slightly to allow each member to disseminate facts about himself - in the form a local key/value store.  Moreover, every member is able to update every other member's store.  Each member's store is disseminated amongs the entire group using the same dissemination component as the membership protocol.  Because the data replicated everywhere, each member's store should aim to be as small as possible. 

For more overview and background on epidemic protocols and their analysis, please see [2].

# Intended Usage

Convoy is intended to serve as the foundation of distributed systems that require eventually consistent, membership knowledge.  Currently, its intention is to form the basis of a general overlay network for use in a new deployment fabric.  Stay tuned... 

# Membership Overview

The heart of convoy is really just the SWIM membership protocol.  Members join, and then serve as an information relay.  In more abstract terms, members form the vertices of a connected graph.  For information to spread in a atomic broadcast fashion (i.e. all members of the group eventually see all information), every member must propagate to some random subset of other members. The original mathematical analysis of randomly connected graphs was done by Paul Erdos¨ and Alfred Renyi [2].  They concluded that for a random graph to become fully connected, each vertex has to have at least *n ~ ln(N)* random edges.

To speed up the dissemination of information, convoy allows consumers to configure the number of recipients of a message based on factors of *ln(N)*.  Due to some very specialised buffer management a mathematical analysis of the speed of dissemination has not been done (Translation: Author sucks at stats)  Anyone wishing to help out with this endeavor is welcome to take a crack at it.  Preston is available anytime to answer any design questions you may require.  

In the meantime, here are the results of benchmarks performed locally as a function of cluster size and fanout factor: 

* TODO: INSERT RESULTS

## Member Health 

During each dissemination period, members are also probed for health.  Due to the expense of
being marked as failed, a member cannot be contacted, a health probe is sent to a random subset 
of members who will attempt to contact the suspected node.  Only when none of the members of 
the probe return success is the member deemed failed and evicted from the cluster. 

# Getting Started

Convoy has been designed to be embedded in other projects as a basis for group memberships
and service discovery.  To get started, pull the dependency with go get:

```sh
go get http://github.com/pkopriv2/bourne/convoy
```

Import it:

```go
import "github.com/pkopriv2/bourne/convoy"
```

## Starting a Cluster.

To start a cluster, there must be a first member.  This is designated as the "seed" member.
It's special in only that it is first.

To start a seed member, simply invoke:

```
host := convoy.StartSeedHost(ctx, "seed.convoy.com:10240")
```

At this point, the seed host knows nothing about any other hosts and will remain alone until
it is contacted.  However, when the first member co


## Adding Members

To add a member:

```
host := convoy.StartHost(ctx, "host1.convoy.com:10240", "seed.convoy.com:10240")
```

## Using the Local Store

Every host manages a local key-value store that is replicated to all other members in the cluster.


* Retrieving the store
```
store := host.Store()
```

* Getting an item from the store

```
ok, item, err := store.Get("key")
```

* Updating an item from the store

```
_, item, err := store.Get("key")
store.Put("key", "val", item.Ver)
```

## Using the Distributed Directory

* Retrieving the directory
```
dir := host.Directory()
```

* To retrieve all the 'active' members of a cluster:
```
all, err := dir.All()
```

* Searching the directory:
```
found, err := dir.Search(func(id uuid.UUID, key string, val string) bool {
    return key == "#tag"
})
```

# Architecture/Design

## Terms and Definitions

* Host: The host is the parent object of a member of a cluster.  The host is responsible
for managing the lifecycle of the member and in the event of being marked as failed, will
detect the failure and attempt to rejoin the cluster.

* Replica: A replica represents a currently active member of the cluster.  The replica's
lifespan extends as long as the member is not evicted or leaves the cluster.  Upon failure
the replica cleans up its resources and dies - ultimately notifying the parent host.

* Store: A store is a single member's key/value store.

* Directory: The directory maintains an eventually consistent, merged view of all members'
stores and their membership status.

* Disseminator: The disseminator is responsible for forwarding data that is has received
to other members per the SWIM algorithm.  The disseminator is also responsible for 
probing and disseminating health information to other healthy members of the cluster.

# Security

This is the earliest version of convoy and security has not been integrated.  However, 
we plan to tackle this issue shortly.  

More details to come.

# Contributors

* Preston Koprivica: pkopriv2@gmail.com
* Andy Attebery: andyatterbery@gmail.com

# References:

1. https://www.cs.cornell.edu/home/rvr/papers/flowgossip.pdf
2. http://se.inf.ethz.ch/old/people/eugster/papers/gossips.pdf

# Other Reading:

 * http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.557.1902&rep=rep1&type=pdf
 * https://www.cs.cornell.edu/~asdas/research/dsn02-swim.pdf
 * http://bitsavers.informatik.uni-stuttgart.de/pdf/xerox/parc/techReports/CSL-89-1_Epidemic_Algorithms_for_Replicated_Database_Maintenance.pdf

# License

// TODO: 

