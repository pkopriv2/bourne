# Overview

Kayak is an implementation of the RAFT [1] algorithm.  It allows consumers to create highly-resilient, strongly
consistent state machines, without all the hassle of managing consensus.

# Distributed Consensus

Distributed consensus is the process by which a group of machines is able to agree to a set of facts.  In the case
of Kayak, and its progenitor, RAFT, the algorithm specifies an algorithm for keeping a log consistent across 
a set of members.  The log has the following guarantees:

* All log items will eventually be received by all members in the same order they were received.


# Getting Started

Kayak has been designed to be embedded in other projects as a basis for distributed consensus.  To get started, pull the dependency with go get:

```sh
go get http://github.com/pkopriv2/bourne/kayak
```

Import it:

```go
import "github.com/pkopriv2/bourne/kayak"
```

## Starting a Cluster

Kayak has been built with dynamic membership as a first-class property.  Therefore, clusters are grown rather
than started. 

To start a cluster, there must be a first member.  It's special in only that it is first.

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

