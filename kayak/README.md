# Overview

Kayak is an implementation of the RAFT [1] algorithm.  It allows consumers to create highly-resilient, strongly
consistent state machines, without all the hassle of managing consensus.

# Distributed Consensus

Distributed consensus is the process by which a group of machines is able to agree to a set of facts.  In the case
of Kayak, and its progenitor, RAFT, the algorithm specifies an algorithm for keeping a log consistent across 
a set of members.  The log has the following guarantees:

* All items will eventually be received
* All items will be received in the order they are written
* Items will never be lost
* Items will only be received once\*

\* The log guarantees exactly-once delivery at the level of items, however, this is NOT inherited by consumer entries.  
It is possible for an append operation to respond with an error but still complete.   Therefore, consumers that require
strict linearizability must also be idempotent.  

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
than started.  The process for growing a cluster is done by starting a single member and then joining 
additional members as required.

To start a cluster, there must be a first member.  It's special in only that it is first.

To start a seed member, simply invoke:

```go
host,err := kayak.Start(ctx, ":10240")
```


### Adding Members

To add a member:

```go
host := convoy.StartHost(ctx, "host1.convoy.com:10240", "seed.convoy.com:10240")
```

### Removing Members

To add a member:

```go
host := convoy.StartHost(ctx, "host1.convoy.com:10240", "seed.convoy.com:10240")
```

## Using the Log 


## Using the Sync

* Searching the directory:
```
found, err := dir.Search(func(id uuid.UUID, key string, val string) bool {
    return key == "#tag"
})
```

# Architecture/Design

# Security

This is the earliest version of kayak and security has not been integrated.  However, 
we plan to tackle this issue shortly.  

More details to come.

# Contributors

* Preston Koprivica: pkopriv2@gmail.com
* Andy Attebery: andyatterbery@gmail.com
* Mike Antonelli: mikeantonelli@me.com

# References:

1. https://www.cs.cornell.edu/home/rvr/papers/flowgossip.pdf
2. http://se.inf.ethz.ch/old/people/eugster/papers/gossips.pdf

# Other Reading:

 * http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.557.1902&rep=rep1&type=pdf
 * https://www.cs.cornell.edu/~asdas/research/dsn02-swim.pdf
 * http://bitsavers.informatik.uni-stuttgart.de/pdf/xerox/parc/techReports/CSL-89-1_Epidemic_Algorithms_for_Replicated_Database_Maintenance.pdf

# License

// TODO: 

