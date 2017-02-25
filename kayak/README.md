# Overview

Kayak is an implementation of the RAFT [1] algorithm.  It allows consumers to create highly-resilient, strongly
consistent state machines, without all the hassle of managing consensus.

# Distributed Consensus

Distributed consensus is the process by which a group of machines is able to agree to a set of facts.  In the case
of Kayak, and its progenitor, RAFT, the algorithm ensures that a log is consistent across a set of members.  The 
log has the following guarantees:

* All committed items will eventually be received by all live members
* All committed items will be received in the order they are written
* Committed items will never be lost
* Committed Items will only be received once\*

\* The log guarantees exactly-once delivery at the level of items, however, this is NOT inherited by consumer entries.  
It is possible for an append operation to respond with an error but still complete.   Therefore, consumers that require
strict linearizability must also be idempotent.  

# Differences From Raft

Even though kayak implements that majority of raft features, e.g.:

* Leader Election
* Log Replication
* Log Snapshotting/Compactions
* Synchronizable Reads
* ... 

It does diverge 

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
host,err := kayak.Join(ctx, ":10240", []string{"host.co.com:10240"})
```

### Removing Members

To remove a member:

```go
host.Close()
```

## Using the Log 

## Using the Sync

# Security

This is the earliest version of kayak and security has not been integrated.  However, 
we plan to tackle this issue shortly.  

More details to come.

# Contributors

* Preston Koprivica: pkopriv2@gmail.com
* Andy Attebery: andyatterbery@gmail.com
* Mike Antonelli: mikeantonelli@me.com
* Danny Purcell: mikeantonelli@me.com

# References:

# Other Reading:

# License

// TODO: 

