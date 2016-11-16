# Amoeba

Amoeba is a library of indexing primitives.  Its intended use is within 
a distributed, eventually convergent database, but has been extracted for 
general use.  It implements a simple in-memory indexer for use in 
applications which require more advanced lookup methods than what a simple 
map can provide.  It supports near-constant time (log32(n)) get/set operations
and a very performant scanning abstraction.

## Getting Started

Pull the dependency with go get:

```sh
go get http://github.com/pkopriv2/bourne/amoeba
```

Import it:

```go
import "github.com/pkopriv2/bourne/amoeba"
```

### Creating a new Indexer

Amoeba consists of basically single public API.  To get an instance of an
indexer:

```go
indexer := amoeba.NewIndexer(common.NewContext(common.EmptyConfig()))
```

You'll notice that virtually none of the amoeba libraries return errors. 
This is by design, as many of the operations only require native language
features and no external IO.  While nice, this does, however mean that
indexers are NOT suitable for large indexes or in applications that require
durability.

### Retrieving a value

```go
indexer.Read(func(v View) {
    item := v.Get(IntKey(1))
    ...
})
```

Reads are transactionally consistent within the provided closure.  However, 
consumers should NOT retain any references to the View outside of the 
closure, as synchronization is no longer guaranteed.  

### Updating a value

```go
indexer.Update(func(u Update) {
    u.Put(IntKey(1), "val", 0)
})
```

The main thing to notice is that every item within an amoeba index requires a 
version argument. Amoeba indexes are specifically designed to be replicated
amongst many hosts, which means that all the problems of distributed systems
are now in play.  Among those are:

* Failed delivery
* Partial delivery
* Out of order delivery

Externalizing time allows us to index data on non-ambiguous elements (e.g. keys)
while reconciling any issues of delivery by only ever accepting the latest 
observed value for an item.  

### Deleting a value

Deleting keys is very similar to updating keys.  Internally, they are represented
the same as a put, with no corresponding value.

```go
indexer.Update(func(u Update) {
    u.Del(IntKey(1), 0)
})
```

### Creating a new key type

Very similar to maps, amoeba indexes support an indexed lookup on keys.  However, 
unlike maps, Amoeba keys have the added requirement of being sortable.  All keys must
implement the amoeba.Sortable interface.  Internally, amoeba uses the sortable 
interface to maintain a sorted B-tree for each index.


```go
type IntKey int

func (i IntKey) Compare(s Sortable) int {
	return int(i - s.(IntKey))
}
```

#### Supported Key Types

Amoeba comes prepackaged with a few convenience key implementations.  As of the 
writing of this document, those are:

* IntKey
* StringKey
* BytesKey 

These may be used as is, or may be used as the building blocks for more advanced, 
composite keys.  Amoeba currently makes no proscriptions on the structure of keys. 
The only requirement is that keys implement the sortable interface.

## Configuration

Amoeba can configured with the following keys:

* amoeba.index.gc.expiration - Minimum time before deleted data is considered garbage.
* amoeba.index.gc.cycle - Minimum duration between gc cycles.
