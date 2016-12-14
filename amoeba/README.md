# Amoeba

Amoeba is a library of indexing abstractions and a single implementing type.  
Its intended use is within a distributed, eventually convergent database, but 
has been extracted for general use.  

## Getting Started

Pull the dependency with go get:

```sh
go get http://github.com/pkopriv2/bourne/amoeba
```

Import it:

```go
import "github.com/pkopriv2/bourne/amoeba"
```

### Creating a new index

Creating an index entails deciding on the implementation to use.  At the time 
of this writing there was only a single type: `BTreeIndex`.  

```go
indexer := amoeba.NewBTreeIndex(32)
```

You'll notice that virtually none of the amoeba libraries return errors. 
This is by design, as many of the operations only require native language
features and no external IO.  While nice, this does, however mean that
indexers are NOT suitable for extremely large indexes or in applications 
that require durability.

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
    u.Put(IntKey(1), "val")
})
```

The only additional thing to note is that while multiple readers are 
allowed, amoeba indices currently only allow a single update at a time

### Deleting a value

```go
indexer.Update(func(u Update) {
    u.Del(IntKey(1))
})
```

NOTE: Deletions are permanent and all history of the item are lost. 
If idempotency is required, external versioning is required.

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

Amoeba currently requires no environmental configuration.


## Authors

* Preston Koprivica: pkoriv2@gmail.com
