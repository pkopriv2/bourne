# Stash

Stash adds a bit of process and lifecycle management around a `*bolt.DB` 
instance. By default, multiple instances of bolt.DBs are precluded through
the use of a an OS file lock.  Stash allows a bolt.DB instance's lifecycle
to be bound to a `common.Context`, enabling multiple consumers to have
access to a bolt instance, while also not having to worry about any 
resource cleanup.

## Getting Started

Pull the dependency with go get:

```sh
go get http://github.com/pkopriv2/bourne/stash
```

Import it:

```go
import "github.com/pkopriv2/bourne/stash"
```

### Opening a Stash Instance

Stash contains basically a single method:

```go
stash := stash.Open(ctx, "path/to/stash.db")
```

This method is guaranteed thread-safe and will only produce a single
bolt.DB instance per stash location.

### Closing a Stash Instance

```go
stash := stash.Open(ctx, "path/to/stash.db")
defer ctx.Close()
```

Stash instances are mantained by the parent context.  Once the context has
been closed, the stash is transitively closed.  Reopening a closed
stash results in undefined behavior.

## Configuration

Stash can configured with the following keys:

* bourne.stash.path - Path to the stash file.
