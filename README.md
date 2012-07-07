## About

This contains two different implementations of the ketama algorithm for
memcache (particularly, bfitz's memcache client available at
https://github.com/bradfitz/gomemcache/).  It was written using and for
the Go programming language.

One implementation (cketama) uses the libketama C library, the other is a pure Go
implementation.

## Installing

### Using *go install*

    (Install libketama.so using your favorite package manager)
    $ go install github.com/rckclmbr/goketama/cketama

or

    $ go install github.com/rckclmbr/goketama/ketama

## Example

    import (
            "github.com/rckclmbr/goketama/cketama"
            "github.com/bradfitz/gomemcache/memcache"
    )

    func main() {
         selector := cketama.NewFromFile("/path/to/servers.txt")
         mc := memcache.NewFromSelector(selector)
         mc.Set(&memcache.Item{Key: "foo", Value: []byte("my value")})

         it, err := mc.Get("foo")
         ...
    }

## Drawbacks

The following are features that should be implemented, but haven't yet:

* Ability to poll/check the file for changes.  Right now it either requires
  creating a new selector, or restarting the server.
* No concurrency/locking in Go version

