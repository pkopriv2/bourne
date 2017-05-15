# Micro

Micro is a micro request/response server - similar to RPC except that it supports only a single dynamic input (Request) and output (Response) type.

## Examples

* Starting a server (Over TCP)

    ```

    fn := func(Request) Response {
        return Response{}
    }

	l, err := net.Listen(10*time.Second, ":0")
	if err != nil {
		panic(err) // do something better
	}

	server, err := NewServer(ctx, l, fn, 10)
	if err != nil {
		panic(err) // do something better
	}
    ```

