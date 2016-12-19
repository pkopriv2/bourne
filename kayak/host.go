package kayak

import "github.com/pkopriv2/bourne/net"

type host struct {
	member *member
	server net.Server
}
