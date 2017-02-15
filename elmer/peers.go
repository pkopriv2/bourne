package elmer

import (
	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/net"
	"github.com/pkopriv2/bourne/scribe"
)

type peers []string

func (r peers) Write(w scribe.Writer) {
	w.WriteStrings("peers", []string(r))
}

func (r peers) Bytes() []byte {
	return scribe.Write(r).Bytes()
}

func hasPeer(peers []string, p string) bool {
	for _, cur := range peers {
		if cur == p {
			return true
		}
	}
	return false
}

func findPeer(peers []string, p string) int {
	for i, cur := range peers {
		if cur == p {
			return i
		}
	}
	return -1
}

func addPeer(cur []string, p string) []string {
	if hasPeer(cur, p) {
		return cur
	}

	return append(cur, p)
}

func delPeer(cur []string, p string) []string {
	index := findPeer(cur, p)
	if index == -1 {
		return cur
	}

	return append(cur[:index], cur[index+1:]...)
}

func collectPeers(peers []string, fn func(string) string) []string {
	ret := make([]string, 0, len(peers))
	for _, cur := range peers {
		ret = append(ret, fn(cur))
	}
	return ret
}

func collectHostnames(peers []string) []string {
	return collectPeers(peers, func(p string) string {
		host, _, _ := net.SplitAddr(p)
		return host
	})
}

func readPeers(r scribe.Reader) (ret []string, err error) {
	err = r.ReadStrings("peers", &ret)
	return
}

func parsePeersBytes(bytes []byte) (ret []string, err error) {
	if bytes == nil {
		return []string{}, nil
	}

	msg, err := scribe.Parse(bytes)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	ret, err = readPeers(msg)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return
}
