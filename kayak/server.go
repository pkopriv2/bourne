package kayak

import (
	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/net"
	"github.com/pkopriv2/bourne/scribe"
)

// server endpoints
const (
	actStatus          = "kayak.replica.status"
	actReplicate       = "kayak.replica.replicate"
	actRequestVote     = "kayak.replica.requestVote"
	actAppend          = "kayak.replica.append"
	actInstallSnapshot = "kayak.replica.snapshot"
	actUpdateRoster    = "kayak.replica.roster"
)

// Meta messages
var (
	metaStatus          = serverNewMeta(actStatus)
	metaReplicate       = serverNewMeta(actReplicate)
	metaRequestVote     = serverNewMeta(actRequestVote)
	metaAppend          = serverNewMeta(actAppend)
	metaInstallSnapshot = serverNewMeta(actInstallSnapshot)
	metaUpdateRoster    = serverNewMeta(actUpdateRoster)
)

type server struct {
	ctx    common.Context
	logger common.Logger
	self   *replica
}

// Returns a new service handler for the ractlica
func newServer(ctx common.Context, logger common.Logger, port string, self *replica) (net.Server, error) {
	server := &server{ctx: ctx, logger: logger.Fmt("Server"), self: self}
	return net.NewTcpServer(ctx, server.logger, port, serverInitHandler(server))
}

func serverInitHandler(s *server) func(net.Request) net.Response {
	return func(req net.Request) net.Response {
		action, err := serverReadMeta(req.Meta())
		if err != nil {
			return net.NewErrorResponse(errors.Wrap(err, "Error parsing action"))
		}

		switch action {
		default:
			return net.NewErrorResponse(errors.Errorf("Unknown action %v", action))
		case actStatus:
			return s.Status(req)
		case actReplicate:
			return s.Replicate(req)
		case actRequestVote:
			return s.RequestVote(req)
		case actAppend:
			return s.Append(req)
		case actUpdateRoster:
			return s.UpdateRoster(req)
		case actInstallSnapshot:
			return s.InstallSnapshot(req)
		}
	}
}

func (s *server) Status(req net.Request) net.Response {
	return nil
	// return statusResponse{s.self.Id, replica.CurrentTerm(), replica.Cluster()}.Response()
}

func (s *server) UpdateRoster(req net.Request) net.Response {
	update, err := readRosterUpdate(req.Body())
	if err != nil {
		return net.NewErrorResponse(err)
	}

	return net.NewErrorResponse(s.self.UpdateRoster(update))
}

func (s *server) InstallSnapshot(req net.Request) net.Response {
	return nil
}

func (s *server) Replicate(req net.Request) net.Response {
	replicate, err := readReplicateEvents(req.Body())
	if err != nil {
		return net.NewErrorResponse(err)
	}

	resp, err := s.self.Replicate(replicate)
	if err != nil {
		return net.NewErrorResponse(err)
	}

	return resp.Response()
}

func (s *server) RequestVote(req net.Request) net.Response {
	voteRequest, err := readRequestVote(req.Body())
	if err != nil {
		return net.NewErrorResponse(err)
	}

	resp, err := s.self.RequestVote(voteRequest)
	if err != nil {
		return net.NewErrorResponse(err)
	}

	return resp.Response()
}

func (s *server) Append(req net.Request) net.Response {
	append, err := readAppendEvent(req.Body())
	if err != nil {
		return net.NewErrorResponse(err)
	}

	item, err := s.self.RemoteAppend(append)
	if err != nil {
		return net.NewErrorResponse(err)
	}

	return appendEventResponse{item.Index, item.Term}.Response()
}

// Helper functions

func serverNewMeta(action string) scribe.Message {
	return scribe.Build(func(w scribe.Writer) {
		w.WriteString("action", action)
	})
}

func serverReadMeta(meta scribe.Reader) (ret string, err error) {
	err = meta.ReadString("action", &ret)
	return
}
