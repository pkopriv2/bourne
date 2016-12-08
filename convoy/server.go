package convoy

import (
	"strconv"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/net"
	"github.com/pkopriv2/bourne/scribe"
)

// server actions
const (
	actEvtPushPull = "/events/pushPull"
	actDirApply    = "/dir/apply"
	actDirList     = "/dir/list"
)

// Meta messages
var (
	metaDirApply    = serverNewMeta(actDirApply)
	metaDirList     = serverNewMeta(actDirList)
	metaEvtPushPull = serverNewMeta(actEvtPushPull)
)

type server struct {
	Ctx common.Context

	// the root server logger.
	Logger common.Logger

	// the member that is represented by this server.
	Self *member

	// the central storage abstraction. the directory is distributed amongst all members
	Dir *directory

	// the disseminator
	Dissem *disseminator
}

// Returns a new service handler for the ractlica
func newServer(ctx common.Context, logger common.Logger, self *member, dir *directory, dissem *disseminator, port int) (net.Server, error) {
	server := &server{
		Ctx:    ctx,
		Logger: logger.Fmt("Server"),
		Self:   self,
		Dir:    dir,
		Dissem: dissem,
	}

	return net.NewTcpServer(ctx, server.Logger, strconv.Itoa(port), serverInitHandler(server))
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
		case actDirApply:
			return s.DirApply(req)
		case actDirList:
			return s.DirList(req)
		case actEvtPushPull:
			return s.EvtPushPull(req)
		}
	}
}

// Handles a /dir/list request
func (s *server) DirList(req net.Request) net.Response {
	return newDirListResponse(s.Dir.Events())
}

// Handles a /dir/size request.  TODO: Finish
func (s *server) DirStats(req net.Request) net.Response {
	return nil
}

// Handles a /dir/apply request
func (s *server) DirApply(req net.Request) net.Response {
	events, err := readDirApplyRequest(req)
	if err != nil {
		return net.NewErrorResponse(err)
	}

	if 0 == len(events) {
		return net.NewErrorResponse(errors.New("Empty events."))
	}

	return newDirApplyResponse(s.Dir.ApplyAll(events))
}

// Handles a /evt/push request
func (s *server) EvtPushPull(req net.Request) net.Response {
	events, err := readEvtPushPullRequest(req)
	if err != nil {
		return net.NewErrorResponse(err)
	}

	return newEvtPushPullResponse(s.Dir.ApplyAll(events), s.Dissem.Evts.Pop(256))
}

// Helper functions

func serverNewMeta(action string) scribe.Message {
	return scribe.Build(func(w scribe.Writer) {
		w.Write("action", action)
	})
}

func serverReadMeta(meta scribe.Reader) (ret string, err error) {
	err = meta.Read("action", &ret)
	return
}

func serverReadEvents(msg scribe.Reader, field string) ([]event, error) {
	msgs, err := scribe.ReadMessages(msg, field)
	if err != nil {
		return nil, errors.Wrap(err, "Error parsing events")
	}

	events := make([]event, 0, len(msgs))
	for _, msg := range msgs {
		e, err := readEvent(msg)
		if err != nil {
			return nil, errors.Wrap(err, "Parsing event requests")
		}

		events = append(events, e)
	}

	return events, nil
}

// /events/pull
func newEvtPushPullRequest(events []event) net.Request {
	return net.NewRequest(metaEvtPushPull, scribe.Build(func(w scribe.Writer) {
		w.Write("events", events)
	}))
}

func newEvtPushPullResponse(success []bool, events []event) net.Response {
	return net.NewStandardResponse(scribe.Build(func(w scribe.Writer) {
		w.Write("success", success)
		w.Write("events", events)
	}))
}

func readEvtPushPullRequest(req net.Request) (events []event, err error) {
	return serverReadEvents(req.Body(), "events")
}

func readEvtPushPullResponse(res net.Response) (success []bool, events []event, err error) {
	if err = res.Error(); err != nil {
		return
	}

	if err = res.Body().Read("success", &success); err != nil {
		return
	}

	events, err = serverReadEvents(res.Body(), "events")
	return
}

// /dir/list
func newDirListRequest() net.Request {
	return net.NewRequest(metaDirList, scribe.EmptyMessage)
}

func newDirListResponse(events []event) net.Response {
	return net.NewStandardResponse(scribe.Build(func(w scribe.Writer) {
		w.Write("events", events)
	}))
}

func readDirListResponse(res net.Response) ([]event, error) {
	if err := res.Error(); err != nil {
		return nil, err
	}

	return serverReadEvents(res.Body(), "events")
}

// /dir/apply
func newDirApplyRequest(events []event) net.Request {
	return net.NewRequest(metaDirApply, scribe.Build(func(w scribe.Writer) {
		w.Write("events", events)
	}))
}

func readDirApplyRequest(req net.Request) ([]event, error) {
	return serverReadEvents(req.Body(), "events")
}

func newDirApplyResponse(success []bool) net.Response {
	return net.NewStandardResponse(scribe.Build(func(w scribe.Writer) {
		w.Write("success", success)
	}))
}

func readDirApplyResponse(res net.Response) (msgs []bool, err error) {
	if err = res.Error(); err != nil {
		return nil, err
	}

	err = res.Body().Read("success", &msgs)
	return
}
