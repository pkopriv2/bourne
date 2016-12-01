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
	actDirSize     = "/dir/size"
	actDirHash     = "/dir/hash"
)

// Meta messages
var (
	metaDirApply    = serverNewMeta(actDirApply)
	metaDirList     = serverNewMeta(actDirList)
	metaDirSize     = serverNewMeta(actDirSize)
	metaDirHash     = serverNewMeta(actDirHash)
	metaEvtPushPull = serverNewMeta(actEvtPushPull)
)

type server struct {
	Ctx    common.Context
	Logger common.Logger
	Self   *member
	Dir    *directory
	Dissem *disseminator
	Log    *timeLog
}

// Returns a new service handler for the ractlica
func newServer(ctx common.Context, logger common.Logger, self *member, dir *directory, dissem *disseminator, port int) (net.Server, error) {
	log, err := newTimeLog(ctx, logger, dir)
	if err != nil {
		return nil, err
	}

	server := &server{
		Ctx:    ctx,
		Logger: logger.Fmt("Server"),
		Self:   self,
		Dir:    dir,
		Dissem: dissem,
		Log:    log,
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
		case actDirSize:
			return s.DirSize(req)
		case actDirHash:
			return s.DirHash(req)
		case actEvtPushPull:
			return s.EvtPushPull(req)
		}
	}
}

// Handles a /dir/list request
func (s *server) DirList(req net.Request) net.Response {
	return newDirListResponse(s.Dir.Events())
}

// Handles a /dir/size request.  TODO: Dead
func (s *server) DirSize(req net.Request) net.Response {
	return newDirSizeResponse(s.Dir.Size())
}

// Handles a /dir/hash request   TODO: Dead
func (s *server) DirHash(req net.Request) net.Response {
	return newDirHashResponse(s.Dir.Hash())
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

	ret, err := s.applyAndDisseminate(events)
	if err != nil {
		return net.NewErrorResponse(err)
	}

	return newDirApplyResponse(ret)
}

// Handles a /evt/push request
func (s *server) EvtPushPull(req net.Request) net.Response {
	events, err := readEvtPushPullRequest(req)
	if err != nil {
		return net.NewErrorResponse(err)
	}

	ret, err := s.applyAndDisseminate(events)
	if err != nil {
		return net.NewErrorResponse(err)
	}

	return newEvtPushPullResponse(ret, s.Log.Pop())
}

func (s *server) applyAndDisseminate(events []event) (ret []bool, err error) {
	if len(events) == 0 {
		return []bool{}, nil
	}

	ret = s.Dir.ApplyAll(events)
	s.Logger.Debug("Applied [%v] events", len(ret))

	unique := serverCollectSuccess(events, ret)
	if err = s.Log.Push(unique); err != nil {
		s.Dissem.Push(unique)
		return
	}

	err = s.Dissem.Push(unique)
	return
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

func serverCollectSuccess(all []event, success []bool) []event {
	forward := make([]event, 0, len(success))
	for i, b := range success {
		if b {
			forward = append(forward, all[i])
		}
	}

	return forward
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

// /dir/size
func newDirSizeRequest() net.Request {
	return net.NewRequest(metaDirSize, scribe.EmptyMessage)
}

func newDirSizeResponse(size int) net.Response {
	return net.NewStandardResponse(scribe.Build(func(w scribe.Writer) {
		w.Write("size", size)
	}))
}

func readDirSizeResponse(res net.Response) (ret int, err error) {
	if err = res.Error(); err != nil {
		return
	}

	err = res.Body().Read("size", &ret)
	return
}

// /dir/hash
func newDirHashRequest() net.Request {
	return net.NewRequest(metaDirHash, scribe.EmptyMessage)
}

func newDirHashResponse(hash []byte) net.Response {
	return net.NewStandardResponse(scribe.Build(func(w scribe.Writer) {
		scribe.WriteBytes(w, "hash", hash)
	}))
}

func readDirHashResponse(res net.Response) ([]byte, error) {
	if err := res.Error(); err != nil {
		return nil, err
	}

	return scribe.ReadBytes(res.Body(), "hash")
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
