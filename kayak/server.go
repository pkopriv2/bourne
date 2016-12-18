package kayak

// import (
	// "strconv"
//
	// "github.com/pkg/errors"
	// "github.com/pkopriv2/bourne/common"
	// "github.com/pkopriv2/bourne/net"
	// "github.com/pkopriv2/bourne/scribe"
	// uuid "github.com/satori/go.uuid"
// )
//
// // server endpoints
// const (
	// actPing      = "/health/ping"
	// actPingProxy = "/health/pingProxy"
	// actPushPull  = "/dissem/pushPull"
	// actDirApply  = "/dir/apply"
	// actDirList   = "/dir/list"
	// actStorePut  = "/store/put"
// )
//
// // Meta messages
// var (
	// metaPing      = serverNewMeta(actPing)
	// metaPingProxy = serverNewMeta(actPingProxy)
	// metaDirApply  = serverNewMeta(actDirApply)
	// metaDirList   = serverNewMeta(actDirList)
	// metaPushPull  = serverNewMeta(actPushPull)
	// metaStorePut  = serverNewMeta(actStorePut)
// )
//
// type server struct {
	// ctx common.Context
//
	// // the root server logger.
	// logger common.Logger
//
	// // the member that is represented by this server.
	// self member
//
	// // the central storage abstraction. the directory is distributed amongst all members
	// dir *directory
//
	// // the disseminator
	// dissem *disseminator
// }
//
// // Returns a new service handler for the ractlica
// func newServer(ctx common.Context, logger common.Logger, self member, dir *directory, dissem *disseminator, port int) (net.Server, error) {
	// server := &server{
		// ctx:    ctx,
		// logger: logger.Fmt("Server"),
		// self:   self,
		// dir:    dir,
		// dissem: dissem,
	// }
//
	// return net.NewTcpServer(ctx, server.logger, strconv.Itoa(port), serverInitHandler(server))
// }
//
// func serverInitHandler(s *server) func(net.Request) net.Response {
	// return func(req net.Request) net.Response {
		// action, err := serverReadMeta(req.Meta())
		// if err != nil {
			// return net.NewErrorResponse(errors.Wrap(err, "Error parsing action"))
		// }
//
		// switch action {
		// default:
			// return net.NewErrorResponse(errors.Errorf("Unknown action %v", action))
		// case actPing:
			// return s.Ping(req)
		// case actPingProxy:
			// return s.ProxyPing(req)
		// case actDirApply:
			// return s.DirApply(req)
		// case actDirList:
			// return s.DirList(req)
		// case actPushPull:
			// return s.PushPull(req)
		// }
	// }
// }
//
// func (s *server) Ping(req net.Request) net.Response {
	// return net.NewEmptyResponse()
// }
//
// // Handles a /dir/list request
// func (s *server) ProxyPing(req net.Request) net.Response {
	// id, err := readPingProxyRequest(req)
	// if err != nil {
		// return net.NewErrorResponse(err)
	// }
//
	// m, ok := s.dir.Get(id)
	// if !ok {
		// return newPingProxyResponse(false)
	// }
//
	// c, err := m.Client(s.ctx)
	// if err != nil || c == nil {
		// return newPingProxyResponse(false)
	// }
	// defer c.Close()
//
	// err = c.Ping()
	// return newPingProxyResponse(err == nil)
// }
//
// // Handles a /dir/list request
// func (s *server) DirList(req net.Request) net.Response {
	// return newDirListResponse(s.dir.Events())
// }
//
// // Handles a /dir/size request.  TODO: Finish
// func (s *server) DirStats(req net.Request) net.Response {
	// return nil
// }
//
// // Handles a /dir/apply request
// func (s *server) DirApply(req net.Request) net.Response {
	// events, err := readDirApplyRequest(req)
	// if err != nil {
		// return net.NewErrorResponse(err)
	// }
//
	// if len(events) == 0 {
		// return net.NewErrorResponse(errors.New("Empty events."))
	// }
//
	// ret, err := s.dir.Apply(events)
	// if err != nil {
		// return net.NewErrorResponse(err)
	// }
//
	// return newDirApplyResponse(ret)
// }
//
// // Handles a /evt/push request
// func (s *server) PushPull(req net.Request) net.Response {
	// source, ver, events, err := readPushPullRequest(req)
	// if err != nil {
		// return net.NewErrorResponse(err)
	// }
//
	// var unHealthy bool
	// s.dir.Core.View(func(v *view) {
		// m, ok := v.Roster[source]
		// h, _ := v.Health[source]
		// unHealthy = ok && m.Version == ver && m.Active && !h.Healthy
	// })
//
	// if unHealthy {
		// s.logger.Error("Unhealthy member detected [%v]", source)
		// return net.NewErrorResponse(FailedError)
	// }
//
	// ret, err := s.dir.Apply(events)
	// if err != nil {
		// return net.NewErrorResponse(err)
	// }
//
	// return newPushPullResponse(ret, s.dissem.events.Pop(1024))
// }
//
// // Helper functions
//
// func serverNewMeta(action string) scribe.Message {
	// return scribe.Build(func(w scribe.Writer) {
		// w.Write("action", action)
	// })
// }
//
// func serverReadMeta(meta scribe.Reader) (ret string, err error) {
	// err = meta.Read("action", &ret)
	// return
// }
//
// func serverReadEvents(msg scribe.Reader, field string) ([]event, error) {
	// msgs, err := scribe.ReadMessages(msg, field)
	// if err != nil {
		// return nil, errors.Wrap(err, "Error parsing events")
	// }
//
	// events := make([]event, 0, len(msgs))
	// for _, msg := range msgs {
		// e, err := readEvent(msg)
		// if err != nil {
			// return nil, errors.Wrap(err, "Parsing event requests")
		// }
//
		// events = append(events, e)
	// }
//
	// return events, nil
// }
//
// // /health/ping
// func newPingRequest() net.Request {
	// return net.NewRequest(metaPing, scribe.EmptyMessage)
// }
//
// // /health/pingProxy
// func newPingProxyRequest(target uuid.UUID) net.Request {
	// return net.NewRequest(metaPingProxy, scribe.Build(func(w scribe.Writer) {
		// scribe.WriteUUID(w, "target", target)
	// }))
// }
//
// func newPingProxyResponse(success bool) net.Response {
	// return net.NewStandardResponse(scribe.Build(func(w scribe.Writer) {
		// w.Write("success", success)
	// }))
// }
//
// func readPingProxyRequest(req net.Request) (id uuid.UUID, err error) {
	// return scribe.ReadUUID(req.Body(), "target")
// }
//
// func readPingProxyResponse(res net.Response) (success bool, err error) {
	// if err = res.Error(); err != nil {
		// return
	// }
//
	// err = res.Body().Read("success", &success)
	// return
// }
//
// // /events/pull
// func newPushPullRequest(source uuid.UUID, version int, events []event) net.Request {
	// return net.NewRequest(metaPushPull, scribe.Build(func(w scribe.Writer) {
		// scribe.WriteUUID(w, "id", source)
		// w.Write("ver", version)
		// w.Write("events", events)
	// }))
// }
//
// func newPushPullResponse(success []bool, events []event) net.Response {
	// return net.NewStandardResponse(scribe.Build(func(w scribe.Writer) {
		// w.Write("success", success)
		// w.Write("events", events)
	// }))
// }
//
// func readPushPullRequest(req net.Request) (id uuid.UUID, ver int, events []event, err error) {
	// id, err = scribe.ReadUUID(req.Body(), "id")
	// if err != nil {
		// return
	// }
//
	// ver, err = scribe.ReadInt(req.Body(), "ver")
	// if err != nil {
		// return
	// }
//
	// events, err = serverReadEvents(req.Body(), "events")
	// return
// }
//
// func readPushPullResponse(res net.Response) (success []bool, events []event, err error) {
	// if err = res.Error(); err != nil {
		// return
	// }
//
	// if err = res.Body().Read("success", &success); err != nil {
		// return
	// }
//
	// events, err = serverReadEvents(res.Body(), "events")
	// return
// }
//
// // /dir/list
// func newDirListRequest() net.Request {
	// return net.NewRequest(metaDirList, scribe.EmptyMessage)
// }
//
// func newDirListResponse(events []event) net.Response {
	// return net.NewStandardResponse(scribe.Build(func(w scribe.Writer) {
		// w.Write("events", events)
	// }))
// }
//
// func readDirListResponse(res net.Response) ([]event, error) {
	// if err := res.Error(); err != nil {
		// return nil, err
	// }
//
	// return serverReadEvents(res.Body(), "events")
// }
//
// // /dir/apply
// func newDirApplyRequest(events []event) net.Request {
	// return net.NewRequest(metaDirApply, scribe.Build(func(w scribe.Writer) {
		// w.Write("events", events)
	// }))
// }
//
// func readDirApplyRequest(req net.Request) ([]event, error) {
	// return serverReadEvents(req.Body(), "events")
// }
//
// func newDirApplyResponse(success []bool) net.Response {
	// return net.NewStandardResponse(scribe.Build(func(w scribe.Writer) {
		// w.Write("success", success)
	// }))
// }
//
// func readDirApplyResponse(res net.Response) (msgs []bool, err error) {
	// if err = res.Error(); err != nil {
		// return nil, err
	// }
//
	// err = res.Body().Read("success", &msgs)
	// return
// }
//
// // /store/put
// func newStorePutRequest(key string, val string, expected int) net.Request {
	// return net.NewRequest(metaDirApply, scribe.Build(func(w scribe.Writer) {
		// w.Write("key", key)
		// w.Write("val", val)
		// w.Write("ver", expected)
	// }))
// }
//
// func readStorePutRequest(req net.Request) (key string, val string, ver int, err error) {
	// if err = req.Body().Read("key", &key); err != nil {
		// return
	// }
	// if err = req.Body().Read("val", &val); err != nil {
		// return
	// }
	// if err = req.Body().Read("ver", &ver); err != nil {
		// return
	// }
	// return
// }
//
// func newStorePutResponse() net.Response {
	// return net.NewStandardResponse(scribe.Build(func(w scribe.Writer) {
		// // w.Write("", success)
	// }))
// }
//
// // func readDirApplyResponse(res net.Response) (msgs []bool, err error) {
	// // if err = res.Error(); err != nil {
		// // return nil, err
	// // }
// //
	// // err = res.Body().Read("success", &msgs)
	// // return
// // }
