package warden

// // Server endpoints
// type rpcServer struct {
// ctx    common.Context
// logger common.Logger
// self   interface{} //place holder
// }
//
// // Returns a new service handler for the ractlica
// func newServer(ctx common.Context, self interface{}, listener netter.Listener, workers int) (netter.Server, error) {
// server := &rpcServer{ctx: ctx, logger: ctx.Logger(), self: self}
// return netter.NewServer(ctx, listener, serverInitHandler(server), workers)
// }
//
// func serverInitHandler(s *rpcServer) func(netter.Request) netter.Response {
// return func(req netter.Request) netter.Response {
// action, err := readMeta(req.Meta())
// if err != nil {
// return netter.NewErrorResponse(errors.Wrap(err, "Error parsing action"))
// }
//
// switch action {
// default:
// return netter.NewErrorResponse(errors.Errorf("Unknown action %v", action))
// case actStatus:
// return s.Status(req)
// case actReadBarrier:
// return s.ReadBarrier(req)
// case actReplicate:
// return s.Replicate(req)
// case actRequestVote:
// return s.RequestVote(req)
// case actAppend:
// return s.Append(req)
// case actUpdateRoster:
// return s.UpdateRoster(req)
// case actInstallSnapshot:
// return s.InstallSnapshot(req)
// }
// }
// }
//
// func (s *rpcServer) Status(req netter.Request) netter.Response {
// return status{s.self.Id, s.self.CurrentTerm(), s.self.Cluster()}.Response()
// }
//
// func (s *rpcServer) ReadBarrier(req netter.Request) netter.Response {
// val, err := s.self.ReadBarrier()
// if err != nil {
// return netter.NewErrorResponse(err)
// }
//
// return newReadBarrierResponse(val)
// }
//
// func (s *rpcServer) UpdateRoster(req netter.Request) netter.Response {
// update, err := readRosterUpdate(req.Body())
// if err != nil {
// return netter.NewErrorResponse(err)
// }
//
// return netter.NewErrorResponse(s.self.UpdateRoster(update))
// }
//
// func (s *rpcServer) InstallSnapshot(req netter.Request) netter.Response {
// snapshot, err := readInstallSnapshot(req.Body())
// if err != nil {
// return netter.NewErrorResponse(err)
// }
//
// resp, err := s.self.InstallSnapshot(snapshot)
// if err != nil {
// return netter.NewErrorResponse(err)
// }
//
// return resp.Response()
// }
//
// func (s *rpcServer) Replicate(req netter.Request) netter.Response {
// replicate, err := readReplicate(req.Body())
// if err != nil {
// return netter.NewErrorResponse(err)
// }
//
// resp, err := s.self.Replicate(replicate)
// if err != nil {
// return netter.NewErrorResponse(err)
// }
//
// return resp.Response()
// }
//
// func (s *rpcServer) RequestVote(req netter.Request) netter.Response {
// voteRequest, err := readRequestVote(req.Body())
// if err != nil {
// return netter.NewErrorResponse(err)
// }
//
// resp, err := s.self.RequestVote(voteRequest)
// if err != nil {
// return netter.NewErrorResponse(err)
// }
//
// return resp.Response()
// }
//
// func (s *rpcServer) Append(req netter.Request) netter.Response {
// append, err := readAppendEvent(req.Body())
// if err != nil {
// return netter.NewErrorResponse(err)
// }
//
// item, err := s.self.RemoteAppend(append)
// if err != nil {
// return netter.NewErrorResponse(err)
// }
//
// return appendEventResponse{item.Index, item.Term}.Response()
// }
//
// type rpcServer struct {
// }
