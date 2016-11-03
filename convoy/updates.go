package convoy

// func newJoin(member Member) update {
// return &join{member}
// }
//
// func newLeave(memberId uuid.UUID, version int) update {
// return &leave{memberId, version}
// }
//
// func parseUpdate(ctx common.Context, r enc.Reader) (update, error) {
// var typ string
// if err := r.Read("type", &typ); err != nil {
// return nil, errors.Wrap(err, "Unable to parse update")
// }
//
// switch typ {
// case "join":
// return readJoin(ctx, r)
// case "leave":
// return readLeave(ctx, r)
// }
//
// return nil, errors.New("Unable to parse update.  Invalid type.")
// }
//
// type leave struct {
// memberId uuid.UUID
// version  int
// }
//
// func (d *leave) Re() uuid.UUID {
// return d.memberId
// }
//
// func (d *leave) Version() int {
// return d.version
// }
//
// func (l *leave) Write(w enc.Writer) {
// w.Write("type", "leave")
// w.Write("re", l.memberId.String())
// w.Write("version", l.version)
// }
//
// func (d *leave) Apply(r Roster) bool {
// return r.leave(d.memberId, d.version)
// }
//
// func readLeave(ctx common.Context, r enc.Reader) (*leave, error) {
// var version int
// if err := r.Read("version", &version); err != nil {
// return nil, errors.Wrap(err, "Unable to read leave update")
// }
//
// id, err := readUUID(r, "re")
// if err != nil {
// return nil, errors.Wrap(err, "Unable to read leave update")
// }
//
// return &leave{id, version}, nil
// }
//
// type join struct {
// member Member
// }
//
// func (p *join) Re() uuid.UUID {
// return p.member.Id()
// }
//
// func (p *join) Version() int {
// return p.member.Version()
// }
//
// func (p *join) Write(w enc.Writer) {
// w.Write("type", "join")
// w.Write("member", p.member)
// }
//
// func (p *join) Apply(r Roster) bool {
// return r.join(p.member)
// }
//
// func readJoin(ctx common.Context, r enc.Reader) (*join, error) {
// var message enc.Message
// if err := r.Read("member", &message); err != nil {
// return nil, errors.Wrap(err, "Unable to read join update")
// }
//
// member, err := readMember(ctx, message)
// if err != nil {
// return nil, errors.Wrap(err, "Unable to read join update")
// }
//
// return &join{member}, nil
// }
//
//
// // func newFail(member Member) update {
// // return &fail{member}
// // }
// //
// // type fail struct {
// // member Member
// // }
// //
// // func (p *fail) Re() uuid.UUID {
// // return p.member.Id()
// // }
// //
// // func (p *fail) Version() int {
// // return p.member.Version()
// // }
// //
// // func (p *fail) Apply(r Roster) bool {
// // return r.fail(p.member)
// // }
