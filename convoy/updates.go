package convoy

import uuid "github.com/satori/go.uuid"

func newJoin(member Member) update {
	return &join{member}
}

func newLeave(memberId uuid.UUID, version int) update {
	return &leave{memberId, version}
}

// func newFail(member Member) update {
// return &fail{member}
// }

// type fail struct {
// member Member
// }
//
// func (p *fail) Re() uuid.UUID {
// return p.member.Id()
// }
//
// func (p *fail) Version() int {
// return p.member.Version()
// }
//
// func (p *fail) Apply(r Roster) bool {
// return r.fail(p.member)
// }

type leave struct {
	memberId uuid.UUID
	version  int
}

func (d *leave) Re() uuid.UUID {
	return d.memberId
}

func (d *leave) Version() int {
	return d.version
}

func (d *leave) Apply(r Roster) bool {
	return r.leave(d.memberId, d.version)
}

type join struct {
	member Member
}

func (p *join) Re() uuid.UUID {
	return p.member.Id()
}

func (p *join) Version() int {
	return p.member.Version()
}

func (p *join) Apply(r Roster) bool {
	return r.join(p.member)
}

func parseUpdate(data map[string]interface{}) (update, error) {
	return nil, nil

}
