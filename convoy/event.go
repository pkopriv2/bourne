package convoy

import (
	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/scribe"
	uuid "github.com/satori/go.uuid"
)

// The event is the fundamental unit of change dissmenation.
type event interface {
	scribe.Writable

	// Applies the event to the directory.
	Apply(u *dirUpdate) bool
}

// Used to parse events as they come off the wire.
const (
	evtMemberAdd    = 1
	evtMemberDel    = 2
	evtMemberUpdate = 3
	evtData         = 4
)

func readEvent(r scribe.Reader) (event, error) {
	var typ int
	if err := r.Read("type", &typ); err != nil {
		return nil, err
	}

	switch typ {
	default:
		return nil, errors.Errorf("Cannot parse event.  Unknown type [%v]", typ)
	case evtData:
		return readDataEvent(r)
	case evtMemberAdd:
		return readMemberAddEvent(r)
	case evtMemberDel:
		return readMemberDelEvent(r)
	case evtMemberUpdate:
		return readMemberUpdateEvent(r)
	}
}

// The primary data event type.
type dataEvent struct {
	Id   uuid.UUID
	Attr string
	Val  string
	Ver  int
	Del  bool
}

func (e *dataEvent) Write(w scribe.Writer) {
	w.Write("type", int(evtData))
	w.Write("id", e.Id.String())
	w.Write("attr", e.Attr)
	w.Write("val", e.Val)
	w.Write("ver", e.Ver)
	w.Write("del", e.Del)
}

func readDataEvent(r scribe.Reader) (*dataEvent, error) {
	id, err := scribe.ReadUUID(r, "id")
	if err != nil {
		return nil, err
	}

	event := &dataEvent{Id: id}
	if err := r.Read("attr", &event.Attr); err != nil {
		return nil, err
	}
	if err := r.Read("val", &event.Val); err != nil {
		return nil, err
	}
	if err := r.Read("ver", &event.Ver); err != nil {
		return nil, err
	}
	if err := r.Read("del", &event.Del); err != nil {
		return nil, err
	}
	return event, nil
}

func (e *dataEvent) Apply(tx *dirUpdate) bool {
	if e.Del {
		return tx.DelMemberAttr(e.Id, e.Attr, e.Ver)
	} else {
		return tx.AddMemberAttr(e.Id, e.Attr, e.Val, e.Ver)
	}
}

// a member join
type memberAddEvent struct {
	Id   uuid.UUID
	Host string
	Port string
	Ver  int
}

func newMemberAddEvent(m *member) event {
	return &memberAddEvent{m.Id, m.Host, m.Port, m.Version}
}

func readMemberAddEvent(r scribe.Reader) (*memberAddEvent, error) {
	id, err := scribe.ReadUUID(r, "id")
	if err != nil {
		return nil, err
	}

	event := &memberAddEvent{Id: id}
	if err := r.Read("host", &event.Host); err != nil {
		return nil, err
	}
	if err := r.Read("port", &event.Port); err != nil {
		return nil, err
	}
	if err := r.Read("ver", &event.Ver); err != nil {
		return nil, err
	}

	return event, nil
}

func (e *memberAddEvent) Write(w scribe.Writer) {
	w.Write("type", int(evtMemberAdd))
	w.Write("id", e.Id.String())
	w.Write("host", e.Host)
	w.Write("port", e.Port)
	w.Write("ver", e.Ver)
}

func (e *memberAddEvent) Apply(tx *dirUpdate) bool {
	return tx.AddMember(newMember(e.Id, e.Host, e.Port, e.Ver, Alive))
}

// a member leave event
type memberDelEvent struct {
	Id  uuid.UUID
	Ver int
}

func readMemberDelEvent(r scribe.Reader) (*memberDelEvent, error) {
	id, err := scribe.ReadUUID(r, "id")
	if err != nil {
		return nil, err
	}

	ver, err := scribe.ReadInt(r, "ver")
	if err != nil {
		return nil, err
	}

	return &memberDelEvent{Id: id, Ver: ver}, nil
}

func (e *memberDelEvent) Write(w scribe.Writer) {
	w.Write("type", int(evtMemberDel))
	w.Write("id", e.Id.String())
	w.Write("ver", e.Ver)
}

func (e *memberDelEvent) Apply(tx *dirUpdate) bool {
	return tx.DelMember(e.Id, e.Ver)
}

// a member alive/failed event.
type memberStatusEvent struct {
	Id     uuid.UUID
	Status MemberStatus
	Ver    int
}

func readMemberUpdateEvent(r scribe.Reader) (*memberStatusEvent, error) {
	id, err := scribe.ReadUUID(r, "id")
	if err != nil {
		return nil, err
	}

	stat, err := scribe.ReadInt(r, "status")
	if err != nil {
		return nil, err
	}

	ver, err := scribe.ReadInt(r, "ver")
	if err != nil {
		return nil, err
	}

	return &memberStatusEvent{Id: id, Status: MemberStatus(stat), Ver: ver}, nil
}

func (e *memberStatusEvent) Write(w scribe.Writer) {
	w.Write("type", int(evtMemberUpdate))
	w.Write("id", e.Id.String())
	w.Write("status", int(e.Status))
	w.Write("ver", e.Ver)
}

func (e *memberStatusEvent) Apply(tx *dirUpdate) bool {
	return tx.UpdateMember(e.Id, e.Status, e.Ver)
}
