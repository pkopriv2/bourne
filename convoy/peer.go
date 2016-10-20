package convoy

type PendingUpdate interface {
	Update() Update
	Failures() int
}

type PendingUpdates interface {
	Next() []Update
}
