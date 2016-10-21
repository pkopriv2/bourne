package convoy

import (
	"fmt"
	"time"

	uuid "github.com/satori/go.uuid"
)

type TimeoutError struct {
	dur time.Duration
	msg string
}

func (e TimeoutError) Error() string {
	return fmt.Sprintf("Timeout[%v]: %v", e.dur, e.msg)
}

type NoSuchMemberError struct {
	id uuid.UUID
}

func (e NoSuchMemberError) Error() string {
	return fmt.Sprintf("No such member [%v]", e.id)
}
