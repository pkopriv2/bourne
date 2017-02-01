package kayak

// Extracts out the raw errors.
func extractError(err error) error {
	if err == nil {
		return nil
	}

	switch err {
	default:
	case ClosedError,
		NotLeaderError,
		NotSlaveError,
		NoLeaderError,
		TimeoutError,
		InvariantError,
		EndOfStreamError,
		OutOfBoundsError,
		CompactionError:
		return err
	}

	// string comparisons for errors transferred over rpc
	switch err.Error() {
	case ClosedError.Error():
		return ClosedError
	case NotLeaderError.Error():
		return NotLeaderError
	case NotSlaveError.Error():
		return NotSlaveError
	case NoLeaderError.Error():
		return NoLeaderError
	case TimeoutError.Error():
		return TimeoutError
	case InvariantError.Error():
		return InvariantError
	case EndOfStreamError.Error():
		return EndOfStreamError
	case OutOfBoundsError.Error():
		return OutOfBoundsError
	}

	// recurse now (more performant to do direct comaparisons first.)
	if cause := extractError(cause(err)); cause != nil {
		return cause
	}

	return err
}

func cause(err error) error {
	type causer interface {
		Cause() error
	}

	cause, ok := err.(causer)
	if !ok {
		return nil
	}

	return cause.Cause()
}
