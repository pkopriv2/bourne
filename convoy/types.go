package convoy

import uuid "github.com/satori/go.uuid"

type PingRequest struct {
}

type PingResponse struct {
}

type ProxyPingRequest struct {
	Target uuid.UUID
}

type ProxyPingResponse struct {
	Success bool
	Err     error
}

type UpdateRequest struct {
	Update update
}

type UpdateResponse struct {
	Success bool
}

type ErrorResponse struct {
	Err error
}
