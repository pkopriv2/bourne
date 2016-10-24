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
	Updates []update
}

type UpdateResponse struct {
	Accepted []bool
}

type ErrorResponse struct {
	Err error
}
