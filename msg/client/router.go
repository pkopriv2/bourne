package client

import (
	"github.com/pkopriv2/bourne/msg/wire"
	"github.com/pkopriv2/bourne/utils"
)


func NewRouter(mainRecv <-chan wire.Packet, mainSend chan<- wire.Packet) (*routingTable, func(utils.Controller, []interface{})) {
	table := newRoutingTable()

	return func(state utils.Controller, args []interface{}) {
		var p wire.Packet
		for {
			select {
			case p = <-mainRecv:
			case mainSend<-p:
			}
		}
	}
}
