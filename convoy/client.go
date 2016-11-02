package convoy

// type cclient struct {
// client net.Client
// }
//
// func newClient(client net.Client) client {
// return &cclient{client: client}
// }
//
// func (c *cclient) Close() error {
// return c.client.Close()
// }
//
// func (c *cclient) Ping() (bool, error) {
// _, err := c.client.Send(newPingRequest())
// if err != nil {
// return false, err
// }
//
// return true, nil
// }
//
// // func (c *cclient) PingProxy(id uuid.UUID) (bool, error) {
// // resp, err := c.client.Send(newPingProxyRequest(id))
// // if err != nil {
// // return false, errors.New(err)
// // }
// //
// // return readPingProxyResponse(resp)
// // }
// //
// // func (c *cclient) Update(updates []update) ([]bool, error) {
// // resp, err := c.client.Send(newUpdateRequest(updates))
// // if err != nil {
// // return nil, errors.New(err)
// // }
// //
// // return readUpdateResponseBody(resp)
// // }
