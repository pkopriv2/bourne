package circuit

// func TestState_noTransition(t *testing.T) {
// state := newState(1, func(c WorkerSocket, args []interface{}) {})
//
// controller := state.run([]interface{}{})
//
// result := <-controller.Wait()
// assert.Equal(t, TerminalState, result.Target)
// assert.Nil(t, result.Args)
// }
//
// func TestState_workerfail(t *testing.T) {
// e := errors.New("error")
//
// state := newState(1, func(c WorkerSocket, args []interface{}) {
// c.Fail(e)
// })
//
// controller := state.run([]interface{}{})
// result := <-controller.Wait()
//
// assert.Equal(t, FailureState, result.Target)
// assert.Equal(t, []interface{}{e}, result.Args)
// }
//
// func TestState_externalFail(t *testing.T) {
// e := errors.New("error")
//
// returned := false
// state := newState(1, func(c WorkerSocket, args []interface{}) {
// <-c.Close()
// returned = true
// })
//
// controller := state.run([]interface{}{})
// select {
// case controller.Transition() <- Fail(e):
// case <-controller.Wait():
// t.Fail()
// }
//
// result := <-controller.Wait()
// assert.Equal(t, FailureState, result.Target)
// assert.Equal(t, []interface{}{e}, result.Args)
// assert.True(t, returned)
// }
//
// func TestState_multiWorker(t *testing.T) {
// e := errors.New("error")
//
// returned1 := false
// worker1 := func(c WorkerSocket, args []interface{}) {
// c.Fail(e)
// returned1 = true
// }
//
// returned2 := false
// worker2 := func(c WorkerSocket, args []interface{}) {
// time.Sleep(1 * time.Second)
// returned2 = true
// }
//
// returned3 := false
// worker3 := func(c WorkerSocket, args []interface{}) {
// time.Sleep(1 * time.Second)
// returned3 = true
// }
//
// state := newState(1, worker1, worker2, worker3)
//
// controller := state.run([]interface{}{})
// result := <-controller.Wait()
//
// assert.Equal(t, FailureState, result.Target)
// assert.Equal(t, []interface{}{e}, result.Args)
// assert.True(t, returned1)
// assert.True(t, returned2)
// assert.True(t, returned3)
// }
//
// func TestStateMachine_empty(t *testing.T) {
// factory := BuildStateMachine()
// machine := factory.Start(1)
//
// control := machine.Control()
// assert.NotNil(t, <-control.Wait())
// }
//
// func TestStateMachine_controlAfterResult(t *testing.T) {
// factory := BuildStateMachine()
// machine := factory.Start(1)
//
// c1 := machine.Control()
// <-c1.Wait()
// c2 := machine.Control()
// assert.NotNil(t, <-c2.Wait())
// }
//
// func TestStateMachine_IllegalTransition(t *testing.T) {
// factory := BuildStateMachine()
// factory.AddState(1, func(c WorkerSocket, args []interface{}) {
// c.Next(2)
// })
//
// machine := factory.Start(1)
//
// c := machine.Control()
// assert.NotNil(t, <-c.Wait())
// }
//
// func TestStateMachine_SingleState(t *testing.T) {
// factory := BuildStateMachine()
// factory.AddState(1, func(c WorkerSocket, args []interface{}) {})
//
// machine := factory.Start(1, "args")
//
// c := machine.Control()
// assert.Nil(t, <-c.Wait())
// assert.Equal(t, []Transition{Transition{1, []interface{}{"args"}}, Terminal()}, c.Summary())
// }
//
// func TestStateMachine_MultiState(t *testing.T) {
// factory := BuildStateMachine()
//
// factory.AddState(1, func(c WorkerSocket, args []interface{}) {
// c.Next(2, "2")
// })
// factory.AddState(2, func(c WorkerSocket, args []interface{}) {
// c.Next(3, "3")
// })
// factory.AddState(3, func(c WorkerSocket, args []interface{}) {
// })
//
// machine := factory.Start(1, "1")
// c := machine.Control()
//
// assert.Nil(t, <-c.Wait())
// assert.Equal(t, []Transition{
// Transition{1, []interface{}{"1"}},
// Transition{2, []interface{}{"2"}},
// Transition{3, []interface{}{"3"}},
// Terminal()}, c.Summary())
// }
//
// func TestStateMachine_ExternalTransition(t *testing.T) {
// factory := BuildStateMachine()
//
// factory.AddState(1, func(c WorkerSocket, args []interface{}) {
// time.Sleep(100 * time.Millisecond)
// c.Next(2)
// })
// factory.AddState(2, func(c WorkerSocket, args []interface{}) {
// c.Next(3)
// })
// factory.AddState(3, func(c WorkerSocket, args []interface{}) {
// })
//
// machine := factory.Start(1)
//
// c := machine.Control()
//
// // skip 2.  TODO: this is susceptible to timing errors...
// select {
// case <-c.Wait():
// t.Fail()
// return
// case c.Transition() <- State(3):
// }
//
// assert.Nil(t, <-c.Wait())
// assert.Equal(t, []Transition{
// State(1),
// State(3),
// Terminal()}, c.Summary())
// }
//
// func TestStateMachine_ExternalFailure(t *testing.T) {
// factory := BuildStateMachine()
//
// factory.AddState(1, func(c WorkerSocket, args []interface{}) {
// time.Sleep(100 * time.Millisecond)
// c.Next(2)
// })
// factory.AddState(2, func(c WorkerSocket, args []interface{}) {
// c.Next(3)
// })
// factory.AddState(3, func(c WorkerSocket, args []interface{}) {
// })
//
// machine := factory.Start(1)
//
// c := machine.Control()
//
// // fail
// e := errors.New("error")
// select {
// case <-c.Wait():
// t.Fail()
// return
// case c.Transition() <- Fail(e):
// }
//
// assert.Equal(t, e, <-c.Wait())
// }
