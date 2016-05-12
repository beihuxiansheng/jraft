# jraft
Yet another Raft consensus implementation

The core algorithm is implemented based on the TLA+ spec, whose safety and liveness are proven.

## Supported Features,
- [x] Core Algorithm, safety, liveness are proven
- [x] Configuration Change Support, add or remove servers one by one without limitation
- [x] Client Request Support, to be enhanced, but is working now.
- [x] **Urgent commit**, see below
- [x] log compaction 

> Urgent Commit, is a new feature introduced by this implementation, which enables the leader asks all other servers to commit one or more logs if commit index is advanced. With Urgent Commit, the system's performance is highly improved and the heartbeat interval could be increased to seconds,depends on how long your application can abide when a leader goes down, usually, one or two seconds is fine. 

## About this implementation
it's always safer to implement such kind of algorithm based on Math description other than natural languge description.
there should be an auto conversion from TLA+ to programming languages, even they are talking things in different ways, but they are identical

## Code Structure
I know it's lack of documentations, I will try my best, but if you can help, let me know.
* **net.data.technology.jraft**, the core algorithm implementation, you can go only with this, however, you need to implement the following interfaces,
  1. **Logger** and **LoggerFactory**
  2. **RpcClient** and **RpcClientFactory**
  3. **RpcListener**
  4. **ServerStateManager** and **SequentialLogStore**
  5. **StateMachine**
* **jraft-extensions**, some implementations for the interfaces mentioned above, it provides TCP based CompletableFuture<T> enabled RPC client and server as well as **FileBasedSequentialLogStore**, with this, you will be able to implement your own system by only implement **StateMachine** interface
* **jraft-dmprinter**, a sample application, as it's name, it's distributed message printer, for sample and testing.
