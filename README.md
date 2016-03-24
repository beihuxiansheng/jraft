# jraft
Yet another Raft consensus implementation

The core algorithm is implemented based on the TLA+ spec, whose safety and liveness are proven.

Pending work items,
1. Add Configuration Change Support
2. Add Log Compact Support
3. Add Client Request Support

it's always safer to implement such kind of algorithm based on Math description other than natural languge description.
there should be an auto conversion from TLA+ to programming languages, even they are talking things in different ways, but they are identical
