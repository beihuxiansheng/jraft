# jraft
Yet another Raft consensus implementation

The core algorithm is implemented based on the TLA+ spec, whose safety and liveness are proven.

Supported Features,
1. Core Algorithm__
2. Configuration Change Support__
3. Client Request Support__

working on log compact feature__


it's always safer to implement such kind of algorithm based on Math description other than natural languge description.
there should be an auto conversion from TLA+ to programming languages, even they are talking things in different ways, but they are identical
