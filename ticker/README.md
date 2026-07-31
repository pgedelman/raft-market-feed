# Ticker

A fault-tolerant, ordered market-data distribution system, backed by a
from-scratch Raft consensus implementation, with a live dashboard for
watching leader elections and failover as nodes are killed on demand.

## Why this exists

Market data feeds need to deliver ticks to subscribers in order, exactly
once, even when a node in the distribution cluster crashes. This project
builds the consensus layer that provides that guarantee, then puts a
market-data-shaped API and a live-failover dashboard on top of it.

## Environment setup (local machine)

1. **Install Go 1.22+**
   - macOS: `brew install go`
   - Linux: use your package manager (`apt install golang-go` on
     Debian/Ubuntu) or download from https://go.dev/dl/
   - Windows: installer from https://go.dev/dl/, or WSL2 + the Linux steps
   - Verify: `go version` should print 1.22 or later.

2. **Clone/copy this scaffold**, then from the project root:
   ```
   go build ./...   # should print nothing and exit 0
   go test ./...    # should show a handful of skipped tests, no failures
   ```
   If both of those work, your environment is correctly set up.

3. **Editor**: VS Code with the official Go extension (`golang.go`) gives
   you gofmt-on-save, inline test running, and debugger support for free —
   worth installing before you start writing the election timer logic,
   since stepping through goroutine state with the debugger will save you
   real time.

4. **Recommended: install `dlv` (Delve debugger)** for stepping through
   concurrent state transitions:
   ```
   go install github.com/go-delve/delve/cmd/dlv@latest
   ```

5. Later (Week 4), you'll also need **Node.js 18+** for the React
   dashboard — not needed yet, skip until you get there.

## Project layout

```
ticker/
├── cmd/
│   ├── node/       entry point for a single cluster member process
│   └── tickgen/    (week 3) simulated market-data tick generator
├── internal/
│   ├── raft/       consensus core — THIS IS WEEK 1-2's ENTIRE FOCUS
│   ├── store/      (week 3) tick schema, sequencing, subscriber delivery
│   ├── api/         (week 3) client-facing publish/subscribe interface
│   └── dashboard/  (week 4) WebSocket server feeding the React UI
└── README.md
```

## Where to start (Week 1)

Everything you need is scaffolded in `internal/raft/`:

- `raft.go` — `Node` struct with all state fields already defined and
  commented, plus a `TODO(week 1)` block listing exactly what to implement:
  `Start()`, the election timer loop, `RequestVote`/`AppendEntries`
  handlers, and the role-transition methods.
- `transport.go` — the RPC boundary as a Go interface, so consensus logic
  never touches sockets directly.
- `transport_memory.go` — a working in-memory transport already
  implemented for you, with a `SetPartition` method for simulating network
  splits. Use this for all of Week 1-2's testing; don't build real
  networking until correctness is proven against this fake transport.
- `raft_test.go` — three tests stubbed with `t.Skip` and a comment sketch
  of what each should assert. **Your Week 1 definition of done is:
  `TestClusterElectsALeader` passes.** Don't consider Week 1 finished
  until you've deleted that `t.Skip` and the test is green.

Suggested order inside `raft.go`:
1. `becomeFollower`, `becomeCandidate`, `becomeLeader` (just role + term
   transitions, no RPCs yet — get the state machine right in isolation)
2. `RequestVote` handler (the "should I vote for this candidate" logic
   from Raft paper §5.2, §5.4)
3. Election timer loop: on timeout, become candidate, request votes from
   all peers via the transport, become leader on majority
4. `AppendEntries` handler used as a heartbeat (empty `Entries`) to keep
   followers from timing out — this is what makes election ever stop
   after one leader wins
5. Once heartbeats keep a leader stable, delete the `t.Skip` in
   `TestClusterElectsALeader` and get it passing

Don't touch log replication (non-empty `AppendEntries`, `commitIndex`
advancement) until election is rock solid — it's a much smaller amount of
new logic once heartbeats already work, and debugging both at once is
where most implementations get stuck.

## Reference

- The original Raft paper ("In Search of an Understandable Consensus
  Algorithm", Ongaro & Ousterhout) — Figure 2 is essentially your spec for
  `transport.go`'s RPC arguments.
- MIT 6.824's Raft lab is the standard reference implementation shape this
  scaffold is loosely modeled on, useful if you get stuck on a specific
  edge case (log matching, election restrictions).
