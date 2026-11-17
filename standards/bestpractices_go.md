# Go best practices

How we write Go in this repo: the `mqttclient` library, its packages and the tools under `cmd/`. Distilled from [Google's Go Style Best Practices](https://google.github.io/styleguide/go/best-practices) and adapted to this codebase.

Google calls its page advisory ("neither normative nor canonical"). Treat this file the same way: it is the default, and [principles.md](principles.md) and [writingtests.md](writingtests.md) win when they disagree. The conflicts we know about are listed in [Conflicts with our standards](#conflicts-with-our-standards).

Existing code that breaks these rules is not a precedent. Don't copy it; fix it when you are already changing that code.

## What we use

Google's page assumes Google's internal tools. Here is what we use instead:

| Google assumes                 | We use                                                                                     |
| ------------------------------ | ------------------------------------------------------------------------------------------ |
| glog (`log.V`, `log.Exit`)     | `log/slog` in the library (`Options.Logger`); `cli.Logf` in the tools                      |
| `google/subcommands`           | the standard `flag` package, one binary per tool (see [Command-line tools](#command-line-tools)) |
| `go-cmp` and assertion helpers | the standard `testing` package: plain `if` checks with `t.Errorf` and `t.Fatalf`           |
| Bazel `go_library`, `testonly` | Go modules; test-only code in `_test.go` files, `...test` packages or behind tags          |
| `go_proto_library` `pb` names  | `protoc-gen-go` packages under `internal/` of the package that owns them (`sparkplug/internal/...`) |

Don't add `go-cmp`, `testify`, `cobra`, `subcommands` or another logging library to match Google. Every dependency has to get through users' security reviews.

## Naming

- Don't repeat what the call site already says: not the package (`yamlconfig.Parse`, not `yamlconfig.ParseYAMLConfig`), not the receiver (`cfg.WriteTo`, not `cfg.WriteConfigTo`), not parameter or return types.
- Functions that return something get noun names, without `Get` (`JobName`). Functions that do something get verb names (`WriteDetail`).
- Variants of one function take a suffix (`ParseInt`, `ParseInt64`). The main one may go without.
- No `util`, `helper` or `common` packages. Name a package for what it provides, and judge the name at the call site.
- Don't shadow. A `:=` inside a new block that makes a second `err` or `ctx` is a bug waiting to happen; declare the variable first and use `=`. Reusing `:=` in the same scope is fine if the old value isn't needed.
- Don't name a variable after a standard package (`url`, `bytes`, `errors`).
- Test doubles are named for their behavior: `AlwaysDenies`, `StubStore`, `spyAudit`. Shared doubles go in a package named after the real one plus `test` (`clitest`).

## Packages and files

- Put types that callers use together in one package. If a caller must import two packages to use either one, merge them. Don't put everything in one package either.
- No one-type-per-file rule. Avoid both huge files and many tiny ones; a reader should be able to guess which file holds a type.

## Errors

- Handle every error on purpose. Never discard one with `_` without a comment saying why that is safe.
- If callers need to tell errors apart, give them a sentinel (`var ErrQueueFull = errors.New("queue full")`) or a typed error, and check with `errors.Is` or `errors.As`. Never match on `err.Error()` text.
- Add context the underlying error doesn't have; don't repeat it (`os.Open` already includes the path). Name the action that failed, without "failed to" or "unable to":

  ```go
  return fmt.Errorf("load scenario %s: %w", name, err)
  ```

- If there's nothing to add, `return err`.
- `%w` makes the wrapped error part of your API: callers can match on it. Use it when they need to, and document and test that. Use `%v` when they shouldn't, especially for errors from a dependency (comqtt, yaml, protobuf) that we don't want to commit to.
- Put `%w` at the end, so the text reads outermost to innermost. When wrapping a sentinel that names a category, put it first: `fmt.Errorf("%w: topic %q", ErrBadTopic, topic)`.
- Document the sentinels and error types a package returns.

## Logging

The library logs only through `Options.Logger`, which defaults to discarding. Its main channels to the application are returned errors and `Handlers`; a log record is extra detail, never the only trace.

- Each failure is reported once, where it is handled, not at every level it passes through. Code that returns an error doesn't also log it. Code that swallows an error, retries it, or is the end of the line (the network loop, `Run`'s reconnect loop) must report it, through a handler (`OnDisconnect`, `OnConnectError`) and the logger. See [principles.md](principles.md#general-caveats): every failure leaves a trace.
- A log message says what went wrong and includes what someone needs to act on it (client ID, server address, packet ID, topic) as `slog` attributes.
- Error level means someone should act on it. Use Warn for degraded but self-healing states, and Debug for detail.
- Rate-limit logs on hot paths (per-message, per-packet, per-reconnect). The application's log is not ours to flood.
- Don't build expensive log arguments when the level is off. Check `logger.Enabled(ctx, slog.LevelDebug)` first.
- The library never logs secrets or payloads: passwords, tokens, private keys, or message contents. The tools may print payloads, since showing them is their job, but never passwords.

## Panics and crashes

The library never decides to stop the application. This is the Go side of the rule in [principles.md](principles.md#general-caveats).

- **Library code** (everything outside `cmd/`) returns errors. It doesn't call `os.Exit` or `log.Fatal`, and it doesn't panic on bad input, bad options, a misbehaving broker or I/O failures. The decision to stop is made by the application, or by a tool's `run`.
- **Connection failures are routine.** A lost or refused connection is reported (returned error, `OnDisconnect`, `OnConnectError`) and, under `Run`, retried with backoff. It never takes down the process.
- **Fail safe, never fail open.** A failure never leads to weaker security. A server certificate that fails verification ends the attempt; the client doesn't retry in plaintext or with verification off.
- **Data integrity.** If a failure may have corrupted session state (in-flight QoS 1 and 2 messages, packet IDs), end the connection and report an error rather than keep delivering. The data must not lie.
- **Tools** exit with their documented status (see [Command-line tools](#command-line-tools)); exiting non-zero is how a test tool reports failure.
- **Programmer errors.** Panic only for mistakes that can't happen if the code is correct (API misuse, impossible states). Use `Must...` helpers only on constant input, such as `regexp.MustCompile` on a literal.
- **Parsers.** A parser may panic internally and recover at its top level, but only from its own panic type, and the panic must never leave the package.
- **Recover.** Don't `recover()` from panics in application callbacks (`Handlers`): a panicking handler is the application's bug and should crash where the application can see it. Recover only inside a parser, as above, and never just to carry on.

## Documentation and comments

Comments are for people (see [principles.md](principles.md#code)). The `gocomments` skill checks them.

- Document what a reader can't see from the signature: units, ranges, nil behavior, ownership, and why.
- Concurrency: callers assume read-only operations are safe and mutating ones are not. Say so when that isn't true, or when a type is safe for concurrent use.
- Cleanup: say what the caller must close or stop, and how.
- Contexts: callers assume cancellation returns `ctx.Err()`. Document only when it doesn't.
- Point out the unusual: `if err == nil { // the topic exists`.
- Use `Example` functions with `// Output:` when a usage example helps; they are tested.

## Declarations

- Use `:=` for values you set; `var x T` when the zero value is what you want.
- Types with a `sync.Mutex` or other uncopyable field take pointer receivers and are passed by pointer.
- Maps must be made before writing. Reading a nil map is fine.
- Preallocate (`make([]T, 0, n)`) when you know the size. Otherwise pick a size from a benchmark, not a guess.
- Give channel parameters a direction (`<-chan`, `chan<-`). It documents ownership and stops the wrong side from closing the channel.
- Anything secret or unguessable (tokens, session IDs, keys, salts) comes from `crypto/rand`. Never `math/rand`, even for throwaway values. `math/rand/v2` is fine for jitter and sampling, with the source injected so tests are deterministic.

## Concurrency

The client is mostly concurrent code, and leaks or deadlocks show up in long-running applications weeks later. Keep it simple enough that a reader can see when each goroutine starts and stops.

Two principles, from Bryan Mills's talk [Rethinking Classical Concurrency Patterns](https://youtu.be/5zXAHh5tJqQ):

1. **Start goroutines when you have concurrent work to do now.** Don't start them to avoid blocking a caller, and don't keep idle ones around in case work arrives.
2. **Share by communicating.** Send the thing itself (the data, the connection, the token) on a channel, not a signal that something changed somewhere.

### APIs

- **Prefer synchronous functions.** Return the result and the error. A synchronous signature already says how errors and cancellation work. The caller can add `go` if it wants concurrency; that is easy to add and hard to take away.
- **Don't return data channels from APIs** (a future, or a goroutine feeding a returned channel). A channel that is only ever closed to signal completion, like `ctx.Done()` or `Pending.Done()`, is fine. The caller can't tell what happens if it cancels, hits an error or stops reading early, and stopping early leaks the producer. Return a slice, or an iterator (`iter.Seq2[Item, error]`) when the results should be streamed.
- **Keep concurrency an internal detail.** A function may fan work out internally, but its channels stay local, so the sender and the receiver are both visible in one place, and it returns only after its goroutines have finished.
- A long-lived part may hand out a receive-only channel for events (a subscription) only if its doc says who closes the channel, when, and what cancelling `ctx` does.

### Goroutine lifetimes

- **Every goroutine has an owner and a known end.** Whoever starts a goroutine makes sure it stops, and waits for it before returning or in its own `Close`/`Stop`. Use `sync.WaitGroup` (`wg.Go(func() { ... })`). `errgroup` is fine when the first error should cancel the rest, but making `golang.org/x/sync` a direct dependency needs review. No fire-and-forget `go f()`.
- **No goroutine blocks forever.** A goroutine stuck on a channel send or receive is a leak; the garbage collector won't stop it. Every send or receive that can block also selects on `ctx.Done()` or a stop channel. One that can't block (the channel is known to have room) doesn't need to.
- **Long-running parts** (the network loop, `Run`'s reconnect loop, a tool's listener) run until their context is cancelled, then return. A constructor that starts goroutines needs a `Close` or `Stop` that ends them, documented on the type. When one part depends on another, shut them down in reverse order of startup.
- **Bound work in flight with a semaphore, not a worker pool.** Long-lived goroutines are right for continuous work, such as a connection or a queue that drains forever. For a batch of tasks, start one goroutine per task and limit how many run at once. Idle workers waste memory and bury the interesting goroutines in a stack dump.

  ```go
  sem := make(chan struct{}, limit) // one token per task in flight
  var wg sync.WaitGroup
  loop:
  for _, task := range tasks {
  	select {
  	case sem <- struct{}{}:
  	case <-ctx.Done():
  		break loop
  	}
  	wg.Go(func() {
  		defer func() { <-sem }()
  		perform(ctx, task)
  	})
  }
  wg.Wait()
  ```

- **Never block the network loop.** Slow work goes through a bounded queue or is the caller's to do. Flow control comes from the protocol (Receive Maximum, in-flight limits): when the limit is reached, `Publish` waits on `ctx` or returns an error; it never queues without bound. An unbounded queue is a memory leak waiting for a BIRTH storm.
- **Callbacks.** `Handlers` run one at a time on a goroutine the client owns, like libmosquitto's callbacks. Document anything a handler must not do (block on the network, wait for an acknowledgement), and never call a handler while holding a lock.

### Sharing state

- **Send the thing, not a signal about it.** A message that only says "something changed" leaves the receiver to go and find out what, and that is where races and missed updates hide.
- **Limits are resources.** A buffered channel of capacity n is a semaphore with n tokens. A pool of connections is a buffered channel of connections. Acquiring one selects on `ctx.Done()`, so callers can give up.
- **Don't use `sync.Cond`.** Signals get forgotten, `Broadcast` wakes waiters that can't proceed, picky waiters starve, and a goroutine waiting on a condition can't also watch `ctx`. Use a channel instead:
  - to hand over data or a resource, send it;
  - to tell every waiter that a state has changed (loaded, stopped, now idle), close a channel;
  - to guard state you also need to select on, keep it in a channel with a buffer of one: receive to take it, send to put it back.
- **A `sync.Mutex` is still fine** for guarding a few fields or a map with short critical sections.
  - Don't copy a `sync.Mutex`, or a struct holding one; `go vet` catches some cases. Don't copy a `bytes.Buffer` either.
  - Types that hold a lock use pointer receivers, and all methods of a type use the same receiver kind.
  - Keep critical sections short. Don't do I/O, send on a channel or call a callback while holding a lock.
  - When code takes more than one lock, document the order and always take them in that order.

### Contexts

- `ctx` is the first parameter. Don't store it in a struct, except to satisfy an interface we don't own.
- Create a root context only in a tool's `run` (`signal.NotifyContext(context.Background(), os.Interrupt)`) and in tests (`t.Context()`). Library code takes its context from the caller.
- When work must outlive the caller's context (for example, sending DISCONNECT after the caller's context is cancelled), use `context.WithoutCancel(ctx)` rather than `context.Background()`, so its values carry over.
- Don't define custom context types or context-like interfaces.

### Checking and exceptions

- CI runs `go test -race -count=1 ./...`, against both `go.mod` and `go.upstream.mod`. A race report is a bug, even if the test passes.
- These patterns are chosen for clarity. If a benchmark shows that a pattern warned against here is measurably faster on a hot path, you may use it: say why in a comment and check in the benchmark.

## Function signatures and options

- Keep signatures short. `ctx context.Context` comes first and is never stored in a struct or an options struct (see [Concurrency](#concurrency)).
- When a function needs many settings, pass an options struct as the last argument. Its zero value must be a safe default.
- Use variadic functional options (`...Option`) only when most callers pass none and the API is public. They cost boilerplate.
- An option takes a value, not its own presence: `WithFailFast(true)`, not `EnableFailFast()`.
- Don't add options nobody needs yet (see [principles.md](principles.md#code)). Every user-facing config key or flag is documented.

## Command-line tools

Each tool under `cmd/` is its own binary built on the standard `flag` package. Follow `cmd/mqttpub` and `cmd/mqttsub`:

- **Shape.** `main` is only `os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))`. `run` builds its own `flag.NewFlagSet`, writes only to the writers it is given, and returns the exit status, so a test can call it directly. No `os.Exit`, `log.Fatal` or package-level `flag.X` calls outside `main`.
- **Exit status.** 0 when the run passed, 1 when it failed (an expectation didn't hold, a message was refused or lost), 2 for a setup error (bad arguments, no broker to connect to). Wrap setup failures with `cli.Setup` and map errors with `cli.ExitCode`. Document the statuses in the package comment.
- **Shared flags.** Connection, TLS and reconnect flags come from `cli.ConnFlags` and `cli.ReconnectFlags`, so every tool spells them the same way and `-config` files work everywhere. Add a connection flag there, not in one tool.
- **Flag names** are short and mosquitto-like where mosquitto has one (`-t`, `-m`, `-q`, `-u`, `-P`), otherwise lowercase words joined by hyphens (`-expect-session`, `-size-step`).
- **Context.** `run` creates the root context with `signal.NotifyContext(context.Background(), os.Interrupt)`, so Ctrl-C ends the run cleanly, and passes it down.
- **Output.** Progress goes through `cli.Logf`. With `-report`, a tool writes a JSON `cli.Report` for a test harness to read.
- **Documentation.** Every flag has help text. No hidden flags (see [principles.md](principles.md#code)). The package comment gives usage and examples, and the tool's `README.md` covers every flag.
- **Logic.** Keep reusable logic in a library package (`fuzz`, `sparkplug/verify`) or `cmd/internal/cli`; `main.go` parses flags, wires things together and reports.

Older tools (`mqttconnect`, `mqttfuzz`, `interactiveclient`) still use the global `flag` set and have no testable `run(args, stdout, stderr)`. Move them to the shape above when you are already changing them.

## Design patterns and SOLID

Plans name the patterns and SOLID principles they apply (see [principles.md](principles.md#what-a-plan-contains)). These are the Go constructs to map them to. Go has no classes or inheritance, so most patterns become an interface, a function value or a goroutine.

### SOLID in Go

| Principle             | Go construct                                                                                                                                                                                                     |
| --------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Single responsibility | A package and its main type have one reason to change. The package is named for what it provides (see [Naming](#naming) and [Packages and files](#packages-and-files)).                                          |
| Open/closed           | Extend by adding a new type that satisfies an existing interface, or by composing (embedding, wrapping), not by editing a growing `switch`. Don't make something pluggable until a second implementation exists. |
| Liskov substitution   | Every implementation of an interface honors its documented contract: errors, nil handling, concurrency safety, and what cancelling `ctx` does. Check it with a contract test (see [Tests](#tests)).              |
| Interface segregation | Small interfaces, defined by the consumer, often one or two methods (see [Interfaces](#interfaces)).                                                                                                             |
| Dependency inversion  | Constructors take their dependencies (dialer, clock, random source) as interface parameters; `main` wires in the concrete types. No globals or service locators (see [Global state](#global-state)).      |

### Patterns in Go

| Pattern                     | Go construct                                                                                                                                                                                    |
| --------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Strategy                    | An interface, or a function-typed field, chosen at construction.                                                                                                                                |
| Adapter                     | A small type or function type that wraps one API to satisfy another interface, like `http.HandlerFunc`.                                                                                         |
| Decorator, middleware       | A function that takes an interface value and returns one of the same type, adding behavior around it.                                                                                           |
| Factory                     | A `NewX(...)` constructor that returns a concrete type. No factory interfaces.                                                                                                                  |
| Builder                     | An options struct, or functional options (see [Function signatures and options](#function-signatures-and-options)).                                                                             |
| Singleton                   | Avoid it. Create one instance in `main` and pass it in (see [Global state](#global-state)).                                                                                                     |
| State machine               | A typed state constant with a transition method that returns the next state or an error, or one goroutine that owns the state and receives events on a channel. Draw it with `stateDiagram-v2`. |
| Observer, pub/sub           | Send each event to each subscriber on its own channel, with a documented close (see [Concurrency](#concurrency)). The exception is `Handlers`, which follow the libmosquitto model (see [Goroutine lifetimes](#goroutine-lifetimes)).                        |
| Command                     | A function value, or a type with a `Run(ctx) error` method.                                                                                                                                     |
| Template method             | Go has no inheritance. Pass the varying steps in as a function or a small interface.                                                                                                            |
| Pipeline, producer–consumer | Goroutines joined by bounded channels, each with an owner and a known end (see [Concurrency](#concurrency)).                                                                                    |
| Semaphore, resource pool    | A buffered channel of tokens or of the resources themselves (see [Concurrency](#concurrency)).                                                                                                  |
| Retry with backoff          | A loop with exponential backoff and jitter, using an injected clock and random source. No dependency needed.                                                                                    |
| Supervisor                  | `Run`: a loop that owns the connection, reports each loss through `OnDisconnect`, and reconnects with backoff, failing over between servers.                                                     |

## Global state

- No package-level mutable state in libraries: no global registries, service locators, singletons or caches that change behavior. Build instances with constructors and pass them in as dependencies.
- Global state breaks `-shuffle=on`, parallel tests and test doubles (see [writingtests.md](writingtests.md#unit-tests)). It also stops two clients with different settings from running in one test process.
- Allowed: true constants, lookup tables that never change, and private caches whose presence callers can't detect.
- Tools keep their state in a `config` struct built in `run`, not in package-level flag variables.

## Interfaces

- Accept interfaces, return concrete types.
- The consumer defines the interface, and it stays small (often one or two methods). Don't define an interface beside its only implementation "for testing".
- Create an interface when there are two real implementations, to break an import cycle, or to inject a fake clock, dialer or random source (see [writingtests.md](writingtests.md#unit-tests)).
- Don't export a test double from the package that owns the real type. Put it in a `...test` package or a `_test.go` file.

## Strings

- `+` for a few pieces, `fmt.Sprintf` for formatting, `fmt.Fprintf` when writing to an `io.Writer`, and `strings.Builder` when building in a loop.
- Raw string literals (backticks) for constant multi-line text.

## Tests

These add Go mechanics to [writingtests.md](writingtests.md), which comes first.

- **Names.** `Test<Unit>_<Scenario>_<Expected>`, for example `TestPublish_QoS2Reconnect_ResendsPubrel`.
- **Independence.** Use `t.TempDir()` for files and listen on `127.0.0.1:0`. Tests pass with `go test -race -count=1 -shuffle=on`.
- **Waiting.** Never `time.Sleep`. Wait on a channel, poll in a loop with a deadline, or advance a fake clock.
- **Contexts.** Use `t.Context()`; it is cancelled when the test ends.
- **Integration tests** live in `integration/`, its own module, so `go test ./...` at the root never runs them. They start their own mosquitto and skip with instructions if it isn't installed. Log any random seed with `t.Logf`. The mosquitto client test suite runs from `conformance/run.sh`.
- **Skips.** `t.Skip("reason")` every time; name the GitHub issue (`#123`) unless the skip is for a missing external tool (see [writingtests.md](writingtests.md#hygiene)).
- **Stop or continue.** Keep going after a failed check so one run reports every problem: `t.Errorf`. Stop when the rest of the test can't mean anything (setup failed, a value needed later is nil): `t.Fatalf`.
- **Tables.** Without subtests, use `t.Errorf` and `continue`. With `t.Run`, `t.Fatalf` ends only that subtest. Name the fields in table structs.
- **Goroutines.** Never call `t.Fatal` or `t.FailNow` from a goroutine other than the test's own; Go doesn't support it. Send the result back on a channel, or use `t.Errorf` and return.
- **Helpers.** Setup helpers take `t`, call `t.Helper()`, fail with `t.Fatalf` and context, and register cleanup with `t.Cleanup`. Don't write custom assertion helpers that hide what is being checked. A helper that can't fail doesn't take `t`.
- **Shared checks.** For a check several tests need, write a function that returns an `error` rather than one that takes `*testing.T`.
- **Messages.** Failure messages say `got X, want Y`, in that order. For structs, print both with `%+v`; use `reflect.DeepEqual` or `slices.Equal` to compare.
- **Real clients.** Test a client by running the real client code against a fake server, not a hand-built imitation of the client. For HTTP use `httptest.NewServer`; for MQTT, an in-process broker on `:0` (`cmd/internal/clitest`, or the fake broker in `broker_test.go`). This is a unit test if it is deterministic and runs under 100 ms; otherwise it goes in `integration/` (see [writingtests.md](writingtests.md#unit-tests)).
- **Setup scope.** Do expensive setup only in the tests that need it, not in `init`. Use `TestMain` only when every test in the package shares setup that must be torn down. Shared fixtures made with `sync.Once` must be read-only.
- **Contract tests.** When several implementations must behave the same (dialers, transports), write one exported check function in a `...test` package that takes an implementation and returns an error.

## Conflicts with our standards

Where Google's guidance and ours disagree, ours wins.

| #   | Topic                    | Google says                                                                | Our standard says                                                                                   | Resolution                                                                                                                                                                 |
| --- | ------------------------ | -------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1   | Assertion libraries      | Assertion helpers are "not idiomatic"; use plain `if` checks and `go-cmp`. | Dependencies must pass security review ([principles.md](principles.md#code)).                       | Agreed on plain `if` checks; don't add `go-cmp` or `testify`. Use `reflect.DeepEqual` and `slices.Equal`.                                                                  |
| 2   | Logging returned errors  | Don't log an error you return; let the caller decide.                      | Every failure must leave a trace ([principles.md](principles.md#general-caveats)).                  | Compatible if the failure is reported once where it is handled. Failures that end in the network loop or `Run`, with no caller, go to a handler and the logger.            |
| 3   | Crashing on invariants   | Use `log.Fatal` for broken invariants. Don't `recover()` to keep running.  | Library code never stops the application ([principles.md](principles.md#general-caveats)).          | Agreed on not recovering. Library code returns errors instead of `log.Fatal`; panics only for programmer errors. See [Panics and crashes](#panics-and-crashes).             |
| 4   | Real transports in tests | Use the real client against a test server.                                 | Unit tests have no network and no fixed ports ([writingtests.md](writingtests.md#unit-tests)).      | An in-process loopback server on `:0` is allowed in a unit test if it is deterministic and runs under 100 ms. Anything else goes in `integration/`.                         |
| 5   | Shared test setup        | `sync.Once` and `TestMain` are fine for expensive shared setup.            | No globals; tests pass in random order ([writingtests.md](writingtests.md#unit-tests)).             | Only for read-only fixtures. Anything a test changes is made per test.                                                                                                     |
| 6   | CLI library              | Prefer `google/subcommands`; cobra has pitfalls.                           | Fewer dependencies ([principles.md](principles.md#code)).                                           | Neither: the standard `flag` package, one binary per tool. See [Command-line tools](#command-line-tools).                                                                   |
| 7   | Hiding error details     | Use `%v` at system boundaries to hide internals.                           | Errors must be useful to whoever has to act on them.                                                | Compatible: wrap with `%w` where callers need to match (our sentinels, `context` errors); use `%v` for dependency errors we don't want to commit to.                       |
| 8   | Global state             | Avoid package-level mutable state.                                         | Same, and stronger: no globals in tests.                                                            | Agreed. Existing package-level mutable state is not a precedent.                                                                                                           |

## Not adopted

- Bazel `go_library` and `testonly` rules, and `go_proto_library` and `go_grpc_library` import naming. We don't use Bazel.
- glog verbosity levels (`log.V(1)` to `log.V(3)`). Use `slog` Debug and Info.
- `safehtml/template`. Use `html/template` for HTML.
- Google canonical status codes (GoTip #89). Use MQTT reason codes at the protocol boundary.

## Source

- [Go Style Best Practices](https://google.github.io/styleguide/go/best-practices), Google, read 2026-11-16.
- [Style Decisions](https://google.github.io/styleguide/go/decisions), Google, read 2026-11-16: only the concurrency topics (goroutine lifetimes, contexts, synchronous functions, copying, receiver type, crypto rand) are used here, in [Concurrency](#concurrency).
- Bryan C. Mills, [Rethinking Classical Concurrency Patterns](https://youtu.be/5zXAHh5tJqQ), GopherCon 2018: the two principles, semaphores over worker pools, channels over `sync.Cond`, and no channel-returning APIs, in [Concurrency](#concurrency).
- Not yet distilled here: the rest of Style Decisions and the [Style Guide](https://google.github.io/styleguide/go/guide).
