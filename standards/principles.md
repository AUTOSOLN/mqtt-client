# Engineering principles

## General caveats

Priorities: correct data, delivered quickly, first; reliability second; aesthetics third.

The client and its tools run on networks and brokers we don't control and often can't reproduce. Plan for not having the environment where a failure happened.

Always write code such that you can add a unit or integration test to cover a scenario nobody anticipated.

Industrial networks are unreliable. If the switch between the client and the broker loses power, or a radio link goes quiet in a storm, an application on `Client.Run` should reconnect and carry on without human intervention once the network is back.

- Failures must not be hidden. Every failure leaves a trace the application or operator can find: a returned error, a `Handlers` callback (`OnDisconnect`, `OnConnectError`, ...) or an `Options.Logger` record. A CLI tool prints it and exits with a non-zero status.
- Degrade, don't crash. Library code never panics or exits because of the network, the broker or its input. It returns an error or reports through a handler, and leaves the decision to stop to the application.
- Fail safe, never fail open. A failure never leads to weaker security: a TLS connection that fails verification is not retried in plaintext or without verification. If a failure may have corrupted what the client delivers, it reports an error rather than deliver data that might lie.
- Releases can be hard to roll out (air gaps, security reviews). Where a likely failure can be worked around, prefer a documented option or procedure to waiting for a new release.

## Code

Claude-generated code should be understandable by humans. Patterns should be identifiable. Comments should be concise and written for people, not AI. A human should be able to inherit and maintain the work without Claude.

Less code provides more value and lowers liability. Reuse the standard library and existing code before writing new code. Don't add abstractions, options or dependencies for needs we don't have yet. Every dependency has to get through users' security reviews.

Every change to dependencies (Go modules in `go.mod`, `go.upstream.mod` and `integration/go.mod`), including upgrades, is vetted and reviewed by a human. List it in the plan, or in the PR if it comes up later.

An option (an `Options` or `Server` field, or a CLI flag) is worth adding when it gives a workaround for a likely failure, or a test tool needs it to reproduce a scenario.

Given that git has a history, delete dead code rather than leaving it in place or commenting it out. This includes unused options and flags.

No hidden or undocumented flags, environment variables, config keys or hooks, including ones added to make testing easier. Undocumented options are a security risk. Each CLI tool describes all of its flags in its `-h` output and its README (see [bestpractices_go.md](bestpractices_go.md#command-line-tools)). When a test needs a hook, use one of these instead:

- A documented, supported option (for example `Options.Dial`).
- Dependency injection in tests (see [writingtests.md](writingtests.md)).
- Code that compiles only under a test build tag, so it never ships in a release binary.

### What a plan contains

- **Acceptance criteria.**
- **Key risks**, and the cheapest effective way to reduce each one (see [writingtests.md](writingtests.md#start-from-risk)).
- **Design patterns.** Name the patterns the design uses, and any you considered and rejected, with a sentence on why. Map each one to the Go construct that implements it (see [bestpractices_go.md](bestpractices_go.md#design-patterns-and-solid)). A pattern is a tool, not a goal: "none needed" is a valid answer, and a pattern added for a need we don't have breaks the [Code](#code) rules.
- **SOLID.** For each new or changed component, say how it meets each of the five principles, or why one doesn't apply, in terms of Go constructs (see [bestpractices_go.md](bestpractices_go.md#design-patterns-and-solid)).
- **Diagrams in Mermaid**, where they apply. If one would show nothing useful, say so in one line instead.
  - **State machine** (`stateDiagram-v2`) for anything with states: a connection, a session, an in-flight QoS 2 message.
  - **Data flow** (`flowchart`) showing where data enters, where it goes and which trust boundaries it crosses. The [security review](#security-compliance) uses this diagram.
  - **Concurrency and synchronization** (`sequenceDiagram`) showing which goroutines exist, who starts and stops each one, and which channels and locks they hand off through.

#### Diagrams must render

Obsidian is the reference renderer. A diagram isn't done until it renders there without errors and reads cleanly: no overlapping labels, nothing tiny or cut off.

- Check it in Obsidian, or in a harness that uses the same Mermaid version Obsidian bundles. Newer Mermaid syntax that Obsidian's version doesn't support counts as broken.
- Look at the rendered result, not only the error output. A diagram can render without errors and still be unreadable.
- Habits that keep diagrams working in Obsidian:
  - In flowcharts, quote labels: `id["Title<br/>detail"]`, and `-->|"label"|` for edge labels.
  - In state diagrams, don't quote transition labels (`Connected --> Reconnecting: keepalive timeout`); the quotes show up in the diagram.
  - An edge that enters a subgraph runs across the subgraph's title. Keep its label short, and check the title is still readable.
  - No `{` or `}` inside Mermaid labels; braces are shape syntax.
  - Prefer `flowchart TB`. Use `LR` only for a short single row.
  - Keep each diagram to about 15 nodes; split it if it grows.
  - No `|` inside a Markdown table cell, even inside inline code.

### Security compliance

The client runs inside industrial networks whose owners must meet security frameworks. Treat the list below as the current set, not a closed one; add a framework here when one becomes relevant.

In the security part of design review, check the design against every framework below. In the design doc's security section, flag each place the design touches one: name the framework and requirement, then say how the design meets it, why it doesn't apply, or that it's an open question. Don't guess at a requirement; raise it.

Where deployments differ, meet a requirement with a documented option and a secure default, not a special build.

#### Purdue model: no level skipping

Many OT networks are segmented by Purdue level: 0 field devices, 1 controllers, 2 HMI/SCADA, 3 site operations, 3.5 IT/OT DMZ, 4–5 enterprise and cloud. Traffic may only pass between adjacent levels, and all traffic between OT (levels 0–3) and IT (levels 4–5) must stop in the DMZ.

- The client opens connections only to the servers it is configured with. It never relays or bridges between them, so it can't become a shortcut between levels.
- A connection can be bound to a chosen local interface or address (an `Options.Dial` built on a `net.Dialer` with `LocalAddr`), so a client on a dual-homed host doesn't join the two networks.
- The design doc says which ports and connections a feature uses and in which direction, so an auditor can review it.

#### Access control, monitoring and patching

- Access control: support authentication (username and password, client certificates) and TLS with server verification on by default. Turning verification off takes an explicit, documented option (such as the tools' `-insecure`).
- Monitoring: connection failures and security-relevant events (refused CONNECT, failed TLS verification) reach the application through errors, handlers and the logger, so its monitoring can collect them.
- Patching: keep dependencies few and current, so fixes can be released and reviewed quickly.

### The MQTT data path

CIA priorities for user data through the client:

1. Integrity
2. Availability
3. Confidentiality

The data the client delivers must not lie: no lost QoS 1 or 2 messages without an error, no duplicates beyond what the QoS level allows, no reordering the spec forbids.

The data should be traceable: an application can tell what was sent, when, and what the broker did with it (acknowledged, refused, never completed). The CLI tools' `-report` output exists for this.

The typical use case is Sparkplug B, spec versions 2.2 and 3.0.
