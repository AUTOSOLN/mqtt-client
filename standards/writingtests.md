# Writing tests

These rules apply to every language in the repo. How to apply them in a given language (test runner, naming, flags, where integration tests live) is in that language's guide, for example [bestpractices_go.md](bestpractices_go.md#tests).

## Start from risk

Tests exist to reduce a feature's key risks, not to tick boxes.

- Before writing tests, list the feature's key risks (for example: loses a QoS 1 message, deadlocks on reconnect, delivers data that lies) and the cheapest effective way to reduce each one. Put this list in the plan so reviewers can check the tests against it.
- The cheapest effective check isn't always an automated test. A short-lived feature might only need a manual check before each release.
- Unit, integration and UI tests that don't cover the key risks give a false sense of being done.

## What makes a test good

Aim for all three. They pull against each other (an empty test never fails falsely, but catches nothing), so balance them.

- **Fidelity:** the test fails when the code is broken. Cover every path through the code, and assert on all the relevant results.
- **Resilience:** the test passes when the code works. Test through the exported API, not internals, so a refactor doesn't break it. A flaky test has no resilience.
- **Precision:** a failure tells you where the defect is. Keep tests small and focused, and give them descriptive names. Integration tests check state at every boundary they cross, not only at the end.

## Check results, not calls

- By default, assert on what the code produces: return values, stored state, messages delivered.
- Prefer fakes and stubs to mocks. A test that only verifies a mock was called can pass while the feature is broken.
- Verify calls only when _how_ the code works is part of being correct: the number or order of calls matters. Examples: exactly one alert is published, a write path does no more than N disk syncs, locks are taken in a fixed order to avoid deadlock. Check results too.

## Unit tests

- Each test checks one behavior. Several asserts are fine if they describe that behavior; prefer table-driven tests.
- Unit tests are deterministic. No real clock, randomness, external network, fixed ports, or shared on-disk state. Inject a clock, dialer or random source instead.
- An in-process loopback server on port 0 (for example an in-process HTTP test server or an in-process broker such as `cmd/internal/clitest`) is allowed in a unit test if it is deterministic and the test runs under 100 ms.
- Each unit test runs in under 100 ms. Slower tests are integration tests.
- Name each test for the unit, the scenario and the expected result, so a failure tells you which widget broke. Failure messages say got X, want Y.
- Never sleep to wait for something; use channels or events, polling with a deadline, or a fake clock.
- Tests are independent: each gets its own temporary directory and port-0 listeners, uses no globals, and passes when run in random order, repeated, and (where the language supports it) under the race detector.

## Integration tests

- Tests that need real time, randomness, or networking beyond an in-process loopback server are integration tests. They are kept apart from unit tests so the unit suite stays fast; the language guide says where.
- Log the random seed so a failure can be replayed.

## What to cover

- The failure path: assert that each failure leaves a trace (returned error, handler call, log record, or a CLI exit status and message).
- Recovery: drop the connection, restore the broker, and assert the client resumes on its own.
- Security settings never degrade, e.g. a server certificate that fails verification is refused and the client never falls back to plaintext.
- Every bug fix adds a regression test, with the GitHub issue number in a comment.

## Hygiene

- A flaky test is a bug: fix it or delete it, don't retry it.
- Every skipped test needs a reason. A skip for a missing external tool (mosquitto, python3) says how to provide it; any other skip names a GitHub issue.

## Sources

- [Testing State vs. Testing Interactions](https://testing.googleblog.com/2013/03/testing-on-toilet-testing-state-vs.html)
- [Effective Testing](https://testing.googleblog.com/2014/05/testing-on-toilet-effective-testing.html)
- [Risk-Driven Testing](https://testing.googleblog.com/2014/05/testing-on-toilet-risk-driven-testing.html)
