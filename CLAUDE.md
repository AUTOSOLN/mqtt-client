# mqtt-client

Team standards. Follow these when planning, designing and writing code.

@standards/principles.md
@standards/writingtests.md
@standards/bestpractices_go.md

## Plans and designs

The built-in Plan and Explore agents don't load this file. Write plans, designs and threat analyses in the main session, or in a subagent told to read `standards/` first. Before a plan or design goes to review, check it against every file in `standards/` and say in the document which rules it touches.

## Before saying done

Run `gofmt -l .`, `go vet ./...` and `go test -race -count=1 ./...` (what CI runs), plus `golangci-lint run ./...` if it is installed, and say whether each passed. When the change touches connection handling, also run the integration tests (`cd integration && go test -race ./...`). If one fails, report the failure; don't call the work done. Check markdown files for formatting.
