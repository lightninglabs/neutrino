# P Models

This tree contains executable P models for neutrino concurrency and scheduling
logic. The models should describe the desired distributed-systems contract first,
then let the implementation follow that contract.

## Layout

- `neutrinoquery/` models query scheduling, timeouts, cancellation, and work
  stealing.
- `neutrinoheadersync/` models checkpoint-backed header range assignment,
  range stealing, out-of-order completion, and ordered commit.
- `scripts/` contains shared model check entrypoints.
- `PGenerated/` and `PCheckerOutput/` are generated at repo root and ignored.

## Rules

- Keep ideal invariants separate from implementation shortcuts.
- Keep known-bad behavior in explicit counterexample test cases so the default
  suite remains green.
- As the Go packages land in later PRs, add bridge or trace replay tests that
  exercise the real implementation against the same model stories.
