# Conformance baseline notes

`conformance-baseline.json` is the floor a PR must clear in CI. As of
2026-05-18 every service is pinned at its full observed pass count —
86,327 / 86,327 variants across all 37 services. CI fails any PR that
regresses any service below its current baseline.

## Bumping baselines

When a service grows new variants (Smithy model update) or fakecloud
implementation closes a real gap, run:

```sh
cargo run -p fakecloud-conformance -- run --services <service>
```

If pass count is higher than baseline and the fix is real (not gaming),
update the per-service entry in `conformance-baseline.json` to the new
exact count and commit alongside the change that lifted it.

## Past flake notes

Prior to the 2026-05 push to true 100%, two services carried a
deliberate floor below observed pass count to absorb cross-run flake:

- **cognito-idp** — observed 4416 / 4426 / 4434 / 4479 across 4 runs of
  unchanged code on the harness side.
- **kms** — observed 2017 / 2024 / 2050 / ~2030 across the same window.

Both are now pinned at 100%. The flake source was cross-test state
leakage in the conformance harness, addressed during the 100% push.
Keep an eye out — if either ever regresses below 100% on unrelated PRs,
that's the harness-isolation issue resurfacing, not a fakecloud bug.
