# Conformance baseline notes

`conformance-baseline.json` is the exact conformance state CI checks. Every
service is pinned at its full observed pass count — currently
260,001 / 260,001 variants across all 107 services. CI fails any PR with a
failing variant and any PR whose results drift from the baseline in either
direction: a service below its pass count, a service above it, a changed
variant total, or a service missing from either side. A gain the baseline
does not record would leave every surface that quotes the counts stale, so
it is a failure too.

## Bumping baselines

When a service grows new variants (Smithy model update) or fakecloud
implementation closes a real gap, run:

```sh
cargo run -p fakecloud-conformance -- run --services <service>
```

If the pass count or the variant total moved and the change is real (not
gaming), update the per-service entry in `conformance-baseline.json` to the
new exact counts with `cargo run -p fakecloud-conformance --
update-baseline` (a hand edit must also update the top-level
`variants_passed` / `total_variants`, which `check` verifies are the
per-service sums), sync the surfaces `scripts/check-doc-counts.sh` checks,
and commit alongside the change that moved them. A new service adds its
baseline entry in the same PR.
`FAKECLOUD_CONFORMANCE_FLAKE_MARGIN` relaxes drops only (a service's passed count, and its variant total shrinking with it), never gains.

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
