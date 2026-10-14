# publishing_to_network is now a PROD-only stage

Context: `publishing_to_network` ran immediately after DEV ingest and unconditionally, per ADR 0003. That let a provider (re-)announce it serves a knowledge kind before any document of that kind had cleared PROD-level review — not a per-document gap (the catalog is kind-level, per ADR 0003) but a provider-level one: the claim "we can answer this kind of question" was backed only by DEV-quality content.

Decision:
- `publishing_to_network` moves to after `promote_document_to_prod_qdrant` succeeds and joins `PROD_ONLY_STAGES`; skipped entirely when `DISABLE_PROD_SETTING=true`.
- Applies uniformly across `DocumentPipelineWorkflow`, `ReingestionWorkflow`, and the standalone `PromoteToProdWorkflow` (which previously never published to network at all).
- A publish failure after a successful prod promotion is still fatal (document → `FAILED`), matching prior all-or-nothing behavior, even though PROD content is by then already live.
- In-flight Temporal executions are protected via `workflow.patched("publish-after-prod-v1")`, following the `auto-tag-v1`/`local-state-update-v1` convention. No backfill of documents already published under the old order.

Alternatives considered:
- **Leave it at DEV-ingest time (status quo).** Rejected: too weak a bar for a live network-facing capability claim.
- **Gate per-document instead of per-provider-capability.** Not possible: the envelope is structurally kind-level (ADR 0003).
- **Treat post-promotion publish failure as non-fatal.** Rejected for v1: avoids a new "prod-live but not announced" intermediate document state; revisit if failures become operationally frequent.

Update: publishing is now off unless `PUBLISH_TO_NETWORK` is set; see ADR 0005. The "skipped entirely when `DISABLE_PROD_SETTING=true`" rule above still holds.
