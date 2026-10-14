# Publishing to the network is opt-in (PUBLISH_TO_NETWORK)

Context: ADR 0004 made `publishing_to_network` a PROD-only stage, but every deployment that promotes to PROD still had to configure a Discovery Service (`DISCOVERY_SERVICE_ENDPOINT`, `NETWORK_SENDER_ID`) or its documents failed at that stage. Deployments that do not take part in the Beckn network, and the upstream docs-pipeline this project forked from, have no network publish at all.

Decision:
- A new setting `PUBLISH_TO_NETWORK` (default `false`) controls whether the stage publishes. Accepted truthy values are `true`, `1` and `yes`, matching `DISABLE_PROD_SETTING`.
- The setting is checked inside the `publish_catalog_to_network` activity, not in the workflow. Activities are not replayed, so changing the setting never changes the path a replaying workflow takes and needs no `workflow.patched` marker. A document in flight simply publishes, or skips, according to the value in force when its publish activity runs.
- When off, the activity does not construct `DiscoveryPublishService`, records a `network_publish_skipped` artifact with the reason `PUBLISH_TO_NETWORK is off`, and the stage completes. The Discovery Service settings are therefore required only when the setting is on.
- `GET /pipeline/stages` omits `publishing_to_network` when the setting is off, and the UI stepper follows that response, so operators do not see a step that does nothing. `PIPELINE_STAGES` and `PROD_ONLY_STAGES` are unchanged.
- Ordering, retry policy and the fatal-on-rejection behaviour from ADR 0003 and ADR 0004 are unchanged when the setting is on.

Alternatives considered:
- **Branch in the workflow on a value passed at start (like `skip_prod`).** Rejected: it needs the same plumbing at four call sites and a patch marker, for no benefit over an activity-level check.
- **Show the stage as "skipped" in the UI.** Rejected for now: the backend tracks one current stage per document and has no per-stage status, so a skipped state would need new storage. Hiding the stage reuses the existing `/pipeline/stages` mechanism.
- **Remove the publish stage altogether.** Rejected: the network publish remains the way the experience layer discovers knowledge kinds for deployments that opt in.
