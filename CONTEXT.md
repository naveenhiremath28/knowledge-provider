# Bharat Vistaar Docs Pipeline

A review-driven document ingestion pipeline: documents move through OCR, translation, and chunking review gates, then get indexed into a vector store (DEV, then optionally PROD) and announced to the wider partner network.

## Language

**Publish to Dev**:
The operator action that approves a document out of `ready_for_ingestion` and lets it enter DEV vector-index ingestion. Implemented as `Permission.APPROVE_INGESTION`.
_Avoid_: "publish" alone — ambiguous with Publish to Network.

**Publish to Network**:
The pipeline stage that runs only after a document's PROD promotion succeeds, and POSTs a Beckn-shaped `publish` envelope to an external Discovery Service. Skipped entirely when PROD is disabled (`DISABLE_PROD_SETTING=true`) or when `PUBLISH_TO_NETWORK` is off (the default) — a document never reaches the network without first promoting to PROD. The envelope it sends is kind-level (see Knowledge Kind), not document-level: it does not make any single document searchable, only re-announces that this provider serves that kind at all.
_Avoid_: "publish" alone — ambiguous with Publish to Dev.

**Scheme Catalog**:
The SQLite-backed, scheme-only registry that aggregates one or more documents sharing a `scheme_code`. Backs the pull-based `GET /catalog/v1/*` API that other BAPs poll.
_Avoid_: "catalog" alone — ambiguous with the Network Catalog Envelope.

**Network Catalog Envelope**:
The Beckn wire-protocol `message.catalogs` object sent by Publish to Network to the Discovery Service. Structurally and semantically unrelated to the Scheme Catalog tables — do not conflate the two.
_Avoid_: "catalog" alone.

**Knowledge Kind**:
What a document *is* to the network — `advisory`, `scheme`, `video`, or an operator-entered slug — held in `documents.document_kind`. Asserted by a reviewer during the pipeline, never inferred from the file, and absent until then (every document starts as the default `document`). Only `advisory` and `scheme` map to a Network Catalog Envelope; the rest publish nothing.
_Avoid_: "document type" — collides with `source_type`/`canonical_input_type`, which describe the input *format* (pdf, spreadsheet). Also avoid saying an advisory is "uploaded": what is uploaded is a file, which only becomes an advisory when someone classifies it.

**Document Validity**:
The period a document's chunks answer searches in, held as `documents.valid_from` / `documents.valid_to` and stamped onto every chunk's vector payload as `start_date` / `end_date`. Defaulted on upload to start that day with no end, meaning it never expires; a reviewer can narrow either end in the same form that sets the Knowledge Kind, behind the `VITE_DOCUMENT_VALIDITY_ENABLED` flag. Both ends are inclusive, and search filters on it so an expired or not-yet-started document is never answered from. Chunks ingested before it existed carry neither date and stay searchable — see `docs/ADR/0005-document-validity-filters-search.md`.
_Avoid_: "expiry" — there is a start as well as an end. Also avoid calling it the document's lifetime: the document stays in the console, editable and reingestable, once its period has passed; only its search answers stop.

**network_visible**:
An operator-controlled flag on a document/scheme (not a pipeline stage) that gates whether it's exposed to other BAPs through the pull-based Scheme Catalog snapshot. Independent of Publish to Network.
