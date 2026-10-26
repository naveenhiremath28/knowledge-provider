# SQLite Schema — knowledge-provider (legacy)

> Legacy. The store is now Postgres, described in `docs/POSTGRES_SCHEMA.md`. This file documents the shape of the old SQLite file that `scripts/migrate_sqlite_to_postgres.py` reads.

## Overview

- One SQLite file is the canonical store for metadata, review state and editable content. MinIO holds blobs, Qdrant holds search projections, Temporal only orchestrates.
- 12 tables. `documents` is the root. `pages`, `chunks`, `document_jobs`, `document_artifacts`, `document_index_status` and `audit_logs` hang off it by `workflow_id`.
- Connection settings: WAL journal mode, `foreign_keys=ON`, `IMMEDIATE` transactions, 30s lock timeout. Writes are serialized by a process-local lock.
- Schema is created on startup. Changes are applied as idempotent `ALTER TABLE ... ADD COLUMN` calls, so the tables below show the final shape with migration-added columns merged in.
- Timestamps are ISO-8601 strings in TEXT. Booleans are INTEGER 0/1. Columns ending in `_json` hold JSON-encoded TEXT.
- `workflow_id` is the key used by the API, UI and Temporal.

## Drawbacks

### Integrity
- **No foreign keys.** Tables are linked by convention on `workflow_id`. Nothing stops orphan rows, and purging a document means deleting from each table by hand.
- **Pointer columns are unenforced.** `latest_job_id`, `original_artifact_id` and `normalized_artifact_id` on `documents` point into tables that point back, with no constraint.
- **No constraints on values.** Stages, statuses, job types, routing and flags are free TEXT or INTEGER. `created_at` and `updated_at` are nullable. Nothing checks that `valid_from <= valid_to` or that counts are non-negative.
- **Audit rows use sentinel strings** (`__system__`, `__settings__`, `__catalog__`) in key columns for non-document events, which a real foreign key could not hold.
- **Settings are untyped.** Every value is TEXT and cast on read. Nothing stops an out-of-range `alpha` or an invalid search method. Defaults disagree across three places (`alpha` is 0.7 in the model, 0.6 in the seed and reset).

### Data modeling
- **Same fact stored in several places.**
  - Uploader username, email and roles are copied onto each document and again into the audit log.
  - Scheme name, aliases and routing are copied onto every document of a scheme.
  - The scheme-to-document relationship is stored twice: a JSON list on the catalog entry and `scheme_code` on the document.
  - The file's MD5 is stored as `document_id`, `canonical_document_id` and `source_file_fingerprint`; `filename` equals `source_filename`.
- **`documents` is too wide.** About 45 columns mix file facts, processing state, uploader identity, PROD request, validity and scheme data.
- **JSON in TEXT.** Lists and objects (`scheme_aliases_json`, `workflow_ids_json`, `source_page_numbers_json`, `config_json`, `details_json`) cannot be validated or queried.
- **Run metadata repeated on every chunk.** Chunking provider, model, config, run ID and version are identical for all chunks of a run.
- **Translation columns sit on `pages`.** Eight mostly-NULL columns for pages that need no translation, and only one target language per page.
- **Roles are comma-separated strings**, with no array semantics.
- **Dead or constant columns.** `marqo_doc_id`, `temporal_run_id`, `content_hash`, `collection_name` and `tool_routing` carry no used data. The manifest table has no writer in the code.
- **Hardcoded language.** The manifest table has a Gujarati title column and the search settings default to a Gujarati query profile.
- **`entity_id` in the audit log is not an ID.** It holds a page or chunk number.
- **Catalog table is a cache** that is fully recomputed from documents and can drift.

## Tables

| # | Table | Purpose |
|---|-------|---------|
| 1 | `documents` | One row per ingested document. Root table, holds stage and metadata. |
| 2 | `pages` | OCR and translated page content plus review state. |
| 3 | `chunks` | Chunked text plus review state. |
| 4 | `audit_logs` | Per-document change and action history. |
| 5 | `settings` | Key/value app config, including search defaults. |
| 6 | `document_jobs` | Temporal job runs per document. |
| 7 | `document_artifacts` | Pointers to blobs stored in MinIO. |
| 8 | `document_index_status` | Vector index state per document and index. |
| 9 | `document_manifest_entries` | Imported manifest (CSV) metadata. |
| 10 | `scheme_catalog_meta` | Single-row catalog version counter. |
| 11 | `scheme_catalog_entries` | Scheme master catalog (served by `/catalog/v1/*`). |
| 12 | `email_otps` | Email OTP login codes. |

## 1. documents

Primary key: `workflow_id`

| Column | Type | Constraints / Default | Purpose |
|--------|------|----------------------|---------|
| workflow_id | TEXT | PRIMARY KEY | Temporal workflow ID. Unique key for the document and the join key used by every other table. |
| document_id | TEXT | NOT NULL | Document identifier carried into chunk payloads and index records. |
| canonical_document_id | TEXT | | Stable ID shared across re-uploads or re-ingestions of the same document. |
| filename | TEXT | NOT NULL | Name of the file as stored by the pipeline. |
| display_name | TEXT | | Human-friendly title shown in the UI. |
| source_filename | TEXT | | Original filename as uploaded by the user. |
| source_manifest_name | TEXT | | Filename of the matching entry in `document_manifest_entries`, used to link imported metadata. |
| source_file_fingerprint | TEXT | | Hash of the source file, used to detect duplicate uploads. |
| filepath | TEXT | NOT NULL | Local path of the uploaded file. |
| stage | TEXT | NOT NULL DEFAULT 'registered' | Current pipeline stage (registered, ocr_processing, ocr_review, translation_processing, translation_review, chunking, chunk_review, ready_for_ingestion, ingesting, approval_for_prod, ingesting_prod, completed, failed). |
| page_count | INTEGER | DEFAULT 0 | Number of pages extracted by OCR. |
| chunk_count | INTEGER | DEFAULT 0 | Number of chunks produced. |
| error_message | TEXT | | Failure reason when the stage is `failed`. |
| is_demo | INTEGER | DEFAULT 0 | Marks the document as demo content so it can be filtered out of summaries and index lists. |
| is_disabled | INTEGER | DEFAULT 0 | Soft-disable flag. A disabled document is hidden from lists and search. |
| created_at | TEXT | | When the row was registered. |
| updated_at | TEXT | | Last time the row changed. |
| ocr_completed_at | TEXT | | When OCR finished. |
| translation_completed_at | TEXT | | When translation finished. |
| chunks_completed_at | TEXT | | When chunking finished. |
| ingested_at | TEXT | | When the document was ingested into the vector index. |
| source_type | TEXT | | Input family derived from the file extension: `spreadsheet` (csv/xlsx), `document` (pdf, image, office) or `unknown`. |
| canonical_input_type | TEXT | | Form the input is normalized to: `spreadsheet` or `pdf`. Drives which pipeline path runs. |
| stop_after_ocr | INTEGER | DEFAULT 0 | When 1, the workflow halts after OCR (OCR-only batch runs). |
| reindex_required | INTEGER | DEFAULT 0 | Flags that approved content changed and the vector index is stale. |
| reindex_reason | TEXT | | Why a reindex is needed. |
| original_artifact_id | INTEGER | | Points to the `document_artifacts` row for the original upload. |
| normalized_artifact_id | INTEGER | | Points to the `document_artifacts` row for the normalized PDF or spreadsheet. |
| latest_job_id | INTEGER | | Points to the most recent `document_jobs` row. |
| instance | TEXT | | Tenant or state code (`bv` is platform-wide, others are state codes such as `mh`). Used for scoping and list filters. NULL or empty is backfilled with `DEFAULT_INSTANCE`. |
| uploaded_by_user_id | TEXT | | Keycloak user ID of the uploader. |
| uploaded_by_username | TEXT | | Username of the uploader. |
| uploaded_by_email | TEXT | | Email of the uploader. |
| uploaded_by_roles | TEXT | | Comma-separated roles of the uploader at upload time. |
| prod_ready_requested_at | TEXT | | When someone requested promotion to PROD. |
| prod_ready_requested_by_user_id | TEXT | | User ID of the PROD promotion requester. |
| prod_ready_requested_by_username | TEXT | | Username of the PROD promotion requester. |
| valid_from | TEXT | | Start of the period in which the document's chunks are searchable. Stamped with the upload day, editable by the approver. NULL means always current. |
| valid_to | TEXT | | End of the validity period. NULL means no end. |
| document_kind | TEXT | DEFAULT 'document' | Knowledge kind slug: `document`, `scheme`, `advisory`, `video` or a custom slug. Decides which Beckn catalog is announced. |
| scheme_code | TEXT | | Scheme identifier (for example `pmkisan`). Used when `document_kind` is `scheme`. Links to `scheme_catalog_entries`. |
| scheme_name | TEXT | | Human-readable scheme name. |
| scheme_aliases_json | TEXT | | JSON list of alternate names for the scheme, used for matching queries. |
| tool_routing | TEXT | | Which AI tool path serves the scheme: `qdrant`, `legacy` or `both`. |
| catalog_visible | INTEGER | DEFAULT 1 | Whether the document appears in the pull-based catalog API. |
| network_visible | INTEGER | DEFAULT 1 | Whether the document may be published to the Beckn network (Discovery Service). |

Indexes:

| Index | Columns | Purpose |
|-------|---------|---------|
| idx_documents_stage | stage | Filter and count documents by stage. |
| idx_documents_instance | instance | Tenant-scoped lists. |
| idx_documents_created | created_at DESC | Newest-first listing. |
| idx_documents_scheme_code | scheme_code | Look up documents by scheme. |

## 2. pages

Primary key: `id`. Unique: `(workflow_id, page_number)`.

| Column | Type | Constraints / Default | Purpose |
|--------|------|----------------------|---------|
| id | INTEGER | PRIMARY KEY AUTOINCREMENT | Row ID. |
| workflow_id | TEXT | NOT NULL | Owning document (`documents.workflow_id`). |
| page_number | INTEGER | NOT NULL | 1-based page position in the document. |
| original_markdown | TEXT | | Raw OCR output for the page, kept unchanged. |
| edited_markdown | TEXT | | Reviewer-corrected OCR text. Takes precedence over the original when present. |
| is_reviewed | INTEGER | DEFAULT 0 | Whether the OCR text for this page has been reviewed. |
| reviewer_notes | TEXT | | Reviewer comments on the OCR text. |
| detected_language | TEXT | | Language detected for the page (via the `lang-detect` service). |
| translated_markdown | TEXT | | Machine translation of the page. |
| edited_translation | TEXT | | Reviewer-corrected translation. |
| translation_reviewed | INTEGER | DEFAULT 0 | Whether the translation has been reviewed. |
| translation_notes | TEXT | | Reviewer comments on the translation. |
| translation_provider | TEXT | | Provider that produced the translation. |
| translation_model | TEXT | | Model used for the translation. |
| translation_target_language | TEXT | | Language the page was translated into. |
| translated_at | TEXT | | When the translation was produced. |

Indexes: `idx_pages_workflow (workflow_id)` — fetch all pages of a document.

## 3. chunks

Primary key: `id`. Unique: `(workflow_id, chunk_number)`.

| Column | Type | Constraints / Default | Purpose |
|--------|------|----------------------|---------|
| id | INTEGER | PRIMARY KEY AUTOINCREMENT | Row ID. |
| workflow_id | TEXT | NOT NULL | Owning document (`documents.workflow_id`). |
| chunk_number | INTEGER | NOT NULL | Order of the chunk within the document. |
| original_text | TEXT | | Chunk text as produced by the chunker. |
| edited_text | TEXT | | Reviewer-edited chunk text. This is what gets indexed when present. |
| token_count | INTEGER | DEFAULT 0 | Token length of the chunk. |
| page_start | INTEGER | DEFAULT 1 | First page the chunk covers. |
| page_end | INTEGER | DEFAULT 1 | Last page the chunk covers. |
| source_page_numbers_json | TEXT | | JSON list of every page the chunk draws from. |
| source_spans_json | TEXT | | JSON list of text spans in the source pages, used to trace a chunk back to its provenance. |
| section_title | TEXT | | Heading of the section the chunk belongs to. |
| content_type | TEXT | | Kind of content in the chunk, for example `heading` or `body`. |
| is_reference | INTEGER | DEFAULT 0 | Marks reference material. Search can exclude these (`search_exclude_reference`). |
| chunking_provider | TEXT | | Chunking provider that produced the chunk. |
| chunking_model | TEXT | | Model used for chunking, if any. |
| chunking_config_json | TEXT | | JSON snapshot of the chunking settings used. |
| chunking_run_id | TEXT | | ID of the chunking run, to group chunks created together. |
| chunk_version | INTEGER | DEFAULT 1 | Version counter for the chunk. |
| is_reviewed | INTEGER | DEFAULT 0 | Whether the chunk has been reviewed. |
| is_excluded | INTEGER | DEFAULT 0 | Reviewer excluded the chunk. Excluded chunks are not indexed. |
| reviewer_notes | TEXT | | Reviewer comments on the chunk. |

Indexes: `idx_chunks_workflow (workflow_id)` — fetch all chunks of a document.

## 4. audit_logs

Primary key: `id`.

| Column | Type | Constraints / Default | Purpose |
|--------|------|----------------------|---------|
| id | INTEGER | PRIMARY KEY AUTOINCREMENT | Row ID. |
| workflow_id | TEXT | NOT NULL | Document the action applied to. |
| document_id | TEXT | NOT NULL | Document identifier at the time of the action. |
| action_type | TEXT | NOT NULL | What happened, for example `promote_to_prod`. |
| entity_type | TEXT | | Kind of record changed, for example `document`, page or chunk. |
| entity_id | INTEGER | | ID or number of the record changed. |
| field_name | TEXT | | Field that was changed. |
| old_value | TEXT | | Value before the change. |
| new_value | TEXT | | Value after the change. |
| metadata | TEXT | | Extra context, stored as JSON text. |
| timestamp | TEXT | NOT NULL | When the action happened. |
| actor_user_id | TEXT | | Keycloak user ID of whoever acted. |
| actor_username | TEXT | | Username of the actor. |
| actor_email | TEXT | | Email of the actor. |
| actor_roles | TEXT | | Roles of the actor at the time. |

Indexes:

| Index | Columns | Purpose |
|-------|---------|---------|
| idx_audit_workflow | workflow_id | Per-document audit trail. |
| idx_audit_timestamp | timestamp DESC | Newest-first global audit log. |

## 5. settings

Primary key: `key`.

| Column | Type | Constraints / Default | Purpose |
|--------|------|----------------------|---------|
| key | TEXT | PRIMARY KEY | Setting name. |
| value | TEXT | NOT NULL | Setting value, always stored as text. |
| description | TEXT | | What the setting does. |
| updated_at | TEXT | NOT NULL | When the setting last changed. |

Default rows (`INSERT OR IGNORE`):

| key | default value | Purpose |
|-----|---------------|---------|
| search_method | HYBRID | Search mode: TENSOR, LEXICAL or HYBRID. |
| search_limit | 12 | Number of search results to return. |
| search_alpha | 0.6 | Hybrid weighting: 0 is lexical only, 1 is semantic only. |
| search_ranking_method | rrf | Hybrid ranking: `rrf` or `normalize_linear`. |
| search_show_highlights | true | Show highlighted matches in results. |
| search_ef_search | 256 | HNSW search accuracy parameter. |
| search_index_name | documents-index | Default vector DB collection name. |
| search_candidate_cap | 120 | Cap on the candidate retrieval pool. |
| search_candidate_multiplier | 10 | Candidate pool multiplier before the final cut. |
| search_max_chunks_per_doc | 2 | Result diversity cap per document. |
| search_use_e5_prefix | true | Prefix queries with `query:` for e5 embedding models. |
| search_exclude_reference | true | Exclude reference chunks when the index supports it. |
| search_query_expansion_profile | gu-v1 | Query expansion profile. |
| search_rerank_mode | none | Post-search reranking mode. |
| search_hybrid_rrfk | 60 | RRF tuning parameter for hybrid search. |

## 6. document_jobs

Primary key: `id`.

| Column | Type | Constraints / Default | Purpose |
|--------|------|----------------------|---------|
| id | INTEGER | PRIMARY KEY AUTOINCREMENT | Job ID. Referenced by `documents.latest_job_id` and `document_artifacts.job_id`. |
| workflow_id | TEXT | NOT NULL | Document the job ran for. |
| job_type | TEXT | NOT NULL | Kind of run: `ocr_only`, `ocr_retry`, `translation_retry`, `chunking_retry`, `reingest`, `promote_prod`, plus the main pipeline run. |
| temporal_workflow_id | TEXT | | Temporal workflow ID, to find the run in the Temporal UI. |
| temporal_run_id | TEXT | | Temporal run ID. |
| status | TEXT | NOT NULL DEFAULT 'running' | Job state. |
| current_stage | TEXT | | Stage the job is in right now. |
| started_at | TEXT | NOT NULL | When the job started. |
| completed_at | TEXT | | When the job ended. |
| error_message | TEXT | | Failure reason. |
| config_json | TEXT | | JSON snapshot of the settings the job ran with. |

Indexes: `idx_document_jobs_workflow (workflow_id, started_at DESC)` — latest jobs for a document.

## 7. document_artifacts

Primary key: `id`.

| Column | Type | Constraints / Default | Purpose |
|--------|------|----------------------|---------|
| id | INTEGER | PRIMARY KEY AUTOINCREMENT | Artifact ID. Referenced by `documents.original_artifact_id` and `normalized_artifact_id`. |
| workflow_id | TEXT | NOT NULL | Owning document. |
| job_id | INTEGER | | Job that produced the artifact (`document_jobs.id`). |
| artifact_type | TEXT | NOT NULL | Kind of output, for example `original_upload`, `normalized_pdf`, `normalized_spreadsheet`, `ocr_pages_json`, `translation_pages_json`, `chunk_json_export`, `vector_payload_export`. |
| stage | TEXT | | Pipeline stage that created it. |
| storage_uri | TEXT | NOT NULL | MinIO location of the blob. |
| mime_type | TEXT | | MIME type of the blob. |
| filename | TEXT | | File name of the blob. |
| size_bytes | INTEGER | | Blob size in bytes. |
| metadata_json | TEXT | | Extra details as JSON, for example the source path or input type. |
| created_at | TEXT | NOT NULL | When the artifact was recorded. |

Indexes: `idx_document_artifacts_workflow (workflow_id, created_at DESC)` — artifacts of a document, newest first.

## 8. document_index_status

Primary key: `(workflow_id, index_name)`.

| Column | Type | Constraints / Default | Purpose |
|--------|------|----------------------|---------|
| workflow_id | TEXT | NOT NULL | Document that was indexed. |
| index_name | TEXT | NOT NULL | Vector index or collection (for example DEV or PROD). One row per document and index. |
| doc_id | TEXT | | Document ID as stored in the vector index. Replaces legacy `marqo_doc_id`. |
| chunk_count_indexed | INTEGER | DEFAULT 0 | Number of chunks currently in the index. |
| last_indexed_at | TEXT | | When the document was last written to the index. |
| last_verified_at | TEXT | | When the index state was last checked against the vector store. |
| schema_version | TEXT | | Index payload schema version, for spotting stale payloads. |
| status | TEXT | NOT NULL DEFAULT 'unknown' | Index state, for example `indexed` or `unknown`. |
| details_json | TEXT | | Extra details as JSON. |

Older DBs may still carry `marqo_doc_id` until a cleanup script is run. Startup backfills `doc_id` from it when present.

## 9. document_manifest_entries

Primary key: `id`. Unique: `manifest_filename`.

| Column | Type | Constraints / Default | Purpose |
|--------|------|----------------------|---------|
| id | INTEGER | PRIMARY KEY AUTOINCREMENT | Row ID. |
| manifest_filename | TEXT | NOT NULL UNIQUE | Filename the entry describes. Matches `documents.source_manifest_name`. |
| title_en | TEXT | | English title. |
| title_gu | TEXT | | Gujarati title. |
| doc_language | TEXT | | Language of the document. |
| category_tags | TEXT | | Category tags for the document. |
| description | TEXT | | Short description. |
| quality_score | TEXT | | Quality rating from the manifest. |
| priority_rank | TEXT | | Ingestion priority from the manifest. |
| ingestion_status | TEXT | | Ingestion status recorded in the manifest. |
| feedback | TEXT | | Reviewer feedback from the manifest. |
| source_csv_path | TEXT | | CSV file the row was imported from. |
| imported_at | TEXT | NOT NULL | When the row was imported. |

Indexes: `idx_manifest_entries_filename (manifest_filename)` — look up by filename.

## 10. scheme_catalog_meta

Single-row table, enforced by `CHECK (id = 1)`.

| Column | Type | Constraints / Default | Purpose |
|--------|------|----------------------|---------|
| id | INTEGER | PRIMARY KEY, CHECK (id = 1) | Fixed to 1 so only one row can exist. |
| version | INTEGER | NOT NULL DEFAULT 0 | Catalog version counter. Bumped on catalog changes so pull clients can detect updates. |
| updated_at | TEXT | | When the version last changed. |
| notes | TEXT | | Reason for the last bump. |

## 11. scheme_catalog_entries

Primary key: `scheme_code`.

| Column | Type | Constraints / Default | Purpose |
|--------|------|----------------------|---------|
| scheme_code | TEXT | PRIMARY KEY | Scheme identifier (for example `pmkisan`). |
| scheme_name | TEXT | | Human-readable scheme name. |
| scheme_aliases_json | TEXT | | JSON list of alternate names for matching queries. |
| instances_json | TEXT | | JSON list of instances (tenants) that have this scheme. |
| workflow_ids_json | TEXT | | JSON list of `documents.workflow_id` values that back the scheme. |
| collection_name | TEXT | | Vector collection holding the scheme's chunks. |
| chunk_count | INTEGER | DEFAULT 0 | Number of chunks across the scheme's documents. |
| status | TEXT | NOT NULL DEFAULT 'disabled' | Catalog state: `live`, `pending_prod`, `pending_reindex` or `disabled`. Only live schemes are served as vector schemes. |
| network_visible | INTEGER | DEFAULT 0 | Whether the scheme is exposed on the network. |
| promoted_at | TEXT | | When the scheme was promoted to PROD. |
| content_hash | TEXT | | Hash of code, name, aliases, visibility and status, used to detect changes. |
| source | TEXT | | Where the entry came from: `pipeline` (built from scheme documents) or `import` (bootstrapped). |
| updated_at | TEXT | | When the entry last changed. |

Indexes: `idx_scheme_catalog_status (status)` — filter entries by status.

## 12. email_otps

Primary key: `email`. One live code per email, overwritten on resend.

| Column | Type | Constraints / Default | Purpose |
|--------|------|----------------------|---------|
| email | TEXT | PRIMARY KEY | Email the code was sent to. |
| code_hash | TEXT | NOT NULL | Hash of the one-time code. The plain code is never stored. |
| expires_at | TEXT | NOT NULL | Expiry time of the code. |
| attempts | INTEGER | NOT NULL DEFAULT 0 | Failed verification attempts. Reset to 0 on resend. |
| created_at | TEXT | NOT NULL | When the code was first issued. |
| last_sent_at | TEXT | NOT NULL | When the code was last sent. Used to throttle resends. |

## Relationships (by convention, not enforced)

| From | Column | To |
|------|--------|----|
| pages | workflow_id | documents.workflow_id |
| chunks | workflow_id | documents.workflow_id |
| audit_logs | workflow_id | documents.workflow_id |
| document_jobs | workflow_id | documents.workflow_id |
| document_artifacts | workflow_id | documents.workflow_id |
| document_artifacts | job_id | document_jobs.id |
| document_index_status | workflow_id | documents.workflow_id |
| documents | original_artifact_id, normalized_artifact_id | document_artifacts.id |
| documents | latest_job_id | document_jobs.id |
| documents | source_manifest_name | document_manifest_entries.manifest_filename |
| documents | scheme_code | scheme_catalog_entries.scheme_code |
| scheme_catalog_entries | workflow_ids_json (JSON list) | documents.workflow_id |
