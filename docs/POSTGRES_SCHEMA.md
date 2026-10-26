# Postgres Schema — knowledge-provider

Postgres schema behind `pipeline/db.py`. A document is content and metadata. A workflow is one run of processing against a document. Source file and vector index state are stored as JSONB on the document. Translations are stored as JSONB on each page.

The schema is created by `migrations/0001_initial_schema.sql` and applied by the api and worker at startup (`pipeline/migrate.py`). `pipeline/db.py` is the only code that reads or writes it. The previous SQLite store is described in `docs/SQLITE_SCHEMA.md` (legacy) and is moved over once by `scripts/migrate_sqlite_to_postgres.py`.

## Conventions

- Every table has a primary key `id`, a UUID v4 (`DEFAULT gen_random_uuid()`, built in from Postgres 13).
- A foreign key is named `<referenced table, singular>_id` (for example `document_id`). A role prefix is added when the role matters (`uploaded_by_user_id`, `started_by_user_id`).
- Text identifiers other systems rely on (`catalog_entries.code`, `temporal_workflow_id`) are ordinary columns, not keys.
- `created_at` and `updated_at` are the last two columns of a table. `updated_at` is maintained by one shared trigger. Append-only or short-lived tables skip `updated_at`, as noted per table.
- Timestamps are `TIMESTAMPTZ` (UTC), booleans are `BOOLEAN`, JSON is `JSONB`, lists of strings are `TEXT[]`.
- JSONB columns have a `CHECK` that the value is an object (or array). Value rules inside them live in Pydantic models, not in the database.
- Fixed value sets are enums. Open sets (`artifact_type`, `action_type`) stay `TEXT`.
- Counts have `CHECK >= 0`. Slugs and identifiers have pattern `CHECK`s.

### Shared trigger

```sql
CREATE FUNCTION set_updated_at() RETURNS trigger AS $$
BEGIN
  NEW.updated_at := now();
  RETURN NEW;
END $$ LANGUAGE plpgsql;
-- one per table that has updated_at:
-- CREATE TRIGGER trg_<table>_updated_at BEFORE UPDATE ON <table>
--   FOR EACH ROW EXECUTE FUNCTION set_updated_at();
```

### Enums

| Enum | Values |
|---|---|
| `workflow_stage` | `registered`, `ocr_processing`, `ocr_review`, `translation_processing`, `translation_review`, `chunking`, `chunk_review`, `ready_for_ingestion`, `ingesting`, `publishing_to_network`, `approval_for_prod`, `ingesting_prod`, `completed`, `failed` |
| `workflow_type_enum` | `pipeline`, `ocr_only`, `ocr_retry`, `translation_retry`, `chunking_retry`, `reingest`, `promote_prod` |
| `workflow_status_enum` | `running`, `waiting_review`, `completed`, `failed` |
| `audit_entity_type` | `document`, `page`, `chunk`, `setting`, `catalog` |

Adding a value later needs `ALTER TYPE ... ADD VALUE`.

## Tables

| # | Table | Purpose |
|---|---|---|
| 1 | `users` | People who upload, review and act, mirrored from Keycloak |
| 2 | `catalog_entries` | Generic named entries that documents belong to. Type `scheme` today, `advisory` and others later |
| 3 | `documents` | One row per document. Content, metadata, source file, index state |
| 4 | `workflows` | One row per processing run of a document. Stage, status, config |
| 5 | `workflow_artifacts` | Files each workflow produces, stored in MinIO |
| 6 | `pages` | OCR layer and translations of each page |
| 7 | `chunks` | Chunked text and review state |
| 8 | `audit_logs` | Append-only action history |
| 9 | `search_settings` | Single-row search configuration |
| 10 | `email_otps` | Email OTP login codes |
| 11 | `webhook_records` | Records pushed to `POST /webhook` |
| — | `schema_migrations` | Applied migration files (created by the runner, not by a migration) |
| — | `catalog` (view) | Computed catalog of all entries. `/catalog/v1/*` serves the `scheme` type |
| — | `document_current_workflow` (view) | Latest workflow of each document |

Relationships:

```
catalog_entries 1 ── * documents 1 ── * workflows 1 ── * workflow_artifacts
                          │
                          ├── * pages
                          └── * chunks
```

Dependency order for creation: `users`, `catalog_entries`, `documents`, `workflows`, `workflow_artifacts`, `pages`, `chunks`, `audit_logs`, `search_settings`, `email_otps`, `webhook_records`, views.

---

## 1. `users`

Uploader, reviewer and actor identity, looked up once from the Keycloak token.

Unique: `keycloak_user_id`; unique index on `lower(email)` where `email IS NOT NULL`.

| Column | Type | Constraints / Default | Purpose |
|---|---|---|---|
| id | UUID | PRIMARY KEY, DEFAULT gen_random_uuid() | Internal user ID. |
| keycloak_user_id | TEXT | NOT NULL UNIQUE | Keycloak `sub` claim. `local-admin` when auth is disabled (not a UUID, so it cannot be the PK). |
| username | TEXT | | Current username. |
| email | TEXT | | Current email. |
| roles | TEXT[] | NOT NULL DEFAULT '{}' | Current roles. |
| first_seen_at | TIMESTAMPTZ | NOT NULL DEFAULT now() | First login or upload. |
| last_seen_at | TIMESTAMPTZ | NOT NULL DEFAULT now() | Most recent activity. |

Notes
- Upserted on each authenticated upload or login. A `local-admin` row is seeded for `AUTH_DISABLED=true`.
- Point-in-time identity is kept in `audit_logs.actor`.

---

## 2. `catalog_entries`

Generic named entries that documents belong to. One table for every entry type: `scheme` today, `advisory` and others later. A new type adds rows, not tables.

Unique: `(type, code)` and `(id, type)` (the second is the target of the composite FK from `documents`).

| Column | Type | Constraints / Default | Purpose |
|---|---|---|---|
| id | UUID | PRIMARY KEY, DEFAULT gen_random_uuid() | Entry ID. |
| type | TEXT | NOT NULL, CHECK `^[a-z0-9]+([_-][a-z0-9]+)*$` | Entry type (`scheme`, later `advisory`, and so on). Open set, so not an enum. |
| code | TEXT | NOT NULL, CHECK `^[a-z0-9]+(-[a-z0-9]+)*$` | Short code (`pm-kmy`) that catalog consumers see. Unique within a type. |
| name | TEXT | NOT NULL | Display name, shared by all documents of the entry. |
| aliases | TEXT[] | NOT NULL DEFAULT '{}', CHECK no empty string | Ordered alternate names used to match queries. The first one is the prompt hint. |
| metadata | JSONB | NOT NULL DEFAULT '{"source": "pipeline"}', CHECK object | `source` and any type-specific fields. See keys below. |
| created_at | TIMESTAMPTZ | NOT NULL DEFAULT now() | Created. |
| updated_at | TIMESTAMPTZ | NOT NULL DEFAULT now() | Last change (trigger). |

### `metadata` keys

| Key | Type | Default | Purpose |
|---|---|---|---|
| source | string | `pipeline` | `bootstrap` (seeded from code), `pipeline` (built from documents) or `import`. |

Type-specific fields are added as keys, with no migration. Pydantic validates the keys per type.

Notes
- Computed catalog values (`status`, `instances`, `workflow_ids`, `chunk_count`, `network_visible`, `promoted_at`) come from the `catalog` view, not stored here.
- Case-insensitive alias uniqueness is enforced in the app (`_dedupe_aliases`). Add a GIN index on `aliases` only if "find entry by alias" is needed in SQL.
- No join table: an entry has many documents and a document has one entry, which `documents.catalog_entry_id` expresses.
- Plain documents (kind `document`) have no entry.

---

## 3. `documents`

One row per document. 13 columns.

| Column | Type | Constraints / Default | Purpose |
|---|---|---|---|
| id | UUID | PRIMARY KEY, DEFAULT gen_random_uuid() | Document ID. |
| instance | TEXT | NOT NULL | Tenant or state code. |
| source | JSONB | NOT NULL, CHECK `jsonb_typeof(source) = 'object'`, CHECK `source->>'fingerprint' ~ '^[0-9a-f]{32}$'` | The uploaded file. See shape below. |
| document_kind | TEXT | NOT NULL DEFAULT 'document', CHECK `^[a-z0-9]+([_-][a-z0-9]+)*$` | Knowledge kind (`document`, `scheme`, `advisory`, `video`, or custom). |
| catalog_entry_id | UUID | | Catalog entry (for example a scheme) the document belongs to. Nullable. Composite FK, see below. |
| valid_from | DATE | | Start of the period chunks are searchable. |
| valid_to | DATE | CHECK `valid_from <= valid_to` | End of the period. NULL means no end. |
| is_disabled | BOOLEAN | NOT NULL DEFAULT FALSE | Soft delete. Restore flips it back. |
| index_status | JSONB | NOT NULL DEFAULT '{}', CHECK `jsonb_typeof(index_status) = 'object'` | Vector index state per index. Keys `dev` and `prod`. |
| metadata | JSONB | NOT NULL, DEFAULT see below, CHECK object, CHECK `(metadata->>'chunk_count')::int >= 0` | Display and flag fields. See keys below. |
| uploaded_by_user_id | UUID | FK → `users(id)` | Who uploaded. |
| created_at | TIMESTAMPTZ | NOT NULL DEFAULT now() | Row created. |
| updated_at | TIMESTAMPTZ | NOT NULL DEFAULT now() | Last change (trigger). |

Composite FK `(catalog_entry_id, document_kind)` → `catalog_entries(id, type)`, `ON UPDATE CASCADE ON DELETE RESTRICT`. It guarantees a document's kind equals its entry's type. When `catalog_entry_id` is NULL the FK is not checked, so plain documents need no entry.

Suggested indexes:
- `(instance)`
- `(created_at DESC)`
- `(catalog_entry_id) WHERE catalog_entry_id IS NOT NULL`
- `(instance, (source->>'fingerprint'))` for duplicate checks and identifier lookups
- `(((metadata->>'is_demo')::boolean))` for the demo filter on every list

### `source`

Facts about the uploaded file, not about processing.

```json
{
  "fingerprint": "<md5 of file contents>",
  "original_filename": "pmkisan.pdf",
  "storage_uri": "s3://...",
  "source_type": "document",
  "canonical_input_type": "pdf",
  "page_count": null
}
```

| Key | Type | Notes |
|---|---|---|
| fingerprint | string | MD5, 32 lowercase hex. Enforced by CHECK. Also the key the document has inside the vector index. |
| original_filename | string | Name as uploaded. |
| storage_uri | string | Location of the uploaded file. |
| source_type | string | `spreadsheet`, `document` or `unknown`. |
| canonical_input_type | string | `spreadsheet` or `pdf`. Normalized pipeline path. |
| page_count | integer or null | NULL until OCR runs. |

### `index_status`

One entry per vector index. DEV and PROD are separate Qdrant collections.

```json
{
  "dev":  {"status": "indexed", "chunk_count_indexed": 42,
           "last_indexed_at": "2026-10-19T06:20:00Z", "last_verified_at": "2026-10-19T06:25:00Z",
           "schema_version": "passage-v1", "details": {}},
  "prod": {"...": "..."}
}
```

- A missing key means the document was never ingested to that index.
- `status` is `unknown`, `indexed` or `missing`.
- The document's key inside the vector index is `source.fingerprint`.
- The key is `prod` for the PROD collection names (`PROD_VECTOR_DB_COLLECTION_NAME`, `PROD_SCHEME_VECTOR_DB_COLLECTION_NAME`) and `dev` for every other collection. The collection name is kept inside the entry as `index_name`, next to `doc_id`, the id the index uses for the document.
- Update one index atomically: `SET index_status = index_status || jsonb_build_object('dev', $1::jsonb)`.
- A third index is a new key, with no migration.

### `metadata`

| Key | Type | Default | Purpose |
|---|---|---|---|
| display_name | string | absent | Human-friendly title. |
| catalog_visible | boolean | true | Counts toward the catalog. |
| network_visible | boolean | true | May be published to the Beckn network. |
| is_demo | boolean | false | Demo content, hidden by default. |
| chunk_count | integer ≥ 0 | 0 | Cached number of chunks. Updated in the same transaction as chunk writes. |
| reindex_required | boolean | false | Approved content changed and the vector index is stale. |
| reindex_reason | string | absent | Why a reindex is needed. |
| chunking | object `{run_number, provider, model, config, created_at}` | absent | Settings of the run that produced the current chunks. |

Column default: `'{"catalog_visible": true, "network_visible": true, "is_demo": false, "chunk_count": 0, "reindex_required": false}'`.

### Notes

- A document's stage is the `current_stage` of its latest workflow. List and filter queries join the `document_current_workflow` view.
- SQL does not check types inside the JSONB columns. The insert path writes every default `metadata` key, and Pydantic validates `source`, `index_status` and `metadata`.
- `metadata.chunk_count` is rewritten in every chunk transaction and `metadata.is_demo` is filtered on every list. If performance suffers, move these two back to columns first.
- Each document keeps its own `source`. Two uploads of the same content from different paths keep their own `storage_uri`. There is no unique key on `(instance, fingerprint)`; the upload path reuses a document by its Temporal workflow id instead. Add `UNIQUE (instance, (source->>'fingerprint'))` only if one document per file per tenant must be enforced.
- Identifier lookup (`find_document_by_doc_identifier`) matches `documents.id`, `workflows.temporal_workflow_id`, `source->>'fingerprint'` and `source->>'original_filename'`. The legacy `md5(identifier)` check is `md5(source->>'fingerprint') = $1` in SQL.
- No `deleted_at`: soft delete is `is_disabled`, hard delete is `purge_document`.

---

## 4. `workflows`

One row per processing run of a document: the main pipeline run, retries, reingest and PROD promotion. Holds all processing state.

Indexes: `(document_id, started_at DESC)`, `(status, started_at DESC)`, `(temporal_workflow_id)`.

| Column | Type | Constraints / Default | Purpose |
|---|---|---|---|
| id | UUID | PRIMARY KEY, DEFAULT gen_random_uuid() | Workflow ID. |
| document_id | UUID | NOT NULL, FK → `documents(id)` ON DELETE CASCADE | Document the workflow runs against. |
| workflow_type | workflow_type_enum | NOT NULL | Kind of run. |
| temporal_workflow_id | TEXT | NOT NULL | Temporal workflow ID. `doc-<md5>` for the main run, suffixed for retries. Not unique (a restart reuses the ID). Used by the API, UI and Temporal. |
| status | workflow_status_enum | NOT NULL DEFAULT 'running' | `running`, `waiting_review`, `completed` or `failed`. |
| current_stage | workflow_stage | NOT NULL DEFAULT 'registered' | Stage the workflow is in now. This is the document's stage when the workflow is the latest. |
| stage_timestamps | JSONB | NOT NULL DEFAULT '{}', CHECK object with only keys `ocr`, `translation`, `chunks` | When each stage first completed in this run, ISO-8601 UTC. Set-once per key. |
| stop_after_ocr | BOOLEAN | NOT NULL DEFAULT FALSE | Halt after OCR (OCR-only batch runs). |
| error_message | TEXT | | Failure reason when status is `failed`. |
| config | JSONB | NOT NULL DEFAULT '{}', CHECK object | Settings the run used, plus `chunking_progress` while chunking. |
| started_by_user_id | UUID | FK → `users(id)` ON DELETE SET NULL | Who started it. For `promote_prod`, the person who requested promotion. NULL for system-started runs. |
| started_at | TIMESTAMPTZ | NOT NULL DEFAULT now() | When the workflow started. Plays the role of `created_at`. |
| completed_at | TIMESTAMPTZ | | When it completed or failed. |
| updated_at | TIMESTAMPTZ | NOT NULL DEFAULT now() | Last change (trigger). Progress updates touch the row often. |

13 columns.

Notes
- `stage_timestamps` is set-once: `stage_timestamps = jsonb_build_object('ocr', $1) || stage_timestamps` (right side wins, so existing keys are kept).
- Progress updates are atomic: `UPDATE ... SET config = config || jsonb_build_object('chunking_progress', ...)`.
- A PROD request inserts a `promote_prod` workflow in `waiting_review`, with `started_by_user_id` set to the requester and `started_at` to the request time. Approval moves it to `running`; when approval starts a `PromoteToProdWorkflow`, that run takes over the waiting row instead of adding a second one.
- The current workflow of a document is the latest by `started_at`. A retry started while an older run is still `running` takes over the stage display, and so does a retry on a `completed` document: its stage goes back to the retry's stage until the retry finishes.
- No CHECK ties `completed_at` to status: `reconcile_materialized_state` sets `waiting_review` without clearing `completed_at`.
- `cancelled` is not a status because nothing writes it.
- Do not copy the uploader snapshot into `config`; `started_by_user_id` and `audit_logs` cover it.

### View: `document_current_workflow`

Latest workflow per document. Stores nothing.

```sql
SELECT DISTINCT ON (document_id) *
FROM workflows
ORDER BY document_id, started_at DESC;
```

---

## 5. `workflow_artifacts`

Ledger of every file a workflow produces or stores. Files live in MinIO; this table records where and what. Append-only.

Indexes: `(workflow_id, created_at DESC)`, `(workflow_id, artifact_type, created_at DESC)`.

| Column | Type | Constraints / Default | Purpose |
|---|---|---|---|
| id | UUID | PRIMARY KEY, DEFAULT gen_random_uuid() | Artifact ID, used in `/documents/{id}/artifacts/{artifact_id}`. |
| workflow_id | UUID | NOT NULL, FK → `workflows(id)` ON DELETE CASCADE | Workflow that produced it. |
| artifact_type | TEXT | NOT NULL, CHECK not blank | Kind of output (`original_upload`, `normalized_pdf`, `normalized_spreadsheet`, `ocr_pages_json`, `translation_pages_json`, `chunk_json_export`, `vector_payload_export`, network publish exchange). Open set, so not an enum. |
| stage | workflow_stage | | Stage that created it. |
| storage_uri | TEXT | NOT NULL | MinIO location of the blob. |
| mime_type | TEXT | | MIME type. |
| filename | TEXT | | File name of the blob. |
| size_bytes | BIGINT | CHECK ≥ 0 | Blob size. |
| metadata | JSONB | NOT NULL DEFAULT '{}', CHECK object | Chunking config for chunk exports, request and response for network publishes, source path for uploads. |
| created_at | TIMESTAMPTZ | NOT NULL DEFAULT now() | When the artifact was recorded. |

10 columns.

Notes
- Every artifact belongs to a workflow. The upload path creates the first `pipeline` workflow before recording `original_upload`.
- The document is reached through `workflows.document_id`. Per-document listing: `SELECT a.* FROM workflow_artifacts a JOIN workflows w ON w.id = a.workflow_id WHERE w.document_id = $1 ORDER BY a.created_at DESC`.
- The latest original or normalized artifact uses the same join with `artifact_type = 'original_upload'` (or `normalized_%`).
- Deleting a workflow deletes its artifact rows. Blobs in MinIO are not removed by the database; cleanup stays in `purge_document`.
- No `updated_at`: rows are append-only.
- Chunking run history lives in `chunk_json_export` metadata.

---

## 6. `pages`

OCR layer and translations of a page.

Unique: `(document_id, page_number)`.

| Column | Type | Constraints / Default | Purpose |
|---|---|---|---|
| id | UUID | PRIMARY KEY, DEFAULT gen_random_uuid() | Page ID. |
| document_id | UUID | NOT NULL, FK → `documents(id)` ON DELETE CASCADE | Owning document. |
| page_number | INTEGER | NOT NULL, CHECK ≥ 1 | 1-based page position. |
| original_markdown | TEXT | | Raw OCR output, kept unchanged. |
| edited_markdown | TEXT | | Reviewer-corrected OCR text. Used instead of the original when present. |
| translations | JSONB | NOT NULL DEFAULT '{}', CHECK `jsonb_typeof(translations) = 'object'` | Translations of this page, keyed by target language. See shape below. |
| metadata | JSONB | NOT NULL DEFAULT '{"is_reviewed": false}', CHECK object | OCR review and language fields. See keys below. |
| created_at | TIMESTAMPTZ | NOT NULL DEFAULT now() | When the page row was first created. |
| updated_at | TIMESTAMPTZ | NOT NULL DEFAULT now() | Last change (trigger). |

### `metadata` keys

| Key | Type | Default | Purpose |
|---|---|---|---|
| is_reviewed | boolean | false | OCR review done. |
| reviewer_notes | string | absent | Reviewer comments on the OCR text. |
| detected_language | string | absent | Language code detected. Non-`en` means the page needs translation. |

SQL does not check these types; Pydantic does. The review stage gate counts reviewed pages with `(metadata->>'is_reviewed')::boolean`, and the translation step finds pages to translate with `metadata->>'detected_language'`. Add expression indexes on both if those queries are slow: `((metadata->>'is_reviewed')::boolean)` and `((metadata->>'detected_language'))`.

### `translations`

```json
{
  "gu": {
    "translated_markdown": "...",
    "edited_translation": null,
    "is_reviewed": false,
    "reviewer_notes": null,
    "provider": "azure",
    "model": "gpt-5.4-mini",
    "translated_at": "2026-10-19T06:14:00Z"
  }
}
```

- Key is the target language. Several languages per page are possible.
- Pages that need no translation (English) keep `'{}'`. An entry exists only for pages actually translated.
- `edited_translation` takes precedence over `translated_markdown` when present.
- Writes target one path, for example `SET translations = jsonb_set(translations, '{gu,edited_translation}', to_jsonb($1::text))`.
- Re-running OCR on a page resets its `translations` to `'{}'`, since the page text changed.
- Counting translated pages is `WHERE translations <> '{}'`. Finding unreviewed translations needs `jsonb_each` over the small per-page value.

Notes
- `save_pages` replaces the whole row, so it must pass `translations` through unchanged unless the translation activity is writing it. Otherwise an OCR save would wipe translations.
- `save_pages` is `INSERT ... ON CONFLICT (document_id, page_number) DO UPDATE SET <all columns>`. It is called during OCR segments, after OCR, after translation and at persist time, and each call replaces the whole row, so the caller passes every field.
- `save_pages` also passes `metadata` through whole. Update single keys with `metadata = metadata || jsonb_build_object('is_reviewed', true)` instead of rewriting the object.
- Page edits are audited in `audit_logs` (`page_edit`, `page_reset`), so no edit-history columns.
- Pages belong to a document, not to the source file: edits, review flags and OCR retries are per document.

---

## 7. `chunks`

Unique: `(document_id, chunk_number)`.

| Column | Type | Constraints / Default | Purpose |
|---|---|---|---|
| id | UUID | PRIMARY KEY, DEFAULT gen_random_uuid() | Chunk ID. |
| document_id | UUID | NOT NULL, FK → `documents(id)` ON DELETE CASCADE | Owning document. |
| chunk_number | INTEGER | NOT NULL, CHECK ≥ 1 | Order within the document. |
| original_text | TEXT | NOT NULL | Chunk text as produced by the chunker. |
| edited_text | TEXT | | Reviewer-edited text. Indexed when present. |
| token_count | INTEGER | NOT NULL DEFAULT 0, CHECK ≥ 0 | Length in tokens. |
| page_start | INTEGER | NOT NULL DEFAULT 1, CHECK ≥ 1 | First page covered. Kept for display and provenance links. |
| page_end | INTEGER | NOT NULL DEFAULT 1, CHECK ≥ `page_start` | Last page covered. |
| source_spans | JSONB | NOT NULL DEFAULT '[]', CHECK array | List of `{page_number, start_char, end_char}` pointing back into the page text. |
| section_title | TEXT | | Heading of the section. |
| content_type | TEXT | | Kind of content (`heading`, `body`, ...). Only those two seen in the chunkers, so left as `TEXT`. |
| is_reference | BOOLEAN | NOT NULL DEFAULT FALSE | Reference material. Search can exclude it. |
| is_reviewed | BOOLEAN | NOT NULL DEFAULT FALSE | Reviewed by a human. |
| is_excluded | BOOLEAN | NOT NULL DEFAULT FALSE | Excluded from indexing. |
| reviewer_notes | TEXT | | Reviewer comments. |
| created_at | TIMESTAMPTZ | NOT NULL DEFAULT now() | Created. |
| updated_at | TIMESTAMPTZ | NOT NULL DEFAULT now() | Last change (trigger). |

Notes
- Chunking provider, model, config and run number are stored once per document in `documents.metadata.chunking`, not per chunk.
- The distinct page numbers a chunk draws from are derived from `source_spans`.
- Re-chunk runs in one transaction: delete the document's chunks, insert the new ones, update `metadata.chunking` (with `run_number` incremented) and `metadata.chunk_count`. Chunk UUIDs change on every re-chunk; nothing references `chunks.id` (audit uses chunk number).
- `search_chunks` does a substring match on `lower(coalesce(edited_text, original_text))`, served by a trigram GIN index on that expression (the migration creates the `pg_trgm` extension).

> **Why `pages` and `chunks` stay separate tables:** a page is a physical unit from OCR and a chunk is a semantic unit from the chunker, so their boundaries do not align. One chunk can span several pages and one page can hold several chunks, which makes the relationship many-to-many. Storing page text inside each chunk would copy a page into every chunk it overlaps, so any page edit, OCR retry or translation change would have to be rewritten in all of those chunks.


---

## 8. `audit_logs`

Append-only history of actions. Must outlive documents, so there is no cascade delete.

| Column | Type | Constraints / Default | Purpose |
|---|---|---|---|
| id | UUID | PRIMARY KEY, DEFAULT gen_random_uuid() | Audit entry ID. |
| document_id | UUID | FK → `documents(id)` ON DELETE SET NULL | Document the action applied to. NULL for system-level entries and after a purge. |
| workflow_id | TEXT | | Temporal workflow ID snapshot. Survives a purge. |
| instance | TEXT | | Tenant snapshot, so tenant scoping works after the document is gone. |
| action_type | TEXT | NOT NULL, CHECK not blank | What happened (`page_edit`, `purge_document`, ...). 22 values and growing, so not an enum. |
| entity_type | audit_entity_type | | `document`, `page`, `chunk`, `setting` or `catalog`. |
| entity_number | INTEGER | | Page number or chunk number the action applied to. |
| field_name | TEXT | | Field that changed. |
| old_value | TEXT | | Value before the change. |
| new_value | TEXT | | Value after the change. |
| metadata | JSONB | NOT NULL DEFAULT '{}', CHECK object | Extra context. |
| actor_user_id | UUID | FK → `users(id)` ON DELETE SET NULL | Who acted. NULL for system actions. |
| actor | JSONB | NOT NULL DEFAULT '{}', CHECK object | Identity at the time: `{username, email, roles[]}`. |
| created_at | TIMESTAMPTZ | NOT NULL DEFAULT now() | When it happened. |

Table CHECK: `entity_type IN ('setting','catalog') OR workflow_id IS NOT NULL`.

Indexes: `(document_id, created_at DESC)`, `(created_at DESC)`, `(action_type, created_at DESC)`, `(created_at DESC) WHERE entity_type = 'setting'`.

Notes
- The FK to `documents` is `SET NULL`, not cascade. A cascade would erase the audit trail on purge, and a strict FK would reject setting and catalog entries that have no document.
- Setting and catalog entries have NULL `document_id` and `workflow_id`, with `entity_type` set to `setting` or `catalog`.
- `instance` is a snapshot, so a purged document's history stays visible only to its own tenant.
- `entity_number` holds a page or chunk number, not an ID (chunk UUIDs would dangle after a re-chunk).
- `get_settings_audit_logs` is `WHERE entity_type = 'setting'`.
- `log_audit` fills `instance`, resolves the actor to a `users` row and builds the `actor` JSON.
- No `updated_at`: entries are immutable.
- The table only grows. Monthly partitioning is possible later without changing this design.

---

## 9. `search_settings`

Single row of search configuration.

Unique: `singleton`.

| Column | Type | Constraints / Default | Keys inside | Purpose |
|---|---|---|---|---|
| id | UUID | PRIMARY KEY, DEFAULT gen_random_uuid() | | Row ID. |
| singleton | BOOLEAN | NOT NULL DEFAULT TRUE UNIQUE, CHECK `singleton` | | Guarantees only one row. |
| retrieval | JSONB | NOT NULL DEFAULT '{}', CHECK object | `searchMethod`, `alpha`, `rankingMethod`, `hybridRrfK` | How semantic and keyword matching are combined and ranked. |
| candidates | JSONB | NOT NULL DEFAULT '{}', CHECK object | `indexName`, `efSearch`, `candidateCap`, `candidateMultiplier` | Which vector index is searched and how many candidates are pulled. |
| results | JSONB | NOT NULL DEFAULT '{}', CHECK object | `limit`, `maxChunksPerDoc`, `excludeReference`, `showHighlights`, `rerankMode` | How the final result list is shaped. |
| query | JSONB | NOT NULL DEFAULT '{}', CHECK object | `useE5Prefix`, `queryExpansionProfile` | How the query is prepared before searching. |
| created_at | TIMESTAMPTZ | NOT NULL DEFAULT now() | | Created. |
| updated_at | TIMESTAMPTZ | NOT NULL DEFAULT now() | | Last change (trigger). |

15 settings in four groups (4 + 4 + 5 + 2). All are global, with no per-tenant scope.

Notes
- The database enforces only "is a JSON object". Value rules live in Pydantic: `SearchSettings` has four nested sub-models (`alpha` 0 to 1, integers ≥ 1, `searchMethod` in `TENSOR|LEXICAL|HYBRID`, `rankingMethod` in `rrf|normalize_linear`, unknown keys rejected).
- The API stays flat. `db.py` keeps one dict mapping each flat key to its group; reads flatten the columns and writes route each key to its column.
- Defaults live only in the Pydantic models. Reads merge stored JSON over the defaults. The default for `alpha` is 0.6.
- Partial update is an atomic merge per group: `UPDATE search_settings SET retrieval = retrieval || $1::jsonb`.
- Audit: one `audit_logs` row per changed key, `entity_type = 'setting'`, `field_name` such as `retrieval.alpha`.
- `rerankMode` has at least `none` and `bm25lite`; the full set was not seen, so no enum.
- Adding a setting to an existing group needs no migration; a new group needs a column.

---

## 10. `email_otps`

Server-side email-OTP login codes. Short-lived.

Unique: `email`; index on `(expires_at)`.

| Column | Type | Constraints / Default | Purpose |
|---|---|---|---|
| id | UUID | PRIMARY KEY, DEFAULT gen_random_uuid() | Row ID. |
| email | TEXT | NOT NULL UNIQUE, CHECK `email = lower(email)` | Address the code was requested for. One live code per email. |
| code_hash | TEXT | NOT NULL | Hash of the one-time code. The plain code is never stored. Random for unknown accounts. |
| expires_at | TIMESTAMPTZ | NOT NULL | When the code stops being valid. |
| attempts | INTEGER | NOT NULL DEFAULT 0, CHECK ≥ 0 | Failed verifications. Reset to 0 on resend. |
| last_sent_at | TIMESTAMPTZ | NOT NULL DEFAULT now() | When the code was last sent. Used for the resend cooldown. |

Notes
- No `created_at` or `updated_at`: rows are overwritten on each send.
- `email` is deliberately not an FK to `users`. A row is written for every request, even for unknown addresses (anti-enumeration, `email_otp.py:276-310`).
- Periodic cleanup (`DELETE FROM email_otps WHERE expires_at < now() - interval '1 day'`) is not scheduled yet. The endpoint is unauthenticated, so unverified rows stay until it is.
- `verify_otp` increments attempts atomically: `UPDATE ... SET attempts = attempts + 1 WHERE email = $1 RETURNING attempts`, or `SELECT ... FOR UPDATE`.
- Code constants (length 6, TTL 300 s, resend cooldown 30 s, max 5 attempts) stay in code.
- Optional: make the table `UNLOGGED`. Losing it in a crash only forces users to request a new code.

---

## View: `catalog`

Computed from `catalog_entries`, `documents`, `workflows` and `document_current_workflow`. Stores nothing. The `/catalog/v1/*` snapshot API reads it with `type = 'scheme'`, and its fields are named `scheme_code`, `scheme_name` and so on, so consumers see no change.

| Column | Type | How it is computed |
|---|---|---|
| entry_id | UUID | From `catalog_entries`. |
| type | TEXT | From `catalog_entries`. |
| code | TEXT | From `catalog_entries`. |
| name | TEXT | From `catalog_entries`. |
| aliases | TEXT[] | From `catalog_entries`. |
| source | TEXT | `catalog_entries.metadata->>'source'`. |
| status | TEXT | `pending_reindex` if a live document needs reindex. Else `live` if any live document exists. Else `pending_prod` if a document awaits PROD approval. Else `live` for a bootstrap entry with no documents. Else `disabled`. |
| instances | TEXT[] | Distinct `instance` of the entry's documents, or `{default}` if none. |
| workflow_ids | TEXT[] | `temporal_workflow_id` of each entry document's main `pipeline` workflow. |
| chunk_count | INTEGER | Sum of `metadata->>'chunk_count'` over the entry's documents. |
| network_visible | BOOLEAN | True if any of its documents has `metadata->>'network_visible'` true. For an entry with no documents, true if bootstrap. |
| promoted_at | TIMESTAMPTZ | Latest `index_status->'prod'->>'last_indexed_at'` of the live documents. |

"Live document" means the latest workflow's `current_stage = 'completed'`, `is_disabled` false, and `metadata->>'catalog_visible'` true. The entry's `type` already ties it to the document's kind through the composite FK.

---

## 11. `webhook_records`

Records pushed to `POST /webhook` (`x-api-key` = `WEBHOOK_API_KEY`). Carried over from the previous store unchanged apart from the types.

Unique: `(source, external_id)`; index on `(source, status)`.

| Column | Type | Constraints / Default | Purpose |
|---|---|---|---|
| id | UUID | PRIMARY KEY, DEFAULT gen_random_uuid() | Record ID. |
| source | TEXT | NOT NULL DEFAULT 'webhook' | Sender. |
| external_id | TEXT | NOT NULL | `contentID`, else `id`, else `slug`, else a hash of the payload. |
| title | TEXT | | From the payload. |
| slug | TEXT | | From the payload. |
| status | TEXT | | From the payload. |
| is_published | BOOLEAN | | From `isPublished`. |
| category | TEXT | | From `domainName`. |
| payload | JSONB | NOT NULL, CHECK object | The full record. |
| payload_hash | TEXT | | SHA-256 of the canonical payload; an upsert with the same hash writes nothing. |
| created_at | TIMESTAMPTZ | NOT NULL DEFAULT now() | The payload's `createdAt` when it has one. |
| updated_at | TIMESTAMPTZ | NOT NULL DEFAULT now() | The payload's `updatedAt` when it has one. |

The payload's own timestamps are stored as given, so these two columns are not maintained by the shared trigger.

---

## `schema_migrations`

Created by the runner, not by a migration file: `version` (the `NNNN` prefix, primary key), `name` (the file name) and `applied_at`. The runner takes a session advisory lock, so concurrent api and worker starts apply each file once, and runs each file in its own transaction.

---

## Implementation notes

- **Identifiers.** `db.py` functions take a `workflow_id` argument that may be the document UUID, a Temporal workflow id, the fingerprint, the filename, an index `doc_id` or the legacy `md5(fingerprint)` doc id (`find_document_by_doc_identifier`). Documents, jobs and artifacts come back as flat dicts; `id` is the UUID and `workflow_id` is the Temporal workflow id of the document's main run (the earliest `pipeline` or `ocr_only` workflow). The API rewrites `/documents/{id}/...` to that workflow id before routing, because handlers signal and query Temporal with it.
- **Jobs.** The API's "jobs" and "runs" are `workflows` rows; job ids and artifact ids are the UUIDs of `workflows` and `workflow_artifacts`. `latest_job_id`, `original_artifact_id` and `normalized_artifact_id` are derived by join, not stored.
- **Catalog version.** `/catalog/v1/version` counts `audit_logs` rows with `entity_type = 'catalog'` and `action_type = 'catalog_version_bump'`, so it only grows. `updated_at` is the newest such row.
- **Document stage.** The stage a document shows is `document_current_workflow.current_stage`; `update_document_stage` writes to that workflow.
- **Page translations.** `save_pages` keeps stored translations unless the page carries `translations` (replaces them) or flat `translated_*` fields (merged into `translation_target_language`). A page whose OCR text changed loses its translations.
- **Users.** Rows are upserted from the auth dependency on write requests (at most once per five minutes per user) and from uploads, workflow starts and audit entries. `local-admin` is seeded by the migration.
