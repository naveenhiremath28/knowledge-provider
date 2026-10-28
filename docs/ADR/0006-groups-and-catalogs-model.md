# Providers are groups, catalogs are (group, type), documents are identified by (catalog, code)

Context: the first Postgres schema modelled a tenant as a free-text `documents.instance`, a `catalog_entries` table keyed by `(type, code)` with one entry per scheme, and a free-form `document_kind` that had to match the entry's type through a composite foreign key. Review feedback was that the unit published on the network is a provider's catalog, that a provider is a Keycloak group, that each scheme is itself a document, and that database enums and demo flags do not belong in the schema.

Decision:
- A **group** is a provider: one `groups` row per Keycloak tenant node (`/states/MH`, `/global`). Role leaves are not mirrored; permissions still come from the token. `users.group_id` is a single nullable FK for now (first tenant in the token, sorted, with a warning when there are several); a join table can replace it if multi-group membership is needed.
- A **catalog** is `(group, type)`: `catalogs.group_id` plus `catalogs.type`, unique together. `type` holds the network schema name. The kind-to-schema map lives in `pipeline/network_constants.py` and is a placeholder: `scheme` is `openagrinet:AgricultureResource`, `advisory` is `openagrinet:KnowledgeAdvisory`. Code branches through that map, never on raw `openagrinet:*` strings. Catalog Beckn information (`publish_directives`, `visible_networks`, `criteria`, `network`) sits in `catalogs.metadata`.
- A **document** belongs to one catalog and is identified there by `code`: `documents.catalog_id` NOT NULL, `UNIQUE (catalog_id, code)`, with `name` and `aliases` on the document. The API fields `code`, `name` and `aliases` are those columns. `documents.instance`, `document_kind` and `is_demo` are gone; the API returns the derived `group` (the group name) and `kind` (from the type map). The `/catalog/v1/*` snapshot keys, the Qdrant payload keys and Temporal arguments keep their earlier names (`scheme_code`, `document_kind`, `instance`, ...) because other services and in-flight workflows depend on them.
- Re-uploading an existing code updates the same document (new workflow, `source` replaced, pages and chunks cleared, `reindex_required` set) rather than adding a second one.
- Catalogs are created on upload for the tenant and kind. The upload therefore takes the document type up front.
- Bootstrap schemes (the code-seeded entries with no documents) are dropped. `/catalog/v1/*` is served from the `catalog_status` view, one row per document, and a scheme appears once a document exists.
- Period, uploader and vector index state moved into `documents.metadata`; counts are derived from `pages` and `chunks`.
- Database enums are replaced by plain `TEXT`, validated by `pipeline/models.py` (`DocumentStage`, `WorkflowType`, `WorkflowStatus`, `AuditEntityType`).
- `audit_logs.instance` is replaced by `audit_logs.group_id` (snapshot, `ON DELETE SET NULL`).
- No Postgres held the first schema, so `migrations/0001` was rewritten in place instead of adding an ALTER migration. `scripts/migrate_sqlite_to_postgres.py` produces v2 and skips `kind=document` and `is_demo` documents, reporting the counts.

Alternatives considered:
- **Keep `documents.instance`.** Rejected: the catalog already carries the provider, and two sources of tenant truth drift.
- **Type as a short kind plus a separate `schema_type` column.** Rejected for now in favour of one `type` column holding the schema name; revisit if scheme and advisory end up sharing a schema (ADR 0003 types both as `KnowledgeAdvisory`).
- **`user_groups` join table now.** Deferred: the team wants one group per user for now.
- **Nullable `catalog_id`/`code` until a reviewer classifies the document.** Rejected: a document without a catalog has no tenant once `instance` is gone. The upload form asks for the type instead.
