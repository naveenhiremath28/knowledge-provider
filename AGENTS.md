# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A review-driven document ingestion pipeline (product name "Bharat Vistaar Docs Pipeline"): Temporal workflows + FastAPI + Postgres + MinIO + Qdrant. Documents move through explicit stages (OCR → review → translate → review → chunk → review → ingest DEV → Super Admin promote to PROD), with every major output persisted as an inspectable artifact rather than hidden.

Full narrative docs (read these before deep architectural changes, don't re-derive):
- `README.md` — what the system does, API surface, supported inputs, auth model
- `docs/SYSTEM_DESIGN.md` — C4 diagrams, containers, DEV/PROD indexing, stage/activity table
- `docs/architecture-pipeline-rbac.md` — login flow, roles, DEV vs Super-Admin-PROD gate
- `docs/MASTER_SCHEME_CATALOG_AND_AI_SYNC_DESIGN.md` — original scheme catalog design (historical: the Postgres master catalog and Redis push it describes were removed; the pull API `/catalog/v1/*` remains)
- `ENV.md` — full annotated environment variable reference (`.env.example` for a working template)

## Commands

```bash
# Run the full stack (api, worker, temporal, minio, qdrant, ui)
docker compose up -d --build
docker compose down

# Tests (pytest.ini: testpaths=tests, markers unit/integration/slow/api/db/workflow)
uv run pytest
uv run pytest -m "not slow"                    # skip slow tests
uv run pytest tests/test_activities.py -v      # single file
uv run pytest tests/test_api.py::test_name     # single test

# Lint
uv run ruff check .

# Quick sanity checks (no test infra needed)
uv run python -c "import pipeline.api"
cd ui && npm run build

# UI dev server
cd ui && npm run dev        # vite, talks to API on :8001
cd ui && npm run build

# Health checks against a running stack
curl http://localhost:8001/health
curl http://localhost:6333/                       # qdrant
curl http://localhost:9000/minio/health/live
```

Local ports: UI `3000`, API `8001`, Qdrant `6333`, Temporal `7233`, Temporal UI `8080`, MinIO API `9000`/console `9001`, Keycloak `8082` (path `/auth`).

Interactive API docs once running: `http://localhost:8001/docs`.

## Architecture

### Everything lives in `pipeline/`, not split into microservices

`pipeline/api.py` (~4.3k lines) is a single FastAPI app with all routes declared directly via `@app.get/post/patch/delete` — there is no router-per-resource split. When adding an endpoint, find the existing block by resource (documents, pages, chunks, artifacts, catalog, audit, admin) and add near its siblings rather than creating a new file.

Core modules (all under `pipeline/`):
- `api.py` — FastAPI app, all HTTP routes
- `workflows.py` — Temporal workflow definitions (`DocumentPipelineWorkflow` is the main one; also `ReingestionWorkflow`, `PromoteToProdWorkflow`, `TranslationOnlyWorkflow`, `OcrOnlyWorkflow`, `ChunkingOnlyWorkflow`)
- `activities.py` — Temporal activities invoked by the workflows (OCR, translate, chunk, ingest, state updates)
- `worker.py` — Temporal worker process; registers workflows/activities on task queue `ocr-pipeline`
- `db.py` — Postgres access layer (psycopg3 sync connection pool); Postgres is the **canonical** metadata/review-state store (documents, workflows, pages, chunks, artifacts, audit log, settings), not just a cache. Documents and jobs come back as flat dicts keyed by `workflow_id` (the document's main Temporal workflow id); every function also accepts the document UUID. Schema: `migrations/` (applied at api/worker startup by `migrate.py`), described in `docs/POSTGRES_SCHEMA.md`
- `document_repository.py` — `DocumentRepository`, a domain layer over `db.py` for document reads; `db.py` stays one-function-per-query with no domain knowledge
- `network_constants.py` — fixed values sent on the network (catalog/resource ids, topics, languages, Beckn version). The file v2 edits when the AI layer starts deriving them
- `catalog_builder.py` — pure builder mapping a document's knowledge kind (`advisory`/`scheme`) to the single `OnDemand` Beckn catalog announced for that kind; returns `None` for any other kind. No env, no I/O
- `document_validity.py` — pure module owning Document Validity: the period a document's chunks answer searches in, the upload-day start with no end by default, and the clock injection point. Shared by `api.py` (stamping the default at upload), `scheme_catalog.py` (validating a reviewer's edit), `activities.py` (stamping chunk payloads) and `vector_store/qdrant_store.py` (filtering at search time). `db.py` stores the dates it is handed and defaults nothing
- `discovery_publish_service.py` — `DiscoveryPublishService`: owns the Publish to Network env vars and the HTTP call. Deliberately Temporal-free
- `models.py` — Pydantic models, including `DocumentStage` enum and `PIPELINE_STAGES` (the stepper-UI stage list)
- `config.py` — `Config` dataclass reading env vars, with defaults
- `instances.py` — instance/tenant (state) code ↔ human name mapping; `bv` = Bharat Vistaar platform-wide, others are state codes matching Keycloak group paths (`/states/MH/...` → `mh`)
- `scheme_catalog.py` — scheme catalog behind the pull-based `/catalog/v1/*` API, served from the `catalog_entries` table and the computed `catalog` view
- `ocr/`, `translation/`, `chunking/`, `vector_store/` — pluggable provider packages, each with a `base.py` protocol/ABC and a `service.py` (or equivalent) that dispatches to the configured provider
- `auth/` — Keycloak JWT validation, group-based tenancy/permissions, email-OTP flow, admin user management

### Provider pattern

`ocr/`, `translation/`, `chunking/`, `vector_store/` each define a `base.py` protocol and one-or-more concrete implementations selected by env var (e.g. `OCR_PROVIDER`, `TRANSLATION_PROVIDER`, `CHUNKING_PROVIDER`) via a `service.py` factory. Adding a new provider means implementing the base protocol and registering it in that package's service dispatcher — not touching `activities.py` call sites.

### Temporal workflow model

`DocumentPipelineWorkflow` pauses at review stages via Temporal **signals** and resumes on operator action (the approve-* API endpoints send the signal). Stage order (`DocumentStage` in `models.py` / `PIPELINE_STAGES`):

```
registered → ocr_processing → ocr_review → translation_processing → translation_review
  → chunking → chunk_review → ready_for_ingestion → ingesting
  → approval_for_prod → ingesting_prod → completed   (or → failed at any point)
```

`approval_for_prod`/`ingesting_prod` are **PROD-only stages** (`PROD_ONLY_STAGES` in `models.py`); with `DISABLE_PROD_SETTING=true` a document goes straight from `ingesting` to `completed`. That flag is read only at workflow *start* and passed in as an argument — never read inside a running workflow, since that would break Temporal replay determinism if flipped mid-flight (see the docstring in `instances.py`).

### DEV vs PROD dual indexing

`ingest_document_from_db` (activity) writes the **DEV** index via `pipeline/vector_store` (`VectorStore` protocol in `base.py`, `QdrantVectorStore` the only implementation). A separate Super-Admin-only approval gate (`approve-prod` / `request-prod-ready` endpoints, `PromoteToProdWorkflow`) triggers `promote_document_to_prod_qdrant`, which promotes into the **PROD** index — a *different* Qdrant collection/deployment (`PROD_VECTOR_DB_*` env vars), reached by constructing a `QdrantVectorStore` directly rather than through the `VectorStore` protocol. DEV and PROD are both Qdrant; they're just two separate indexes, not two backends.

### Storage responsibilities (don't blur these)

- **Postgres** — canonical metadata/content/review state (documents, workflows, pages, chunks, artifacts, audit, search settings). Source of truth for anything editable.
- **MinIO** — blob storage for original uploads, normalized files, and stage artifacts (OCR/translation/chunk exports, vector-index payload snapshots).
- **Temporal** — orchestration/retries/review-gate signaling only; not a content store.
- **Qdrant** — downstream search projections of approved chunk state; never the place to edit content.

### Auth (optional, off by default)

`AUTH_DISABLED=true` by default — no login, a synthetic local admin is used. When enabled (`AUTH_DISABLED=false`), the API validates Keycloak RS256 JWTs against realm JWKS. Preferred model is Keycloak **groups** (`/states/{STATE}/contributor`, `/global/super-admin`); realm roles + `instances` claims are a legacy fallback. `pipeline/auth/` holds `jwt.py` (validation), `groups.py`/`tenancy.py` (state scoping), `permissions.py` (role→permission mapping), `email_otp.py` (OTP login flow), `keycloak_admin.py` (admin API calls). See `docs/architecture-pipeline-rbac.md` for the full role table and login flow before changing anything here.

### UI (`ui/`)

React 18 + Vite + Tailwind v4 + Radix UI operator console (not a plain upload form) — dashboard, document operations (stage/artifacts/jobs/pages/chunks/vector-index state/audit in one view), search workbench, settings, audit log. `ui/src/` layout: `views/`, `components/` (+ `components/ui/` for primitives), `hooks/`, `lib/`, `auth/`, `config/`, `styles/`.

### Scripts (`scripts/`)

Operational tools, not one-off hacks — e.g. `list_failed_workflows.py`, `terminate_stuck_workflows.py`, `keycloak_bootstrap_docs_pipeline.py`, `migrate_sqlite_to_postgres.py`. Check here before writing a new maintenance script — an equivalent likely exists.
