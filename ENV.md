# Environment variables — Frontend & Backend

Single reference for every environment variable used by the docs-pipeline **UI (FE)** and **API / worker (BE)**.  
Sources: `ui/.env.example`, root `.env.example`, and runtime `os.environ` / `import.meta.env` reads in code.

---

## Frontend (Vite / `ui/`)

Anything the browser needs **must** be prefixed with `VITE_`.  
Load from `ui/.env` (local) or build-time env. See also `ui/.env.example`.

| Variable | Default | Required when | Purpose |
|----------|---------|---------------|---------|
| `VITE_API_PROXY_TARGET` | `http://api:8001` (compose) / use `http://localhost:8001` locally | Dev server | Vite proxy target for `/api` → FastAPI |
| `VITE_AUTH_ENABLED` | `false` | — | `true` shows the login page and requires SSO; `false` opens the app without login |
| `VITE_KEYCLOAK_URL` | `''` | Auth on | Keycloak base URL (include `/auth` if used). Production: `https://auth-vistaar.da.gov.in/auth` |
| `VITE_KEYCLOAK_REALM` | `''` | Auth on | Realm name. Production: `bharat-vistaar` |
| `VITE_KEYCLOAK_CLIENT_ID` | `docs-pipeline-ui` | Auth on | Public OIDC client. Production shared client: `bharat-vistaar` |
| `VITE_KEYCLOAK_IDP_HINT` | `google` | Optional | Identity-provider hint for SSO pop-up (`google`, etc.) |

### Frontend auth notes

- Login UI is ported from **vistaar-platform** (`/login`, hero panel, “Continue with SSO” pop-up).
- SSO callback route: `/auth/sso-callback`.
- Register these **Valid Redirect URIs** on the Keycloak client:
  - `http://localhost:<port>/login`
  - `http://localhost:<port>/auth/sso-callback`
  - `https://<your-app-origin>/login`
  - `https://<your-app-origin>/auth/sso-callback`
- When auth is enabled, the UI calls `GET /api/auth/me` with `Authorization: Bearer <token>`.  
  Backend must have `AUTH_DISABLED=false` and matching `KEYCLOAK_*` or the session will fail after SSO.

### Recommended production FE values

```bash
VITE_AUTH_ENABLED=true
VITE_KEYCLOAK_URL=https://auth-vistaar.da.gov.in/auth
VITE_KEYCLOAK_REALM=bharat-vistaar
VITE_KEYCLOAK_CLIENT_ID=bharat-vistaar
VITE_KEYCLOAK_IDP_HINT=google
VITE_API_PROXY_TARGET=http://localhost:8001   # or in-cluster API URL in compose
```

---

## Backend (API + worker)

Root `.env` (see `.env.example`). Required by FastAPI (`pipeline/api.py`), Temporal worker (`pipeline/worker.py`), and activities.

### Required

| Variable | Default | Purpose |
|----------|---------|---------|
| `MINIO_ACCESS_KEY` | *(required)* | MinIO access key |
| `MINIO_SECRET_KEY` | *(required)* | MinIO secret key |

### Core infrastructure

| Variable | Default | Purpose |
|----------|---------|---------|
| `TEMPORAL_HOST` | `localhost:7233` | Temporal gRPC address |
| `TEMPORAL_MAX_CONCURRENT_ACTIVITIES` | `4` | Worker activity concurrency |
| `LOG_LEVEL` | `INFO` | Worker log verbosity (`DEBUG`/`INFO`/`WARNING`/`ERROR`); the `temporalio` SDK logger stays at `INFO` regardless, to avoid its own noisy debug output |
| `MINIO_ENDPOINT` | `localhost:9000` | MinIO API host:port |
| `MINIO_BUCKET` | `documents` | Object storage bucket |
| `DOCUMENT_DB_PATH` | `/data/documents.db` | SQLite path (use `./data/documents.db` for local non-Docker) |

### API HTTP surface

| Variable | Default | Purpose |
|----------|---------|---------|
| `CORS_ORIGINS` | `http://localhost:3000` | Comma-separated browser origins (add `http://localhost:3001` if UI uses that port) |
| `RATE_LIMIT_DEFAULT` | `100/minute` | Default rate limit |
| `RATE_LIMIT_UPLOAD` | `10/minute` | Upload rate limit |
| `ALLOWED_FILE_PATHS` | `/app/books,/data/documents` | Allowed local paths for path-based ingest |
| `DOCS_PIPELINE_API_URL` | request base URL | Public API base for provenance/links |
| `DOCS_PIPELINE_UI_URL` | `http://localhost:3000` | Public UI base for links |
| `DEFAULT_INSTANCE` | `default` | Default tenant / instance id |

### Webhook (`POST /webhook`)

| Variable | Default | Purpose |
|----------|---------|---------|
| `WEBHOOK_MAX_RECORDS` | `500` | Max records per `/webhook` request; more returns 413 |
| `WEBHOOK_API_KEY` | *(empty — required)* | 32-char hex secret. Callers send it in the `x-api-key` header. Missing/invalid header → 401. Unset/empty → 503 (fails closed, ignores `AUTH_DISABLED`). Generate with `openssl rand -hex 16`. |

Body is a JSON array of objects (max `WEBHOOK_MAX_RECORDS`); stored in the SQLite `webhook_records` table, upserted on `(source, external_id)` where `external_id` is `contentID` (else `id`, `slug`, payload hash).

### Auth / Keycloak (backend JWT validation)

| Variable | Default | Purpose |
|----------|---------|---------|
| `AUTH_DISABLED` | `true` | `true` = bypass permissions (still reads name/email/roles from JWT when sent). `false` = enforce JWT validation |
| `KEYCLOAK_ISSUER` | `''` | **Must match FE realm** — JWT `iss` e.g. `https://auth-vistaar.da.gov.in/auth/realms/bharat-vistaar` |
| `KEYCLOAK_JWKS_URL` | derived from issuer | JWKS URL for the same realm |
| `KEYCLOAK_AUDIENCE` | `''` (skip aud) | Leave empty for SPA tokens; set only if you enforce a fixed `aud` claim |
| `KEYCLOAK_JWT_LEEWAY_SECONDS` | `30` | Clock-skew leeway for `exp`/`nbf` |
| `KEYCLOAK_ADMIN_USERNAME` | `''` | Super-admin **Users** UI: Keycloak admin username (often master `admin`) |
| `KEYCLOAK_ADMIN_PASSWORD` | `''` | Keycloak admin password for provisioning users |
| `KEYCLOAK_ADMIN_BASE_URL` | from `KEYCLOAK_ISSUER` | e.g. `https://dev-auth-vistaar.da.gov.in/auth` |
| `KEYCLOAK_ADMIN_REALM` | from issuer | Realm to manage (e.g. `bharat-vistaar`) |
| `KEYCLOAK_ADMIN_TOKEN_REALM` | `master` | Realm used to obtain admin token via `admin-cli` |

**FE ↔ BE pairing (required for SSO):**

| Frontend (`ui/.env`) | Backend (root `.env`) |
|----------------------|------------------------|
| `VITE_KEYCLOAK_URL` + `VITE_KEYCLOAK_REALM` | `KEYCLOAK_ISSUER` = `{URL}/realms/{REALM}` |
| same realm | `KEYCLOAK_JWKS_URL` = `{ISSUER}/protocol/openid-connect/certs` |
| `VITE_KEYCLOAK_CLIENT_ID=bharat-vistaar` | public client used by browser only |
| `VITE_AUTH_ENABLED=true` | send Bearer tokens; optional `AUTH_DISABLED=false` to enforce them |

Keycloak is an external instance (not provisioned by this repo's compose files) — see `KEYCLOAK_*` above for connecting to it.

### OCR (Chandra)

OCR is a **separate process** (not in `docker-compose`). The worker calls
`CHANDRA_VLLM_BASE_URL` (HF mode → `POST {base}/ocr/pages`).

| Variable | Default | Purpose |
|----------|---------|---------|
| `OCR_PROVIDER` | `chandra` | OCR provider (`chandra`, `mock`, `pypdf`) |
| `OCR_MODEL` | `chandra` | Model name |
| `CHANDRA_VLLM_BASE_URL` | `''` | Base URL; local HF default `http://localhost:8010/v1` |
| `CHANDRA_OCR_API_URL` | `''` | Alternate full OCR API URL (overrides base path build) |
| `CHANDRA_INFERENCE_MODE` | `hf` | `hf` = HTTP page API; `vllm` = chandra-ocr client + OpenAI base |
| `CHANDRA_MAX_OUTPUT_TOKENS` | `12288` | Max generation tokens |
| `CHANDRA_OCR_MAX_WORKERS` | `4` | Parallel OCR workers |
| `CHANDRA_IMAGE_DPI` | `192` | Page render DPI |
| `CHANDRA_REQUEST_TIMEOUT_SECONDS` | `300` | Request timeout |
| `OCR_MAX_SPLIT_PAGES` | `40` | Max split pages |
| `OCR_SEGMENT_PAGES` | `20` | Segment size for long docs |
| `CHANDRA_HF_HOME` / `HF_HOME` | — | Hugging Face cache (HF server / scripts) |

**Local start (real model, GPU + torch + model download):**

```bash
# requires: pip install chandra-ocr torch  (and enough VRAM/disk for Chandra weights)
python scripts/chandra_hf_server.py   # listens on :8010
```

**Local unblock without GPU (placeholder OCR):**

```bash
python scripts/mock_chandra_ocr_server.py   # same :8010 API surface as HF server
# or in-process (no HTTP server): OCR_PROVIDER=mock  # restart worker
```

### Translation (Gemma)

| Variable | Default | Purpose |
|----------|---------|---------|
| `TRANSLATION_PROVIDER` | `gemma_vllm` | Provider |
| `TRANSLATION_MODEL` | `gemma-4-31b-it` | Model id |
| `TRANSLATION_VLLM_BASE_URL` | `http://localhost:8020/v1` | OpenAI-compatible endpoint |
| `TRANSLATION_API_KEY` | `''` | Optional API key |
| `TRANSLATION_PAGE_CONCURRENCY` | `1` | Parallel pages |
| `TRANSLATION_MAX_RETRIES` | `6` | Retry count |
| `TRANSLATION_RETRY_BASE_SECONDS` | `2.0` | Backoff base |
| `TRANSLATION_MAX_OUTPUT_TOKENS` | `8000` | Max tokens |
| `TRANSLATION_REQUEST_TIMEOUT_SECONDS` | `300` | Timeout |
| `TRANSLATION_USE_MAX_COMPLETION_TOKENS` | `false` | `true` sends `max_completion_tokens` instead of `max_tokens` — required by some OpenAI-compatible endpoints (e.g. Azure AI Foundry) that reject `max_tokens` outright |
| `TRANSLATION_TEMPERATURE` | `0.0` | Sent as `temperature` in the request. Set to empty (`TRANSLATION_TEMPERATURE=`) to omit the field entirely — some endpoints (e.g. Azure AI Foundry) reject any non-default temperature. Independent of `TRANSLATION_USE_MAX_COMPLETION_TOKENS` |
| `DISABLE_PROD_SETTING` | `false` | `true` skips the `approval_for_prod` and `ingesting_prod` stages — documents complete at DEV ingest. Read at workflow start; in-flight documents keep the shape they began with |
| `TRANSLATION_SCRIPT_GATE_ENABLED` | `true` | Regex script gate: only pages with non-Latin (Indic) script are translated. `false` restores per-line pyfranc detection, which misreads OCR noise as European languages |
| `TRANSLATION_SCRIPT_MIN_CHARS` | `15` | Minimum non-Latin characters on a page before it counts as non-English |
| `TRANSLATION_SCRIPT_MIN_RATIO` | `0.05` | Minimum share of all letters that must be non-Latin |
| `SCRIPT_FAMILIES_CONFIG_PATH` | unset (bundled `pipeline/translation/script_families.json`) | Path to a JSON file that fully replaces the script → language table used by the regex gate and pyfranc disambiguation |

### Domain tagging

| Variable | Default | Purpose |
|----------|---------|---------|
| `DOMAIN_TAGGING_ENABLED` | `true` | Enable/disable |
| `DOMAIN_TAGGING_PROVIDER` | `gemma_vllm` | Provider |
| `DOMAIN_TAGGING_MODEL` | falls back to `TRANSLATION_MODEL` | Model |
| `DOMAIN_TAGGING_VLLM_BASE_URL` | falls back to `TRANSLATION_VLLM_BASE_URL` | Endpoint |
| `DOMAIN_TAGGING_API_KEY` | falls back to `TRANSLATION_API_KEY` | Key |
| `DOMAIN_TAGGING_STRICT_TAXONOMY` | `true` | Restrict tags to taxonomy |
| `DOMAIN_TAXONOMY_PATH` | package default | Path to taxonomy JSON |
| `DOMAIN_TAGGING_CONCURRENCY` | `4` | Parallelism |
| `DOMAIN_TAGGING_REQUEST_TIMEOUT_SECONDS` | `120` | Timeout |
| `DOMAIN_TAGGING_MAX_OUTPUT_TOKENS` | `1024` | Max tokens |

### Chunking

| Variable | Default | Purpose |
|----------|---------|---------|
| `CHUNKING_PROVIDER` | `recursive_splitter` | Provider (`recursive_splitter` or `deterministic`) |
| `CHUNKING_MODEL` | provider name | Model id (label only; no LLM calls) |
| `CHUNKING_TARGET_CHUNK_TOKENS` | `450` | Target chunk size |
| `CHUNKING_MAX_CHUNK_TOKENS` | `450` | Max chunk size |
| `CHUNKING_MIN_CHUNK_TOKENS` | `100` | Min chunk size |
| `CHUNKING_OVERLAP_TOKENS` | `128` | Overlap |
| `CHUNKING_MAX_PAGES_PER_CHUNK` | `8` | Max page span |
| `CHUNKING_PAGE_WINDOW_SIZE` | `8` | Window size |
| `CHUNKING_FALLBACK_PROVIDER` | `deterministic` | Fallback provider |
| `CHUNKING_REQUEST_TIMEOUT_SECONDS` | `120` | Timeout |

### Metadata enrichment (worker)

| Variable | Default | Purpose |
|----------|---------|---------|
| `DOCUMENT_METADATA_CSV_PATH` | `/app/workspace/document_manifest.csv` | Optional manifest CSV |
| `DOCUMENT_DESCRIPTIONS_JSONL_PATH` | `/app/workspace/document_descriptions.jsonl` | Optional descriptions JSONL |

### Vector backend (Qdrant)

Search, index status, deletes, and ingestion go through `pipeline/vector_store` → Qdrant.

| Variable | Purpose |
|----------|---------|
| `VECTOR_DB_URL` | Vector DB base URL (e.g. `http://localhost:6333` or reverse-proxy HTTPS) |
| `VECTOR_DB_API_KEY` | Vector DB API key (required for non-local hosts) |
| `VECTOR_DB_COLLECTION_NAME` | Collection name (default `documents-index`) |
| `VECTOR_DB_TIMEOUT_SECONDS` | Client timeout |
| `EMBEDDING_PROVIDER` | `sentence_transformers` (local) or `openai_compatible` |
| `EMBEDDING_MODEL` | Embedding model id (default `intfloat/multilingual-e5-large`) |
| `EMBEDDING_VECTOR_SIZE` | Vector dimensions (default `1024`) |
| `EMBEDDING_BASE_URL` / `EMBEDDING_API_KEY` | Remote embeddings API when `openai_compatible` |

### Master Scheme Catalog (AI tool / prompt sync)

Exposes `/catalog/v1/*` so **bharat-oan-api** and **bharat-provider-backend** can refresh scheme lists and tool prompts without redeploying hard-coded registries.

| Variable | Default | Purpose |
|----------|---------|---------|
| `CATALOG_SERVICE_API_KEYS` | *(empty)* | Comma-separated service keys for `X-Catalog-Service-Key` (OAN/provider warmers). When empty and `AUTH_DISABLED=true`, catalog is open to admin/search JWT bypass. |
| `CENTRAL_INSTANCES` | `default` | Instances that default `network_visible=true` for new scheme docs |
| `PROD_VECTOR_DB_URL` | — | PROD vector DB for document promote |
| `PROD_VECTOR_DB_API_KEY` | — | PROD vector DB key |
| `PROD_VECTOR_DB_COLLECTION_NAME` | `documents-index` | PROD collection for normal documents |
| `PROD_SCHEME_VECTOR_DB_URL` | `PROD_VECTOR_DB_URL` | PROD vector DB for scheme promotes |
| `PROD_SCHEME_VECTOR_DB_API_KEY` | `PROD_VECTOR_DB_API_KEY` | Scheme collection API key |
| `PROD_SCHEME_VECTOR_DB_COLLECTION_NAME` | `schemes-index` | Must **not** equal documents collection |

**Endpoints:** `GET /catalog/v1/snapshot`, `/version`, `/schemes`, `/tool-prompt`; `POST /catalog/v1/rebuild`, `/bootstrap`; `PATCH /documents/{id}/scheme-metadata`.

### Publish to Network

First piece of the network-discovery push architecture: once a document's PROD promotion succeeds, the `publishing_to_network` pipeline stage POSTs a Beckn-shaped `publish` envelope to an external Discovery Service. It's a PROD-only stage (see `docs/ADR/0004-publishing-to-network-is-prod-only.md`) — skipped entirely when `DISABLE_PROD_SETTING=true`, since a document never reaches the network without first promoting to PROD. This is going to be the only way the experience layer discovers schemas going forward — the Master Scheme Catalog pull API above stays in place for backward compatibility. The catalog announces the *knowledge kind* the document was classified as — one catalog with exactly one `OnDemand` resource for `advisory`, another for `scheme`; any other `document_kind` publishes nothing. Publishing is **off by default**: set `PUBLISH_TO_NETWORK=true` to turn it on (see `docs/ADR/0005-publish-to-network-is-opt-in.md`). While it is off the activity records a `network_publish_skipped` artifact and the stage is left out of `GET /pipeline/stages`, so the UI stepper does not show it. `DISCOVERY_SERVICE_ENDPOINT` and `NETWORK_SENDER_ID` are **required only when `PUBLISH_TO_NETWORK` is on**.

| Variable | Default | Purpose |
|----------|---------|---------|
| `PUBLISH_TO_NETWORK` | `false` | Turns the Discovery Service publish on (`true`/`1`/`yes`). Read when the publish activity runs, not when a workflow starts, so changing it never affects the path of a workflow already in flight |
| `DISCOVERY_SERVICE_ENDPOINT` | *(empty — required when `PUBLISH_TO_NETWORK` is on)* | Base URL of the external Discovery Service; the pipeline POSTs to `{DISCOVERY_SERVICE_ENDPOINT}/publish` |
| `NETWORK_SENDER_ID` | *(empty — required when `PUBLISH_TO_NETWORK` is on)* | This instance's Beckn `senderId`, also sent as `networkId` |
| `DISCOVERY_SERVICE_PUBLISH_INITIAL_INTERVAL_SECONDS` | `30` | Wait before the first retry of a failed publish |
| `DISCOVERY_SERVICE_PUBLISH_BACKOFF_COEFFICIENT` | `2.0` | Multiplier applied to the wait after each retry |
| `DISCOVERY_SERVICE_PUBLISH_MAX_INTERVAL_SECONDS` | `300` | Cap on the wait between retries |
| `DISCOVERY_SERVICE_PUBLISH_MAX_ATTEMPTS` | `5` | Total attempts (including the first) before the stage fails |

---

## Who consumes what

| Concern | FE | API | Worker |
|---------|----|-----|--------|
| `VITE_*` proxies & Keycloak browser login | ✅ | — | — |
| JWT validation (`AUTH_*`, `KEYCLOAK_*`) | — | ✅ | — |
| Temporal / MinIO / SQLite | — | ✅ | ✅ |
| OCR / translation / chunking / domain tags | — | config / status | ✅ runs jobs |
| Qdrant (`VECTOR_DB_*`) | — | ✅ | ✅ |
| Publish to Network (`PUBLISH_TO_NETWORK`, `DISCOVERY_SERVICE_ENDPOINT`, `NETWORK_SENDER_ID`) | — | ✅ `PUBLISH_TO_NETWORK` only (stage list) | ✅ runs the publish activity |

---

## Local non-Docker quick start

**Backend** (repo root):

```bash
set -a && source .env && set +a
export DOCUMENT_DB_PATH="$(pwd)/data/documents.db"
export ALLOWED_FILE_PATHS="$(pwd)/books,$(pwd)/data/documents"
export CORS_ORIGINS="http://localhost:3000,http://localhost:3001"
export PYTHONPATH="$(pwd)"
.venv/bin/uvicorn pipeline.api:app --host 0.0.0.0 --port 8001 --reload
```

**Frontend** (`ui/`):

```bash
# ui/.env should set VITE_API_PROXY_TARGET=http://localhost:8001
npm run dev -- --port 3001
```

With auth:

```bash
# ui/.env
VITE_AUTH_ENABLED=true
VITE_KEYCLOAK_URL=https://auth-vistaar.da.gov.in/auth
VITE_KEYCLOAK_REALM=bharat-vistaar
VITE_KEYCLOAK_CLIENT_ID=bharat-vistaar
VITE_KEYCLOAK_IDP_HINT=google

# root .env (API) — same realm as FE
AUTH_DISABLED=true
KEYCLOAK_ISSUER=https://auth-vistaar.da.gov.in/auth/realms/bharat-vistaar
KEYCLOAK_JWKS_URL=https://auth-vistaar.da.gov.in/auth/realms/bharat-vistaar/protocol/openid-connect/certs
KEYCLOAK_AUDIENCE=
```


---

## Files to keep in sync

| File | Role |
|------|------|
| `ENV.md` | This document (human-readable contract) |
| `.env.example` | Backend annotated template |
| `.env` | Local backend secrets (gitignored) |
| `ui/.env.example` | Frontend annotated template |
| `ui/.env` | Local frontend overrides (gitignored if configured) |
| `docker-compose.yml` | Compose-injected env for API, worker, UI |
