CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE FUNCTION set_updated_at() RETURNS trigger AS $$
BEGIN
  NEW.updated_at := now();
  RETURN NEW;
END $$ LANGUAGE plpgsql;

CREATE TYPE workflow_stage AS ENUM (
  'registered', 'ocr_processing', 'ocr_review', 'translation_processing',
  'translation_review', 'chunking', 'chunk_review', 'ready_for_ingestion',
  'ingesting', 'publishing_to_network', 'approval_for_prod', 'ingesting_prod',
  'completed', 'failed'
);
CREATE TYPE workflow_type_enum AS ENUM (
  'pipeline', 'ocr_only', 'ocr_retry', 'translation_retry', 'chunking_retry',
  'reingest', 'promote_prod'
);
CREATE TYPE workflow_status_enum AS ENUM ('running', 'waiting_review', 'completed', 'failed');
CREATE TYPE audit_entity_type AS ENUM ('document', 'page', 'chunk', 'setting', 'catalog');

-- 1. users
CREATE TABLE users (
  id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  keycloak_user_id TEXT NOT NULL UNIQUE,
  username         TEXT,
  email            TEXT,
  roles            TEXT[] NOT NULL DEFAULT '{}',
  first_seen_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX users_email_lower_key ON users (lower(email)) WHERE email IS NOT NULL;

INSERT INTO users (keycloak_user_id, username, roles)
VALUES ('local-admin', 'local-admin', '{superadmin}');

-- 2. catalog_entries
CREATE TABLE catalog_entries (
  id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  type       TEXT NOT NULL CHECK (type ~ '^[a-z0-9]+([_-][a-z0-9]+)*$'),
  code       TEXT NOT NULL CHECK (code ~ '^[a-z0-9]+(-[a-z0-9]+)*$'),
  name       TEXT NOT NULL,
  aliases    TEXT[] NOT NULL DEFAULT '{}' CHECK (array_position(aliases, '') IS NULL),
  metadata   JSONB NOT NULL DEFAULT '{"source": "pipeline"}' CHECK (jsonb_typeof(metadata) = 'object'),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (type, code),
  UNIQUE (id, type)
);
CREATE TRIGGER trg_catalog_entries_updated_at BEFORE UPDATE ON catalog_entries
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- 3. documents
CREATE TABLE documents (
  id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  instance            TEXT NOT NULL,
  source              JSONB NOT NULL
                      CHECK (jsonb_typeof(source) = 'object')
                      CHECK (source->>'fingerprint' ~ '^[0-9a-f]{32}$'),
  document_kind       TEXT NOT NULL DEFAULT 'document' CHECK (document_kind ~ '^[a-z0-9]+([_-][a-z0-9]+)*$'),
  catalog_entry_id    UUID,
  valid_from          DATE,
  valid_to            DATE CHECK (valid_from <= valid_to),
  is_disabled         BOOLEAN NOT NULL DEFAULT FALSE,
  index_status        JSONB NOT NULL DEFAULT '{}' CHECK (jsonb_typeof(index_status) = 'object'),
  metadata            JSONB NOT NULL
                      DEFAULT '{"catalog_visible": true, "network_visible": true, "is_demo": false, "chunk_count": 0, "reindex_required": false}'
                      CHECK (jsonb_typeof(metadata) = 'object')
                      CHECK ((metadata->>'chunk_count')::int >= 0),
  uploaded_by_user_id UUID REFERENCES users(id),
  created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  FOREIGN KEY (catalog_entry_id, document_kind) REFERENCES catalog_entries (id, type)
    ON UPDATE CASCADE ON DELETE RESTRICT
);
CREATE INDEX documents_instance_idx ON documents (instance);
CREATE INDEX documents_created_at_idx ON documents (created_at DESC);
CREATE INDEX documents_catalog_entry_idx ON documents (catalog_entry_id) WHERE catalog_entry_id IS NOT NULL;
CREATE INDEX documents_fingerprint_idx ON documents (instance, (source->>'fingerprint'));
CREATE INDEX documents_is_demo_idx ON documents (((metadata->>'is_demo')::boolean));
CREATE TRIGGER trg_documents_updated_at BEFORE UPDATE ON documents
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- 4. workflows
CREATE TABLE workflows (
  id                   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  document_id          UUID NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
  workflow_type        workflow_type_enum NOT NULL,
  temporal_workflow_id TEXT NOT NULL,
  status               workflow_status_enum NOT NULL DEFAULT 'running',
  current_stage        workflow_stage NOT NULL DEFAULT 'registered',
  stage_timestamps     JSONB NOT NULL DEFAULT '{}'
                       CHECK (jsonb_typeof(stage_timestamps) = 'object'
                              AND (stage_timestamps - ARRAY['ocr', 'translation', 'chunks']) = '{}'::jsonb),
  stop_after_ocr       BOOLEAN NOT NULL DEFAULT FALSE,
  error_message        TEXT,
  config               JSONB NOT NULL DEFAULT '{}' CHECK (jsonb_typeof(config) = 'object'),
  started_by_user_id   UUID REFERENCES users(id) ON DELETE SET NULL,
  started_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
  completed_at         TIMESTAMPTZ,
  updated_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX workflows_document_started_idx ON workflows (document_id, started_at DESC);
CREATE INDEX workflows_status_started_idx ON workflows (status, started_at DESC);
CREATE INDEX workflows_temporal_id_idx ON workflows (temporal_workflow_id);
CREATE TRIGGER trg_workflows_updated_at BEFORE UPDATE ON workflows
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE VIEW document_current_workflow AS
SELECT DISTINCT ON (document_id) *
FROM workflows
ORDER BY document_id, started_at DESC, id DESC;

-- 5. workflow_artifacts
CREATE TABLE workflow_artifacts (
  id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  workflow_id   UUID NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
  artifact_type TEXT NOT NULL CHECK (btrim(artifact_type) <> ''),
  stage         workflow_stage,
  storage_uri   TEXT NOT NULL,
  mime_type     TEXT,
  filename      TEXT,
  size_bytes    BIGINT CHECK (size_bytes >= 0),
  metadata      JSONB NOT NULL DEFAULT '{}' CHECK (jsonb_typeof(metadata) = 'object'),
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX workflow_artifacts_workflow_idx ON workflow_artifacts (workflow_id, created_at DESC);
CREATE INDEX workflow_artifacts_type_idx ON workflow_artifacts (workflow_id, artifact_type, created_at DESC);

-- 6. pages
CREATE TABLE pages (
  id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  document_id       UUID NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
  page_number       INTEGER NOT NULL CHECK (page_number >= 1),
  original_markdown TEXT,
  edited_markdown   TEXT,
  translations      JSONB NOT NULL DEFAULT '{}' CHECK (jsonb_typeof(translations) = 'object'),
  metadata          JSONB NOT NULL DEFAULT '{"is_reviewed": false}' CHECK (jsonb_typeof(metadata) = 'object'),
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (document_id, page_number)
);
CREATE TRIGGER trg_pages_updated_at BEFORE UPDATE ON pages
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- 7. chunks
CREATE TABLE chunks (
  id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  document_id    UUID NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
  chunk_number   INTEGER NOT NULL CHECK (chunk_number >= 1),
  original_text  TEXT NOT NULL,
  edited_text    TEXT,
  token_count    INTEGER NOT NULL DEFAULT 0 CHECK (token_count >= 0),
  page_start     INTEGER NOT NULL DEFAULT 1 CHECK (page_start >= 1),
  page_end       INTEGER NOT NULL DEFAULT 1,
  source_spans   JSONB NOT NULL DEFAULT '[]' CHECK (jsonb_typeof(source_spans) = 'array'),
  section_title  TEXT,
  content_type   TEXT,
  is_reference   BOOLEAN NOT NULL DEFAULT FALSE,
  is_reviewed    BOOLEAN NOT NULL DEFAULT FALSE,
  is_excluded    BOOLEAN NOT NULL DEFAULT FALSE,
  reviewer_notes TEXT,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (document_id, chunk_number),
  CHECK (page_end >= page_start)
);
CREATE INDEX chunks_text_trgm_idx ON chunks
  USING gin (lower(coalesce(edited_text, original_text)) gin_trgm_ops);
CREATE TRIGGER trg_chunks_updated_at BEFORE UPDATE ON chunks
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- 8. audit_logs
CREATE TABLE audit_logs (
  id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  document_id   UUID REFERENCES documents(id) ON DELETE SET NULL,
  workflow_id   TEXT,
  instance      TEXT,
  action_type   TEXT NOT NULL CHECK (btrim(action_type) <> ''),
  entity_type   audit_entity_type,
  entity_number INTEGER,
  field_name    TEXT,
  old_value     TEXT,
  new_value     TEXT,
  metadata      JSONB NOT NULL DEFAULT '{}' CHECK (jsonb_typeof(metadata) = 'object'),
  actor_user_id UUID REFERENCES users(id) ON DELETE SET NULL,
  actor         JSONB NOT NULL DEFAULT '{}' CHECK (jsonb_typeof(actor) = 'object'),
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK (entity_type IN ('setting', 'catalog') OR workflow_id IS NOT NULL)
);
CREATE INDEX audit_logs_document_idx ON audit_logs (document_id, created_at DESC);
CREATE INDEX audit_logs_created_idx ON audit_logs (created_at DESC);
CREATE INDEX audit_logs_action_idx ON audit_logs (action_type, created_at DESC);
CREATE INDEX audit_logs_settings_idx ON audit_logs (created_at DESC) WHERE entity_type = 'setting';

-- 9. search_settings
CREATE TABLE search_settings (
  id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  singleton  BOOLEAN NOT NULL DEFAULT TRUE UNIQUE CHECK (singleton),
  retrieval  JSONB NOT NULL DEFAULT '{}' CHECK (jsonb_typeof(retrieval) = 'object'),
  candidates JSONB NOT NULL DEFAULT '{}' CHECK (jsonb_typeof(candidates) = 'object'),
  results    JSONB NOT NULL DEFAULT '{}' CHECK (jsonb_typeof(results) = 'object'),
  query      JSONB NOT NULL DEFAULT '{}' CHECK (jsonb_typeof(query) = 'object'),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TRIGGER trg_search_settings_updated_at BEFORE UPDATE ON search_settings
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();
INSERT INTO search_settings DEFAULT VALUES;

-- 10. email_otps
CREATE TABLE email_otps (
  id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  email        TEXT NOT NULL UNIQUE CHECK (email = lower(email)),
  code_hash    TEXT NOT NULL,
  expires_at   TIMESTAMPTZ NOT NULL,
  attempts     INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
  last_sent_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX email_otps_expires_idx ON email_otps (expires_at);

-- webhook_records
CREATE TABLE webhook_records (
  id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  source       TEXT NOT NULL DEFAULT 'webhook',
  external_id  TEXT NOT NULL,
  title        TEXT,
  slug         TEXT,
  status       TEXT,
  is_published BOOLEAN,
  category     TEXT,
  payload      JSONB NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
  payload_hash TEXT,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (source, external_id)
);
CREATE INDEX webhook_records_source_status_idx ON webhook_records (source, status);

-- catalog view
CREATE VIEW catalog AS
SELECT
  e.id AS entry_id,
  e.type,
  e.code,
  e.name,
  e.aliases,
  e.metadata->>'source' AS source,
  CASE
    WHEN COALESCE(a.has_pending_reindex, FALSE) THEN 'pending_reindex'
    WHEN COALESCE(a.live_count, 0) > 0 THEN 'live'
    WHEN COALESCE(a.pending_prod_count, 0) > 0 THEN 'pending_prod'
    WHEN COALESCE(a.doc_count, 0) = 0 AND e.metadata->>'source' = 'bootstrap' THEN 'live'
    ELSE 'disabled'
  END AS status,
  CASE WHEN COALESCE(cardinality(a.instances), 0) = 0 THEN ARRAY['default']::TEXT[] ELSE a.instances END AS instances,
  COALESCE(a.workflow_ids, '{}'::TEXT[]) AS workflow_ids,
  COALESCE(a.chunk_count, 0)::INTEGER AS chunk_count,
  CASE
    WHEN COALESCE(a.doc_count, 0) = 0 THEN COALESCE(e.metadata->>'source' = 'bootstrap', FALSE)
    ELSE COALESCE(a.network_visible, FALSE)
  END AS network_visible,
  a.promoted_at
FROM catalog_entries e
LEFT JOIN LATERAL (
  SELECT
    COUNT(*) AS doc_count,
    COUNT(*) FILTER (WHERE x.is_live) AS live_count,
    BOOL_OR(x.is_live AND COALESCE((x.metadata->>'reindex_required')::boolean, FALSE)) AS has_pending_reindex,
    COUNT(*) FILTER (WHERE x.current_stage = 'approval_for_prod' AND NOT x.is_disabled) AS pending_prod_count,
    ARRAY(SELECT DISTINCT d2.instance FROM documents d2
          WHERE d2.catalog_entry_id = e.id ORDER BY d2.instance) AS instances,
    ARRAY(SELECT w.temporal_workflow_id FROM documents d3
          JOIN LATERAL (
            SELECT w2.temporal_workflow_id FROM workflows w2
            WHERE w2.document_id = d3.id AND w2.workflow_type = 'pipeline'
            ORDER BY w2.started_at ASC, w2.id ASC LIMIT 1
          ) w ON TRUE
          WHERE d3.catalog_entry_id = e.id
          ORDER BY d3.created_at, d3.id) AS workflow_ids,
    COALESCE(SUM((x.metadata->>'chunk_count')::int), 0) AS chunk_count,
    BOOL_OR(COALESCE((x.metadata->>'network_visible')::boolean, TRUE)) AS network_visible,
    MAX((x.index_status->'prod'->>'last_indexed_at')::timestamptz) FILTER (WHERE x.is_live) AS promoted_at
  FROM (
    SELECT d.*, cw.current_stage,
           (cw.current_stage = 'completed' AND NOT d.is_disabled
            AND COALESCE((d.metadata->>'catalog_visible')::boolean, TRUE)) AS is_live
    FROM documents d
    LEFT JOIN document_current_workflow cw ON cw.document_id = d.id
    WHERE d.catalog_entry_id = e.id
  ) x
) a ON TRUE;
