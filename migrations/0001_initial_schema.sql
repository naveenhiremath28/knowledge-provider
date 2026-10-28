CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE FUNCTION set_updated_at() RETURNS trigger AS $$
BEGIN
  NEW.updated_at := now();
  RETURN NEW;
END $$ LANGUAGE plpgsql;

-- 1. groups: one row per tenant node (provider), mirrored from Keycloak
CREATE TABLE groups (
  id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  keycloak_group_id TEXT UNIQUE,
  name             TEXT NOT NULL,
  path             TEXT NOT NULL UNIQUE,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TRIGGER trg_groups_updated_at BEFORE UPDATE ON groups
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- 2. users
CREATE TABLE users (
  id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  keycloak_user_id TEXT NOT NULL UNIQUE,
  username         TEXT,
  email            TEXT,
  roles            TEXT[] NOT NULL DEFAULT '{}',
  group_id         UUID REFERENCES groups(id) ON DELETE SET NULL,
  first_seen_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX users_email_lower_key ON users (lower(email)) WHERE email IS NOT NULL;

INSERT INTO users (keycloak_user_id, username, roles)
VALUES ('local-admin', 'local-admin', '{superadmin}');

-- 3. catalogs: one row per provider group and resource type
CREATE TABLE catalogs (
  id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  group_id   UUID NOT NULL REFERENCES groups(id) ON DELETE RESTRICT,
  type       TEXT NOT NULL CHECK (type ~ '^[a-z0-9]+:[A-Za-z0-9]+$'),
  name       TEXT NOT NULL,
  metadata   JSONB NOT NULL DEFAULT '{}' CHECK (jsonb_typeof(metadata) = 'object'),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (group_id, type)
);
CREATE TRIGGER trg_catalogs_updated_at BEFORE UPDATE ON catalogs
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- 4. documents
CREATE TABLE documents (
  id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  catalog_id  UUID NOT NULL REFERENCES catalogs(id) ON DELETE RESTRICT,
  code        TEXT NOT NULL CHECK (code ~ '^[a-z0-9]+(-[a-z0-9]+)*$'),
  name        TEXT NOT NULL,
  aliases     TEXT[] NOT NULL DEFAULT '{}' CHECK (array_position(aliases, '') IS NULL),
  source      JSONB NOT NULL
              CHECK (jsonb_typeof(source) = 'object')
              CHECK (source->>'fingerprint' ~ '^[0-9a-f]{32}$'),
  is_disabled BOOLEAN NOT NULL DEFAULT FALSE,
  metadata    JSONB NOT NULL
              DEFAULT '{"index_status": {}, "catalog_visible": true, "network_visible": true, "reindex_required": false}'
              CHECK (jsonb_typeof(metadata) = 'object'),
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (catalog_id, code)
);
CREATE INDEX documents_created_at_idx ON documents (created_at DESC);
CREATE INDEX documents_fingerprint_idx ON documents (catalog_id, (source->>'fingerprint'));
CREATE TRIGGER trg_documents_updated_at BEFORE UPDATE ON documents
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- 5. workflows
CREATE TABLE workflows (
  id                   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  document_id          UUID NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
  workflow_type        TEXT NOT NULL,
  temporal_workflow_id TEXT NOT NULL,
  status               TEXT NOT NULL DEFAULT 'running',
  current_stage        TEXT NOT NULL DEFAULT 'registered',
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

-- 6. workflow_artifacts
CREATE TABLE workflow_artifacts (
  id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  workflow_id   UUID NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
  artifact_type TEXT NOT NULL CHECK (btrim(artifact_type) <> ''),
  stage         TEXT,
  storage_uri   TEXT NOT NULL,
  mime_type     TEXT,
  filename      TEXT,
  size_bytes    BIGINT CHECK (size_bytes >= 0),
  metadata      JSONB NOT NULL DEFAULT '{}' CHECK (jsonb_typeof(metadata) = 'object'),
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX workflow_artifacts_workflow_idx ON workflow_artifacts (workflow_id, created_at DESC);
CREATE INDEX workflow_artifacts_type_idx ON workflow_artifacts (workflow_id, artifact_type, created_at DESC);

-- 7. pages
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

-- 8. chunks
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

-- 9. audit_logs
CREATE TABLE audit_logs (
  id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  document_id   UUID REFERENCES documents(id) ON DELETE SET NULL,
  workflow_id   TEXT,
  group_id      UUID REFERENCES groups(id) ON DELETE SET NULL,
  action_type   TEXT NOT NULL CHECK (btrim(action_type) <> ''),
  entity_type   TEXT,
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
CREATE INDEX audit_logs_group_idx ON audit_logs (group_id, created_at DESC);
CREATE INDEX audit_logs_created_idx ON audit_logs (created_at DESC);
CREATE INDEX audit_logs_action_idx ON audit_logs (action_type, created_at DESC);
CREATE INDEX audit_logs_settings_idx ON audit_logs (created_at DESC) WHERE entity_type = 'setting';

-- 10. search_settings
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

-- 11. email_otps
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

-- catalog_status view: one row per document, computed from catalogs, documents and workflows
CREATE VIEW catalog_status AS
SELECT
  d.id AS document_id,
  c.id AS catalog_id,
  c.group_id,
  g.name AS group_name,
  c.type,
  d.code,
  d.name,
  d.aliases,
  CASE
    WHEN x.is_live AND COALESCE((d.metadata->>'reindex_required')::boolean, FALSE) THEN 'pending_reindex'
    WHEN x.is_live THEN 'live'
    WHEN cw.current_stage = 'approval_for_prod' AND NOT d.is_disabled THEN 'pending_prod'
    ELSE 'disabled'
  END AS status,
  (SELECT w.temporal_workflow_id FROM workflows w
    WHERE w.document_id = d.id AND w.workflow_type = 'pipeline'
    ORDER BY w.started_at ASC, w.id ASC LIMIT 1) AS workflow_id,
  (SELECT count(*) FROM chunks ch WHERE ch.document_id = d.id)::INTEGER AS chunk_count,
  COALESCE((d.metadata->>'network_visible')::boolean, TRUE) AS network_visible,
  CASE WHEN x.is_live
       THEN (d.metadata->'index_status'->'prod'->>'last_indexed_at')::timestamptz END AS promoted_at
FROM documents d
JOIN catalogs c ON c.id = d.catalog_id
JOIN groups g ON g.id = c.group_id
LEFT JOIN document_current_workflow cw ON cw.document_id = d.id
CROSS JOIN LATERAL (
  SELECT (cw.current_stage = 'completed' AND NOT d.is_disabled
          AND COALESCE((d.metadata->>'catalog_visible')::boolean, TRUE)) AS is_live
) x;
