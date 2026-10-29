"""
Postgres persistence for documents, workflows, pages, chunks, audit and settings.

Schema: ``migrations/`` (see docs/POSTGRES_SCHEMA.md). One function per query.

A document is content and metadata; a workflow is one processing run of a
document. Callers still address a document by ``workflow_id`` - the Temporal
workflow id of its main run (``doc-<md5>``) - and every function here also
accepts the document UUID, the fingerprint or the filename (see
``find_document_by_doc_identifier``). Documents, jobs and artifacts come back
as flat dicts so call sites need not know how the tables are split; ``id``
carries the UUID.
"""

import hashlib
import json
import logging
import os
import re
import threading
import uuid
from contextlib import contextmanager
from datetime import date, datetime, timezone
from typing import Any, Optional

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool

from . import migrate
from .models import (
    CandidateSettings,
    ChunkingRun,
    DocumentMetadata,
    DocumentSource,
    IndexState,
    PageMetadata,
    PageTranslation,
    QuerySettings,
    ResultSettings,
    RetrievalSettings,
)
from .network_constants import KIND_TO_CATALOG_TYPE, catalog_type_for_kind, kind_for_catalog_type

logger = logging.getLogger(__name__)

DEFAULT_DATABASE_URL = "postgresql://postgres:postgres@localhost:5432/knowledge_provider"

_pool: Optional[ConnectionPool] = None
_pool_url: Optional[str] = None
_pool_lock = threading.Lock()


def database_url() -> str:
    return os.environ.get("DATABASE_URL") or DEFAULT_DATABASE_URL


def get_pool() -> ConnectionPool:
    global _pool, _pool_url
    url = database_url()
    if _pool is not None and _pool_url == url:
        return _pool
    with _pool_lock:
        if _pool is not None and _pool_url != url:
            _pool.close()
            _pool = None
        if _pool is None:
            pool = ConnectionPool(
                conninfo=url,
                min_size=int(os.environ.get("DB_POOL_MIN_SIZE", "1")),
                max_size=int(os.environ.get("DB_POOL_MAX_SIZE", "10")),
                timeout=float(os.environ.get("DB_POOL_TIMEOUT_SECONDS", "30")),
                kwargs={"row_factory": dict_row, "options": "-c timezone=UTC"},
                open=False,
            )
            pool.open(wait=True)
            _pool = pool
            _pool_url = url
            logger.info("db pool opened: %s", pool.get_stats())
    return _pool


def close_pool() -> None:
    global _pool, _pool_url
    with _pool_lock:
        if _pool is not None:
            _pool.close()
        _pool = None
        _pool_url = None


@contextmanager
def get_connection():
    """A pooled connection; the transaction commits on exit and rolls back on error."""
    with get_pool().connection() as conn:
        yield conn


def init_db() -> None:
    """Apply pending migrations and open the connection pool."""
    applied = migrate.apply_migrations(database_url())
    current = migrate.current_version(database_url())
    logger.info("db migrations current=%s applied_now=%s", current, applied)
    get_pool()
    created = sync_catalogs_for_all_groups()
    if created:
        logger.info("catalog sync created %d missing catalogs", created)


def check_health() -> dict:
    """Reachability and migration state, for the /health endpoint."""
    try:
        with get_connection() as conn:
            conn.execute("SELECT 1")
        return {"reachable": True, "migrations_current": migrate.is_current(database_url())}
    except Exception:
        logger.exception("database health check failed")
        return {"reachable": False, "migrations_current": False}


# =============================================================================
# Helpers
# =============================================================================

_UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)
_FINGERPRINT_RE = re.compile(r"^[0-9a-f]{32}$")

_REVIEW_STAGES = {
    "ocr_review",
    "translation_review",
    "chunk_review",
    "ready_for_ingestion",
    "approval_for_prod",
}

PROD_INDEX_KEY = "prod"
DEV_INDEX_KEY = "dev"


def _default_group() -> str:
    return (os.environ.get("DEFAULT_INSTANCE") or "default").strip().lower() or "default"


def _normalize_groups(groups: list[str]) -> list[str]:
    default = _default_group()
    return sorted({(i or "").strip().lower() or default for i in groups})


def _iso_now() -> str:
    return datetime.now(timezone.utc).replace(tzinfo=None).isoformat()


def _is_uuid(value: Any) -> bool:
    return isinstance(value, str) and bool(_UUID_RE.match(value))


def _clean(value: Any) -> Any:
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            value = value.astimezone(timezone.utc).replace(tzinfo=None)
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return value


def _clean_row(row: Optional[dict]) -> Optional[dict]:
    if row is None:
        return None
    return {k: _clean(v) for k, v in row.items()}


def _csv_roles(roles: Any) -> list[str]:
    if roles is None:
        return []
    if isinstance(roles, (list, tuple, set)):
        return [str(r).strip() for r in roles if str(r).strip()]
    return [part.strip() for part in str(roles).split(",") if part.strip()]


def _fingerprint_for(
    source_file_fingerprint: Optional[str],
    document_id: Optional[str],
    workflow_id: Optional[str],
) -> str:
    for candidate in (source_file_fingerprint, document_id):
        if candidate and _FINGERPRINT_RE.match(candidate):
            return candidate
    return hashlib.md5((document_id or workflow_id or "").encode()).hexdigest()


def _prod_collection_names() -> set[str]:
    names = {
        os.environ.get("PROD_VECTOR_DB_COLLECTION_NAME"),
        os.environ.get("PROD_SCHEME_VECTOR_DB_COLLECTION_NAME"),
    }
    return {n.strip() for n in names if n and n.strip()}


def _index_key_for(index_name: str) -> str:
    return PROD_INDEX_KEY if index_name in _prod_collection_names() else DEV_INDEX_KEY


# =============================================================================
# Identifier resolution
# =============================================================================

def _resolve_document_uuid(conn, identifier: Optional[str]) -> Optional[str]:
    """Document UUID for a UUID, Temporal workflow id, fingerprint or filename."""
    if not identifier:
        return None
    identifier = str(identifier)

    if _is_uuid(identifier):
        row = conn.execute(
            "SELECT id FROM documents WHERE id = %s::uuid", (identifier,)
        ).fetchone()
        if row:
            return str(row["id"])

    row = conn.execute(
        """
        SELECT document_id FROM workflows
        WHERE temporal_workflow_id = %s
        ORDER BY started_at DESC LIMIT 1
        """,
        (identifier,),
    ).fetchone()
    if row:
        return str(row["document_id"])

    row = conn.execute(
        """
        SELECT id FROM documents
        WHERE source->>'fingerprint' = %s OR source->>'original_filename' = %s
        ORDER BY created_at DESC LIMIT 1
        """,
        (identifier, identifier),
    ).fetchone()
    if row:
        return str(row["id"])

    row = conn.execute(
        """
        SELECT id FROM documents d
        WHERE EXISTS (
            SELECT 1 FROM jsonb_each(COALESCE(d.metadata->'index_status', '{}'::jsonb)) e
            WHERE e.value->>'doc_id' = %s
        ) OR md5(d.source->>'fingerprint') = %s
        ORDER BY created_at DESC LIMIT 1
        """,
        (identifier, identifier),
    ).fetchone()
    return str(row["id"]) if row else None


def _require_document_uuid(conn, identifier: str) -> str:
    doc_uuid = _resolve_document_uuid(conn, identifier)
    if doc_uuid is None:
        raise LookupError(f"Document not found: {identifier}")
    return doc_uuid


# =============================================================================
# Document row <-> flat dict
# =============================================================================

_DOC_SELECT = """
    SELECT
        d.id, d.catalog_id, d.code, d.name, d.aliases, d.source, d.is_disabled, d.metadata,
        d.created_at, d.updated_at,
        c.type AS catalog_type, g.name AS group_name,
        (SELECT count(*) FROM pages p WHERE p.document_id = d.id) AS page_total,
        (SELECT count(*) FROM chunks ch WHERE ch.document_id = d.id) AS chunk_total,
        root.temporal_workflow_id AS root_workflow_id,
        root.stop_after_ocr AS root_stop_after_ocr,
        cw.id AS current_workflow_pk,
        cw.current_stage AS current_stage,
        cw.error_message AS current_error_message,
        u.keycloak_user_id AS up_user_id, u.username AS up_username,
        u.email AS up_email, u.roles AS up_roles,
        ts.ocr_completed_at, ts.translation_completed_at, ts.chunks_completed_at,
        ts.ingested_at,
        req.started_at AS prod_ready_requested_at,
        ru.keycloak_user_id AS prod_ready_requested_by_user_id,
        ru.username AS prod_ready_requested_by_username,
        oa.id AS original_artifact_id,
        na.id AS normalized_artifact_id
    FROM documents d
    JOIN catalogs c ON c.id = d.catalog_id
    JOIN groups g ON g.id = c.group_id
    LEFT JOIN document_current_workflow cw ON cw.document_id = d.id
    LEFT JOIN LATERAL (
        SELECT w.temporal_workflow_id, w.stop_after_ocr FROM workflows w
        WHERE w.document_id = d.id
        ORDER BY (w.workflow_type IN ('pipeline', 'ocr_only')) DESC, w.started_at ASC, w.id ASC
        LIMIT 1
    ) root ON TRUE
    LEFT JOIN users u ON u.id = NULLIF(d.metadata->>'uploaded_by_user_id', '')::uuid
    LEFT JOIN LATERAL (
        SELECT min(w.stage_timestamps->>'ocr') AS ocr_completed_at,
               min(w.stage_timestamps->>'translation') AS translation_completed_at,
               min(w.stage_timestamps->>'chunks') AS chunks_completed_at,
               max(COALESCE(w.completed_at, w.updated_at))
                   FILTER (WHERE w.current_stage = 'completed') AS ingested_at
        FROM workflows w WHERE w.document_id = d.id
    ) ts ON TRUE
    LEFT JOIN LATERAL (
        SELECT w.started_at, w.started_by_user_id FROM workflows w
        WHERE w.document_id = d.id AND w.workflow_type = 'promote_prod'
          AND w.status = 'waiting_review'
        ORDER BY w.started_at DESC LIMIT 1
    ) req ON TRUE
    LEFT JOIN users ru ON ru.id = req.started_by_user_id
    LEFT JOIN LATERAL (
        SELECT a.id FROM workflow_artifacts a JOIN workflows w ON w.id = a.workflow_id
        WHERE w.document_id = d.id AND a.artifact_type = 'original_upload'
        ORDER BY a.created_at DESC, a.id DESC LIMIT 1
    ) oa ON TRUE
    LEFT JOIN LATERAL (
        SELECT a.id FROM workflow_artifacts a JOIN workflows w ON w.id = a.workflow_id
        WHERE w.document_id = d.id
          AND a.artifact_type IN ('normalized_pdf', 'normalized_spreadsheet')
        ORDER BY a.created_at DESC, a.id DESC LIMIT 1
    ) na ON TRUE
"""


def _kind_of(catalog_type: Optional[str]) -> Optional[str]:
    """Knowledge kind a catalog type stands for; an unmapped type is returned as is."""
    if not catalog_type:
        return None
    try:
        return kind_for_catalog_type(catalog_type)
    except ValueError:
        return catalog_type


def _document_from_row(row: Optional[dict]) -> Optional[dict]:
    if row is None:
        return None
    source = row["source"] or {}
    meta = row["metadata"] or {}
    fingerprint = source.get("fingerprint")
    roles = row["up_roles"] or []
    entry_aliases = row["aliases"]
    doc = {
        "id": row["id"],
        "workflow_id": row["root_workflow_id"],
        "document_id": fingerprint,
        "canonical_document_id": fingerprint,
        "filename": source.get("original_filename") or "",
        "name": row["name"],
        "source_filename": source.get("original_filename"),
        "source_manifest_name": None,
        "source_file_fingerprint": fingerprint,
        "filepath": source.get("storage_uri") or "",
        "group": (row["group_name"] or "").lower(),
        "stage": row["current_stage"] or "registered",
        "page_count": int(row["page_total"] or 0),
        "chunk_count": int(row["chunk_total"] or 0),
        "error_message": row["current_error_message"],
        "is_disabled": bool(row["is_disabled"]),
        "source_type": source.get("source_type"),
        "canonical_input_type": source.get("canonical_input_type"),
        "stop_after_ocr": bool(row["root_stop_after_ocr"]),
        "reindex_required": bool(meta.get("reindex_required")),
        "reindex_reason": meta.get("reindex_reason"),
        "kind": _kind_of(row["catalog_type"]),
        "code": row["code"],
        "aliases": list(entry_aliases or []),
        "tool_routing": "qdrant",
        "catalog_visible": bool(meta.get("catalog_visible", True)),
        "network_visible": bool(meta.get("network_visible", True)),
        "valid_from": meta.get("valid_from"),
        "valid_to": meta.get("valid_to"),
        "uploaded_by_user_id": row["up_user_id"],
        "uploaded_by_username": row["up_username"],
        "uploaded_by_email": row["up_email"],
        "uploaded_by_roles": ",".join(roles) if roles else None,
        "prod_ready_requested_at": row["prod_ready_requested_at"],
        "prod_ready_requested_by_user_id": row["prod_ready_requested_by_user_id"],
        "prod_ready_requested_by_username": row["prod_ready_requested_by_username"],
        "ocr_completed_at": row["ocr_completed_at"],
        "translation_completed_at": row["translation_completed_at"],
        "chunks_completed_at": row["chunks_completed_at"],
        "ingested_at": row["ingested_at"],
        "original_artifact_id": row["original_artifact_id"],
        "normalized_artifact_id": row["normalized_artifact_id"],
        "latest_job_id": row["current_workflow_pk"],
        "catalog_id": row["catalog_id"],
        "index_status": meta.get("index_status") or {},
        "superseded_doc_ids": meta.get("superseded_doc_ids") or {},
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }
    return {k: _clean(v) for k, v in doc.items()}


def _fetch_document(conn, doc_uuid: Optional[str]) -> Optional[dict]:
    if doc_uuid is None:
        return None
    row = conn.execute(_DOC_SELECT + " WHERE d.id = %s::uuid", (doc_uuid,)).fetchone()
    return _document_from_row(row)


def _document_filters(
    include_disabled: bool,
    groups: Optional[list[str]],
    alias: str = "d",
    group_alias: str = "g",
) -> tuple[list[str], list]:
    """WHERE clauses for document lists. The query must join ``groups`` as ``group_alias``."""
    clauses: list[str] = []
    params: list = []
    if not include_disabled:
        clauses.append(f"NOT {alias}.is_disabled")
    if groups is not None:
        clauses.append(f"lower({group_alias}.name) = ANY(%s)")
        params.append(_normalize_groups(groups))
    return clauses, params


# =============================================================================
# Groups
# =============================================================================

def _tenant_path(group: str) -> str:
    """Keycloak group path of a tenant code: ``bv`` is ``/global``, others ``/states/<CODE>``."""
    return "/global" if group == "bv" else f"/states/{group.upper()}"


def _tenant_code_from_path(path: str) -> str:
    return "bv" if path == "/global" else path.rstrip("/").rsplit("/", 1)[-1].lower()


def upsert_group(path: str, keycloak_group_id: Optional[str] = None, conn=None) -> str:
    """Create or refresh the ``groups`` row for a Keycloak group path; returns its UUID.

    Rows made by the SQLite move have no Keycloak id yet; the first sync that
    sees the path fills it in. The group's catalogs (one per supported type) are
    ensured in the same transaction, so a group never exists without them.
    """
    if conn is None:
        with get_connection() as own:
            return upsert_group(path, keycloak_group_id, conn=own)
    row = conn.execute(
        """
        INSERT INTO groups (keycloak_group_id, name, path) VALUES (%s, %s, %s)
        ON CONFLICT (path) DO UPDATE SET
            keycloak_group_id = COALESCE(EXCLUDED.keycloak_group_id, groups.keycloak_group_id)
        RETURNING id
        """,
        (keycloak_group_id, _tenant_code_from_path(path), path),
    ).fetchone()
    group_id = str(row["id"])
    ensure_group_catalogs(group_id, conn=conn)
    return group_id


def get_or_create_group(group: Optional[str], conn=None) -> str:
    """UUID of the group for a tenant code (``bv``, ``mh``, ...), created when missing."""
    code = (group or _default_group()).strip().lower() or _default_group()
    return upsert_group(_tenant_path(code), conn=conn)


def _catalog_seed_rows() -> tuple[list[str], list[str]]:
    """Parallel ``(types, kinds)`` lists for every supported catalog type."""
    kinds = list(KIND_TO_CATALOG_TYPE)
    return [KIND_TO_CATALOG_TYPE[k] for k in kinds], kinds


def ensure_group_catalogs(group_id: str, conn=None) -> None:
    """Create the group's missing catalogs, one per supported type; existing rows are untouched."""
    if conn is None:
        with get_connection() as own:
            return ensure_group_catalogs(group_id, conn=own)
    types, kinds = _catalog_seed_rows()
    conn.execute(
        """
        INSERT INTO catalogs (group_id, type, name)
        SELECT g.id, t.type, upper(g.name) || ' ' || t.kind || ' catalog'
        FROM groups g, unnest(%s::text[], %s::text[]) AS t(type, kind)
        WHERE g.id = %s::uuid
        ON CONFLICT (group_id, type) DO NOTHING
        """,
        (types, kinds, group_id),
    )


def sync_catalogs_for_all_groups(conn=None) -> int:
    """Add every missing ``(group, type)`` catalog; returns how many rows were created.

    Idempotent. Run at startup so a type added to the kind map reaches groups
    that already exist.
    """
    if conn is None:
        with get_connection() as own:
            return sync_catalogs_for_all_groups(conn=own)
    types, kinds = _catalog_seed_rows()
    cur = conn.execute(
        """
        INSERT INTO catalogs (group_id, type, name)
        SELECT g.id, t.type, upper(g.name) || ' ' || t.kind || ' catalog'
        FROM groups g, unnest(%s::text[], %s::text[]) AS t(type, kind)
        ON CONFLICT (group_id, type) DO NOTHING
        """,
        (types, kinds),
    )
    return cur.rowcount


def get_or_create_catalog(group_id: str, kind: str, conn=None) -> dict:
    """The ``(group, kind)`` catalog row; created when missing (normally seeded with the group)."""
    if conn is None:
        with get_connection() as own:
            return get_or_create_catalog(group_id, kind, conn=own)
    catalog_type = catalog_type_for_kind(kind)
    row = conn.execute(
        """
        INSERT INTO catalogs (group_id, type, name)
        SELECT g.id, %s, upper(g.name) || ' ' || %s || ' catalog' FROM groups g WHERE g.id = %s::uuid
        ON CONFLICT (group_id, type) DO UPDATE SET type = EXCLUDED.type
        RETURNING id, group_id, type, name, metadata
        """,
        (catalog_type, kind, group_id),
    ).fetchone()
    return _clean_row(row)


# =============================================================================
# Users
# =============================================================================

def upsert_user(
    keycloak_user_id: str,
    username: Optional[str] = None,
    email: Optional[str] = None,
    roles: Any = None,
    conn=None,
    tenants: Optional[list[str]] = None,
) -> Optional[str]:
    """Create or refresh the ``users`` row for a Keycloak subject; returns its UUID.

    ``tenants`` are the tenant codes in the user's token. A user has one
    ``group_id``, so the first code (sorted) wins and a warning is logged when
    there are several. Authorization still reads the token, not this column.
    """
    keycloak_user_id = (keycloak_user_id or "").strip()
    if not keycloak_user_id:
        return None
    if conn is None:
        with get_connection() as own:
            return upsert_user(keycloak_user_id, username, email, roles, conn=own, tenants=tenants)

    role_list = _csv_roles(roles)
    tenant_codes = sorted({(t or "").strip().lower() for t in (tenants or []) if (t or "").strip()})
    if len(tenant_codes) > 1:
        logger.warning(
            "user %s has several tenants %s; users.group_id uses %s",
            keycloak_user_id, tenant_codes, tenant_codes[0],
        )
    group_uuid = get_or_create_group(tenant_codes[0], conn=conn) if tenant_codes else None
    email = (email or "").strip() or None
    username = (username or "").strip() or None
    for candidate_email in ([email, None] if email else [None]):
        try:
            with conn.transaction():
                row = conn.execute(
                    """
                    INSERT INTO users (keycloak_user_id, username, email, roles, group_id)
                    VALUES (%s, %s, %s, %s, %s::uuid)
                    ON CONFLICT (keycloak_user_id) DO UPDATE SET
                        username = COALESCE(EXCLUDED.username, users.username),
                        email = COALESCE(EXCLUDED.email, users.email),
                        roles = CASE WHEN cardinality(EXCLUDED.roles) > 0
                                     THEN EXCLUDED.roles ELSE users.roles END,
                        group_id = COALESCE(EXCLUDED.group_id, users.group_id),
                        last_seen_at = now()
                    RETURNING id
                    """,
                    (keycloak_user_id, username, candidate_email, role_list, group_uuid),
                ).fetchone()
            return str(row["id"])
        except psycopg.errors.UniqueViolation:
            continue
    return None


def get_user_by_keycloak_id(keycloak_user_id: str) -> Optional[dict]:
    with get_connection() as conn:
        return _clean_row(
            conn.execute(
                "SELECT * FROM users WHERE keycloak_user_id = %s", (keycloak_user_id,)
            ).fetchone()
        )


# =============================================================================
# Webhook records
# =============================================================================

def _parse_timestamp(value: Any) -> Optional[datetime]:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _webhook_payload_and_hash(rec: dict) -> tuple[str, str]:
    payload = json.dumps(rec, sort_keys=True, ensure_ascii=False, default=str)
    return payload, hashlib.sha256(payload.encode("utf-8")).hexdigest()


def webhook_external_id(rec: dict) -> str:
    """The (source, external_id) key of a webhook record: contentID, else id, else slug, else payload hash."""
    _, payload_hash = _webhook_payload_and_hash(rec)
    for key in ("contentID", "id", "slug"):
        value = rec.get(key)
        if value not in (None, ""):
            return str(value)
    return payload_hash


def upsert_webhook_records(source: str, records: list) -> int:
    """Upsert records by (source, external_id) in one transaction; returns len(records).

    external_id comes from contentID, else id, else slug, else a hash of the payload.
    Unchanged payloads (same payload_hash) are not rewritten but still counted.
    """
    now = datetime.now(timezone.utc)
    rows = []
    for rec in records:
        if not isinstance(rec, dict):
            raise ValueError("Each record must be a JSON object")
        payload_text, payload_hash = _webhook_payload_and_hash(rec)
        ext = webhook_external_id(rec)
        pub = rec.get("isPublished")
        rows.append((
            source, str(ext), rec.get("title"), rec.get("slug"), rec.get("status"),
            bool(pub) if isinstance(pub, (bool, int)) else None,
            rec.get("domainName"), Jsonb(json.loads(payload_text)), payload_hash,
            _parse_timestamp(rec.get("createdAt")) or now,
            _parse_timestamp(rec.get("updatedAt")) or now,
        ))
    with get_connection() as conn:
        for r in rows:
            conn.execute(
                """
                INSERT INTO webhook_records
                    (source, external_id, title, slug, status, is_published, category,
                     payload, payload_hash, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (source, external_id) DO UPDATE SET
                    title = EXCLUDED.title,
                    slug = EXCLUDED.slug,
                    status = EXCLUDED.status,
                    is_published = EXCLUDED.is_published,
                    category = EXCLUDED.category,
                    payload = EXCLUDED.payload,
                    payload_hash = EXCLUDED.payload_hash,
                    created_at = EXCLUDED.created_at,
                    updated_at = EXCLUDED.updated_at
                WHERE webhook_records.payload_hash IS DISTINCT FROM EXCLUDED.payload_hash
                """,
                r,
            )
    return len(rows)


_WEBHOOK_RECORD_COLUMNS = (
    "id, source, external_id, title, slug, status, is_published, category, payload, "
    "payload_hash, created_at, updated_at, indexed_hash, promoted_hash"
)


def get_webhook_record(record_id: str) -> Optional[dict]:
    """One webhook_records row as a dict (``payload`` already parsed from JSONB); None if missing."""
    if not _is_uuid(record_id):
        return None
    with get_connection() as conn:
        row = conn.execute(
            f"SELECT {_WEBHOOK_RECORD_COLUMNS} FROM webhook_records WHERE id = %s", (record_id,)
        ).fetchone()
    if row is None:
        return None
    record = dict(row)
    record["id"] = str(record["id"])
    if not isinstance(record["payload"], dict):
        record["payload"] = {}
    return record


def get_unindexed_webhook_record_ids(
    source: str = "webhook", external_ids: Optional[list] = None
) -> list[str]:
    """Ids of rows whose payload changed since they were last indexed (or never indexed).

    ``external_ids`` limits the check to the records of one webhook call.
    """
    sql = (
        "SELECT id FROM webhook_records WHERE source = %s "
        "AND indexed_hash IS DISTINCT FROM payload_hash"
    )
    params: list = [source]
    if external_ids is not None:
        if not external_ids:
            return []
        sql += " AND external_id = ANY(%s)"
        params.append([str(e) for e in external_ids])
    with get_connection() as conn:
        return [str(r["id"]) for r in conn.execute(sql + " ORDER BY created_at, id", params)]


def get_webhook_record_ids(
    source: str = "webhook", published_only: bool = False, indexed_only: bool = False
) -> list[str]:
    """All record ids for a source, optionally only published and/or dev-indexed ones.

    ``indexed_only`` keeps rows whose current payload is what was last written to the dev
    collection (``indexed_hash = payload_hash``), so a bulk promote never pushes a payload
    that has not been indexed in dev.
    """
    sql = "SELECT id FROM webhook_records WHERE source = %s"
    if published_only:
        sql += " AND is_published AND status = 'Published'"
    if indexed_only:
        sql += " AND indexed_hash = payload_hash"
    with get_connection() as conn:
        return [str(r["id"]) for r in conn.execute(sql + " ORDER BY created_at, id", (source,))]


def _mark_webhook_record(column: str, record_id: str, payload_hash: str) -> None:
    with get_connection() as conn:
        conn.execute(f"UPDATE webhook_records SET {column} = %s WHERE id = %s", (payload_hash, record_id))


def mark_webhook_record_indexed(record_id: str, payload_hash: str) -> None:
    """Record the payload_hash that was written to the dev collection."""
    _mark_webhook_record("indexed_hash", record_id, payload_hash)


def mark_webhook_record_promoted(record_id: str, payload_hash: str) -> None:
    """Record the payload_hash that was written to the PROD collection."""
    _mark_webhook_record("promoted_hash", record_id, payload_hash)


def advisory_status_counts(source: str = "webhook") -> dict:
    """Counts for the advisory status endpoint."""
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE is_published AND status = 'Published') AS published,
                COUNT(*) FILTER (WHERE indexed_hash = payload_hash) AS indexed,
                COUNT(*) FILTER (WHERE promoted_hash = payload_hash) AS promoted
            FROM webhook_records WHERE source = %s
            """,
            (source,),
        ).fetchone()
    total = int(row["total"])
    indexed = int(row["indexed"])
    return {
        "total": total,
        "published": int(row["published"]),
        "indexed": indexed,
        "unindexed": total - indexed,
        "promoted": int(row["promoted"]),
    }


# =============================================================================
# Email OTPs
# =============================================================================

def store_email_otp(
    *, email: str, code_hash: str, expires_at: str, created_at: str, last_sent_at: str
) -> None:
    """Upsert the one live OTP for an email, resetting attempts on resend."""
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO email_otps (email, code_hash, expires_at, attempts, last_sent_at)
            VALUES (%s, %s, %s, 0, %s)
            ON CONFLICT (email) DO UPDATE SET
                code_hash = EXCLUDED.code_hash,
                expires_at = EXCLUDED.expires_at,
                attempts = 0,
                last_sent_at = EXCLUDED.last_sent_at
            """,
            (email.strip().lower(), code_hash, expires_at, last_sent_at),
        )


def get_email_otp(email: str) -> Optional[dict]:
    """The live OTP row; timestamps are timezone-aware ISO strings (UTC)."""
    with get_connection() as conn:
        row = conn.execute(
            "SELECT * FROM email_otps WHERE email = %s", (email.strip().lower(),)
        ).fetchone()
    if row is None:
        return None
    row = _clean_row(row) | {
        "expires_at": row["expires_at"].astimezone(timezone.utc).isoformat(),
        "last_sent_at": row["last_sent_at"].astimezone(timezone.utc).isoformat(),
    }
    return row


def increment_email_otp_attempts(email: str) -> Optional[int]:
    """Atomically count one failed attempt; returns the new total."""
    with get_connection() as conn:
        row = conn.execute(
            "UPDATE email_otps SET attempts = attempts + 1 WHERE email = %s RETURNING attempts",
            (email.strip().lower(),),
        ).fetchone()
    return int(row["attempts"]) if row else None


def delete_email_otp(email: str) -> None:
    with get_connection() as conn:
        conn.execute("DELETE FROM email_otps WHERE email = %s", (email.strip().lower(),))


# =============================================================================
# Catalog
# =============================================================================

CATALOG_VERSION_ACTION = "catalog_version_bump"


def get_catalog_meta() -> dict:
    """Catalog version: the number of version bumps recorded in ``audit_logs``."""
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT count(*) AS version, max(created_at) AS updated_at,
                   (array_agg(metadata->>'reason' ORDER BY created_at DESC))[1] AS notes
            FROM audit_logs
            WHERE entity_type = 'catalog' AND action_type = %s
            """,
            (CATALOG_VERSION_ACTION,),
        ).fetchone()
    return {
        "id": 1,
        "version": int(row["version"] or 0),
        "updated_at": _clean(row["updated_at"]),
        "notes": row["notes"],
    }


def bump_catalog_version(reason: str = "") -> int:
    """Monotonic catalog version bump. Returns new version."""
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO audit_logs (action_type, entity_type, metadata)
            VALUES (%s, 'catalog', %s)
            """,
            (CATALOG_VERSION_ACTION, Jsonb({"reason": (reason or "")[:500]})),
        )
        row = conn.execute(
            "SELECT count(*) AS version FROM audit_logs "
            "WHERE entity_type = 'catalog' AND action_type = %s",
            (CATALOG_VERSION_ACTION,),
        ).fetchone()
    return int(row["version"])


def list_document_codes(kind: str) -> set[str]:
    """Every document code in catalogs of one knowledge kind."""
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT d.code FROM documents d JOIN catalogs c ON c.id = d.catalog_id WHERE c.type = %s",
            (catalog_type_for_kind(kind),),
        ).fetchall()
    return {r["code"] for r in rows}


def _catalog_row(row: dict) -> dict:
    cleaned = _clean_row(row)
    cleaned.pop("group_name", None)
    cleaned["aliases"] = list(row["aliases"] or [])
    cleaned["groups"] = [(row["group_name"] or "").lower()]
    cleaned["workflow_ids"] = [row["workflow_id"]] if row["workflow_id"] else []
    cleaned["source"] = "pipeline"
    return cleaned


def list_catalog_entries(status: Optional[str] = None, kind: str = "scheme") -> list[dict]:
    """Rows of the ``catalog_status`` view (one per document) for one knowledge kind."""
    catalog_type = catalog_type_for_kind(kind)
    with get_connection() as conn:
        if status:
            rows = conn.execute(
                "SELECT * FROM catalog_status WHERE type = %s AND status = %s ORDER BY code",
                (catalog_type, status),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM catalog_status WHERE type = %s ORDER BY code", (catalog_type,)
            ).fetchall()
    return [_catalog_row(r) for r in rows]


def get_catalog_entry(code: str, kind: str = "scheme") -> Optional[dict]:
    with get_connection() as conn:
        row = conn.execute(
            "SELECT * FROM catalog_status WHERE type = %s AND code = %s LIMIT 1",
            (catalog_type_for_kind(kind), code),
        ).fetchone()
    return _catalog_row(row) if row else None


def list_used_scheme_codes(exclude_workflow_id: Optional[str] = None, kind: str = "scheme") -> set[str]:
    """Every document code already taken in a kind - the collision domain for auto-generated codes.

    The excluded document's own code is not counted, so re-deriving a
    document's code does not collide with itself.
    """
    with get_connection() as conn:
        doc_uuid = _resolve_document_uuid(conn, exclude_workflow_id) if exclude_workflow_id else None
        rows = conn.execute(
            """
            SELECT d.code FROM documents d JOIN catalogs c ON c.id = d.catalog_id
            WHERE c.type = %s AND (%s::uuid IS NULL OR d.id <> %s::uuid)
            """,
            (catalog_type_for_kind(kind), doc_uuid, doc_uuid),
        ).fetchall()
    return {r["code"] for r in rows}


def list_scheme_catalog_entries_for_workflow(workflow_id: str) -> list[dict]:
    """Catalog rows whose documents still include this workflow."""
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT code, workflow_id FROM catalog_status WHERE type = %s AND workflow_id = %s",
            (catalog_type_for_kind("scheme"), workflow_id),
        ).fetchall()
    return [{"code": r["code"], "workflow_ids": [r["workflow_id"]]} for r in rows]


def find_document_by_code(group: str, kind: str, code: str) -> Optional[dict]:
    """The document holding ``code`` in the group's ``kind`` catalog, if any."""
    tenant = (group or _default_group()).strip().lower() or _default_group()
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT d.id FROM documents d
            JOIN catalogs c ON c.id = d.catalog_id
            JOIN groups g ON g.id = c.group_id
            WHERE lower(g.name) = %s AND c.type = %s AND d.code = %s
            """,
            (tenant, catalog_type_for_kind(kind), code),
        ).fetchone()
        return _fetch_document(conn, str(row["id"])) if row else None


class DuplicateDocumentCode(ValueError):
    """The ``(catalog, code)`` pair is already held by another document."""


# =============================================================================
# Documents
# =============================================================================

def upsert_document(
    workflow_id: str,
    document_id: str,
    filename: str,
    filepath: str,
    canonical_document_id: Optional[str] = None,
    name: Optional[str] = None,
    source_filename: Optional[str] = None,
    source_manifest_name: Optional[str] = None,
    source_file_fingerprint: Optional[str] = None,
    stage: str = "registered",
    page_count: int = 0,
    chunk_count: int = 0,
    error_message: Optional[str] = None,
    ocr_completed_at: Optional[str] = None,
    translation_completed_at: Optional[str] = None,
    chunks_completed_at: Optional[str] = None,
    ingested_at: Optional[str] = None,
    source_type: Optional[str] = None,
    canonical_input_type: Optional[str] = None,
    stop_after_ocr: bool = False,
    original_artifact_id: Optional[str] = None,
    normalized_artifact_id: Optional[str] = None,
    latest_job_id: Optional[str] = None,
    group: Optional[str] = None,
    uploaded_by_user_id: Optional[str] = None,
    uploaded_by_username: Optional[str] = None,
    uploaded_by_email: Optional[str] = None,
    uploaded_by_roles: Optional[str] = None,
    valid_from: Optional[str] = None,
    valid_to: Optional[str] = None,
    with_workflow: bool = True,
    kind: Optional[str] = None,
    code: Optional[str] = None,
    aliases: Optional[list[str]] = None,
) -> str:
    """Insert or update a document record; returns the document UUID.

    A new document needs ``kind`` (``scheme`` / ``advisory``) and ``code``:
    ``group`` (a tenant code) and the kind pick its ``(group, kind)`` catalog,
    which is created when missing. ``name`` becomes ``documents.name`` (the
    code when absent). ``DuplicateDocumentCode`` is raised when another
    document already holds ``code`` in that catalog.

    ``group`` and the catalog are applied on INSERT only. Updates leave them
    alone so a re-upload / restart cannot silently reassign tenants.
    Uploader identity is set on INSERT, and filled on UPDATE only when currently null.

    ``valid_from``/``valid_to`` are the document's validity period, stored as
    given and likewise INSERT-only, so an approver's later edit survives a
    restart that re-registers the same workflow. What the period should default
    to is the caller's call (see ``pipeline/document_validity.py``); omitting
    both stores NULL, which search reads as "no stated period".

    ``page_count`` and ``chunk_count`` are accepted for call-site compatibility
    and ignored: both are counted from ``pages`` and ``chunks``.

    A new document also gets its first workflow row (``temporal_workflow_id`` =
    ``workflow_id``) unless ``with_workflow`` is False, for callers that create
    it themselves with ``create_document_job`` (addressing the document by the
    returned UUID, since no workflow id resolves to it yet).
    """
    group_value = (group or _default_group()).strip().lower() or _default_group()
    fingerprint = _fingerprint_for(
        source_file_fingerprint or canonical_document_id, document_id, workflow_id
    )
    source_patch = {
        "fingerprint": fingerprint,
        "original_filename": filename,
        "storage_uri": filepath,
        "source_type": source_type,
        "canonical_input_type": canonical_input_type,
    }
    with get_connection() as conn:
        doc_uuid = _resolve_document_uuid(conn, workflow_id) if workflow_id else None
        if doc_uuid:
            patch = {k: v for k, v in source_patch.items() if v is not None}
            DocumentSource.model_validate({**_load(conn, doc_uuid, "source"), **patch})
            uploader_uuid = (
                upsert_user(uploaded_by_user_id, uploaded_by_username, uploaded_by_email,
                            uploaded_by_roles, conn=conn, tenants=[group_value])
                if uploaded_by_user_id else None
            )
            conn.execute(
                """
                UPDATE documents SET
                    source = source || %s::jsonb,
                    name = COALESCE(%s, name),
                    metadata = CASE
                        WHEN %s::text IS NULL OR metadata ? 'uploaded_by_user_id' THEN metadata
                        ELSE metadata || jsonb_build_object('uploaded_by_user_id', %s::text)
                    END
                WHERE id = %s::uuid
                """,
                (Jsonb(patch), name or None, uploader_uuid, uploader_uuid, doc_uuid),
            )
            cw = conn.execute(
                "SELECT id FROM document_current_workflow WHERE document_id = %s::uuid", (doc_uuid,)
            ).fetchone()
            if cw:
                conn.execute(
                    """
                    UPDATE workflows SET current_stage = %s, error_message = %s,
                        stop_after_ocr = %s
                    WHERE id = %s::uuid
                    """,
                    (stage, error_message, bool(stop_after_ocr), str(cw["id"])),
                )
            return doc_uuid

        if not kind or not code:
            raise ValueError("A new document needs kind and code")
        source = DocumentSource.model_validate(
            {k: v for k, v in source_patch.items() if v is not None}
        ).model_dump(exclude_none=True)
        user_uuid = (
            upsert_user(uploaded_by_user_id, uploaded_by_username, uploaded_by_email,
                        uploaded_by_roles, conn=conn, tenants=[group_value])
            if uploaded_by_user_id else None
        )
        metadata = DocumentMetadata(
            valid_from=valid_from, valid_to=valid_to, uploaded_by_user_id=user_uuid
        ).model_dump(exclude_none=True)
        catalog = get_or_create_catalog(get_or_create_group(group_value, conn=conn), kind, conn=conn)
        try:
            with conn.transaction():
                row = conn.execute(
                    """
                    INSERT INTO documents (catalog_id, code, name, aliases, source, metadata)
                    VALUES (%s::uuid, %s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (
                        catalog["id"], code, name or code,
                        [a for a in (aliases or []) if a], Jsonb(source), Jsonb(metadata),
                    ),
                ).fetchone()
        except psycopg.errors.UniqueViolation as exc:
            if exc.diag.constraint_name == "documents_catalog_id_code_key":
                raise DuplicateDocumentCode(
                    f"Code '{code}' is already used in the {kind} catalog of {group_value}"
                ) from exc
            raise
        if with_workflow:
            conn.execute(
                """
                INSERT INTO workflows (document_id, workflow_type, temporal_workflow_id,
                                       current_stage, error_message, stop_after_ocr,
                                       started_by_user_id)
                VALUES (%s::uuid, %s, %s, %s, %s, %s, %s::uuid)
                """,
                (
                    str(row["id"]),
                    "ocr_only" if stop_after_ocr else "pipeline",
                    workflow_id, stage, error_message, bool(stop_after_ocr), user_uuid,
                ),
            )
        return str(row["id"])


def reset_document_for_reupload(
    doc_uuid: str,
    *,
    filename: str,
    filepath: str,
    fingerprint: str,
    source_type: Optional[str] = None,
    canonical_input_type: Optional[str] = None,
) -> None:
    """Point an existing document at a re-uploaded file.

    ``source`` is replaced, pages and chunks are cleared for the new run, and
    the document is flagged ``reindex_required`` because its vector index is now
    stale. Earlier workflows and artifacts stay as history.

    The fingerprint is the document's key in the vector index, so the old key
    is remembered in ``metadata.superseded_doc_ids`` for every index (dev/prod)
    that holds it. The next ingest/promote and disable/purge delete those points;
    without this they would stay searchable with nothing pointing at them.
    """
    source = DocumentSource.model_validate({
        "fingerprint": fingerprint,
        "original_filename": filename,
        "storage_uri": filepath,
        "source_type": source_type,
        "canonical_input_type": canonical_input_type,
    }).model_dump(exclude_none=True)
    with get_connection() as conn:
        current = conn.execute(
            "SELECT source->>'fingerprint' AS fingerprint, metadata FROM documents "
            "WHERE id = %s::uuid FOR UPDATE",
            (doc_uuid,),
        ).fetchone()
        if current is None:
            raise LookupError(f"Document not found: {doc_uuid}")
        meta = current["metadata"] or {}
        superseded = {k: list(v) for k, v in (meta.get("superseded_doc_ids") or {}).items()}
        for env, entry in (meta.get("index_status") or {}).items():
            old_key = (entry or {}).get("doc_id") or current["fingerprint"]
            if old_key and old_key != fingerprint and old_key not in superseded.get(env, []):
                superseded.setdefault(env, []).append(old_key)
        conn.execute("DELETE FROM pages WHERE document_id = %s::uuid", (doc_uuid,))
        conn.execute("DELETE FROM chunks WHERE document_id = %s::uuid", (doc_uuid,))
        conn.execute(
            """
            UPDATE documents SET
                source = %s::jsonb,
                metadata = (metadata - 'chunking')
                    || jsonb_build_object('reindex_required', TRUE, 'reindex_reason', 'source_replaced',
                                          'superseded_doc_ids', %s::jsonb)
            WHERE id = %s::uuid
            """,
            (Jsonb(source), Jsonb(superseded), doc_uuid),
        )


def doc_id_used_by_other_document(doc_id: str, exclude_doc_uuid: Optional[str], env: str) -> bool:
    """True when another *live* document is indexed in ``env`` (dev/prod) under ``doc_id``.

    The index key is the file fingerprint, so two documents holding the same file
    share points in the vector index. Deleting a key another document still needs
    would remove that document's vectors too. Only a document that is not
    disabled and whose own ``index_status[env]`` holds this key counts: one that
    was merely uploaded, or is disabled, has nothing in the index to protect.
    """
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT 1 FROM documents d
            WHERE (%s::uuid IS NULL OR d.id <> %s::uuid)
              AND NOT d.is_disabled
              AND d.metadata->'index_status'->%s::text->>'doc_id' = %s
            LIMIT 1
            """,
            (exclude_doc_uuid, exclude_doc_uuid, env, doc_id),
        ).fetchone()
    return row is not None


def clear_superseded_doc_ids(workflow_id: str, env: str, doc_ids: list[str]) -> None:
    """Forget superseded index keys once their points are deleted from ``env`` (dev/prod)."""
    with get_connection() as conn:
        doc_uuid = _resolve_document_uuid(conn, workflow_id)
        if doc_uuid is None:
            return
        row = conn.execute(
            "SELECT metadata->'superseded_doc_ids' AS sup FROM documents WHERE id = %s::uuid FOR UPDATE",
            (doc_uuid,),
        ).fetchone()
        superseded = {k: list(v) for k, v in (row["sup"] or {}).items()}
        remaining = [i for i in superseded.get(env, []) if i not in set(doc_ids)]
        if remaining:
            superseded[env] = remaining
        else:
            superseded.pop(env, None)
        conn.execute(
            "UPDATE documents SET metadata = metadata || jsonb_build_object('superseded_doc_ids', %s::jsonb) "
            "WHERE id = %s::uuid",
            (Jsonb(superseded), doc_uuid),
        )


def _load(conn, doc_uuid: str, column: str) -> dict:
    row = conn.execute(f"SELECT {column} FROM documents WHERE id = %s::uuid", (doc_uuid,)).fetchone()
    return dict(row[column]) if row else {}


def update_document_stage(
    workflow_id: str,
    stage: str,
    page_count: Optional[int] = None,
    chunk_count: Optional[int] = None,
    error_message: Optional[str] = None
):
    """Update the stage (of the document's current workflow).

    ``page_count`` and ``chunk_count`` are accepted for call-site compatibility;
    both are counted from ``pages`` and ``chunks``, so only the audit entry keeps them.
    """
    old_stage = None
    audit_workflow_id = workflow_id
    with get_connection() as conn:
        doc_uuid = _resolve_document_uuid(conn, workflow_id)
        if doc_uuid is None:
            return
        cw = conn.execute(
            "SELECT id, current_stage FROM document_current_workflow WHERE document_id = %s::uuid",
            (doc_uuid,),
        ).fetchone()
        if cw is None:
            return
        old_stage = cw["current_stage"]

        assignments = ["current_stage = %s"]
        params: list = [stage]
        if error_message is not None:
            assignments.append("error_message = %s")
            params.append(error_message)
        stamp_key = {
            "ocr_review": "ocr",
            "translation_review": "translation",
            "chunk_review": "chunks",
        }.get(stage)
        if stamp_key:
            assignments.append(
                "stage_timestamps = jsonb_build_object(%s::text, %s::text) || stage_timestamps"
            )
            params.extend([stamp_key, _iso_now()])
        if stage == "completed":
            assignments.append("completed_at = now()")
        params.append(str(cw["id"]))
        conn.execute(
            f"UPDATE workflows SET {', '.join(assignments)} WHERE id = %s::uuid", params
        )

        conn.execute("UPDATE documents SET updated_at = now() WHERE id = %s::uuid", (doc_uuid,))
        audit_workflow_id = _root_workflow_id(conn, doc_uuid) or workflow_id

    if old_stage and old_stage != stage:
        log_audit(
            workflow_id=audit_workflow_id,
            document_id=audit_workflow_id,
            action_type="stage_change",
            field_name="stage",
            old_value=old_stage,
            new_value=stage,
            metadata={"page_count": page_count, "chunk_count": chunk_count}
        )


def _root_workflow_id(conn, doc_uuid: str) -> Optional[str]:
    row = conn.execute(
        """
        SELECT temporal_workflow_id FROM workflows WHERE document_id = %s::uuid
        ORDER BY (workflow_type IN ('pipeline', 'ocr_only')) DESC, started_at ASC, id ASC
        LIMIT 1
        """,
        (doc_uuid,),
    ).fetchone()
    return row["temporal_workflow_id"] if row else None


def reconcile_materialized_state(workflow_id: str) -> Optional[dict]:
    """
    Reconcile a document row against already-persisted page/chunk content.

    This is intentionally conservative: it only moves a document forward into
    review states when the corresponding materialized content already exists.
    """
    doc = get_document(workflow_id)
    if not doc:
        return None
    workflow_id = doc["workflow_id"]

    with get_connection() as conn:
        page_stats = conn.execute(
            """
            SELECT COUNT(*) AS page_rows, COALESCE(MAX(page_number), 0) AS max_page_number
            FROM pages WHERE document_id = %s::uuid
            """,
            (doc["id"],),
        ).fetchone()
        chunk_stats = conn.execute(
            "SELECT COUNT(*) AS chunk_rows FROM chunks WHERE document_id = %s::uuid",
            (doc["id"],),
        ).fetchone()

    page_count = int((page_stats["page_rows"] if page_stats else 0) or 0)
    max_page_number = int((page_stats["max_page_number"] if page_stats else 0) or 0)
    materialized_pages = max(page_count, max_page_number, int(doc.get("page_count") or 0))
    chunk_count = int((chunk_stats["chunk_rows"] if chunk_stats else 0) or 0)

    current_stage = doc.get("stage")
    latest_job = get_latest_document_job(workflow_id)
    updated = False
    now = _iso_now()

    # Highest-confidence reconciliation first: if chunks exist, the document has
    # already reached chunk review materiality even if the workflow stalled.
    if chunk_count > 0 and current_stage not in {
        "chunk_review",
        "ready_for_ingestion",
        "ingesting",
        "approval_for_prod",
        "completed",
    }:
        update_document_stage(
            workflow_id=workflow_id,
            stage="chunk_review",
            page_count=materialized_pages,
            chunk_count=chunk_count,
            error_message=None,
        )
        if latest_job:
            update_document_job(
                latest_job["id"],
                current_stage="chunk_review",
                status="waiting_review",
                error_message=None,
            )
        doc = get_document(workflow_id) or doc
        current_stage = doc.get("stage")
        updated = True

    # Lower-confidence but still safe: if OCR pages exist and the document is
    # somehow still marked as pre-review, move it into OCR review.
    elif materialized_pages > 0 and current_stage in {"registered", "ocr_processing"}:
        update_document_stage(
            workflow_id=workflow_id,
            stage="ocr_review",
            page_count=materialized_pages,
            chunk_count=int(doc.get("chunk_count") or 0),
            error_message=None,
        )
        if latest_job:
            update_document_job(
                latest_job["id"],
                current_stage="ocr_review",
                status="waiting_review",
                error_message=None,
            )
        updated = True

    # Even when stage is already correct, keep counts truthful.
    elif materialized_pages != int(doc.get("page_count") or 0) or chunk_count != int(doc.get("chunk_count") or 0):
        update_document_stage(
            workflow_id=workflow_id,
            stage=current_stage,
            page_count=materialized_pages,
            chunk_count=chunk_count,
            error_message=doc.get("error_message"),
        )
        updated = True

    if not updated:
        return {
            "workflow_id": workflow_id,
            "updated": False,
            "stage": current_stage,
            "page_count": int(doc.get("page_count") or 0),
            "chunk_count": int(doc.get("chunk_count") or 0),
        }

    refreshed = get_document(workflow_id) or doc
    refreshed_job = get_latest_document_job(workflow_id)
    return {
        "workflow_id": workflow_id,
        "updated": True,
        "stage": refreshed.get("stage"),
        "page_count": int(refreshed.get("page_count") or 0),
        "chunk_count": int(refreshed.get("chunk_count") or 0),
        "job_status": refreshed_job.get("status") if refreshed_job else None,
        "job_stage": refreshed_job.get("current_stage") if refreshed_job else None,
        "reconciled_at": now,
    }


def get_document(workflow_id: str) -> Optional[dict]:
    """Get a single document by workflow id, UUID or any other identifier."""
    with get_connection() as conn:
        return _fetch_document(conn, _resolve_document_uuid(conn, workflow_id))


def list_documents(
    stage: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
    include_disabled: bool = False,
    groups: Optional[list[str]] = None,
) -> list[dict]:
    """List documents with optional stage / group filters.

    Args:
        stage: Filter by document stage
        limit: Max documents to return
        offset: Pagination offset
        include_disabled: If False (default), excludes soft-deleted documents from results
        groups: If provided, only return docs whose group is in this list
    """
    if groups is not None and not _normalize_groups(groups):
        return []
    clauses, params = _document_filters(include_disabled, groups)
    if stage:
        clauses.append("cw.current_stage = %s")
        params.append(stage)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    with get_connection() as conn:
        rows = conn.execute(
            f"{_DOC_SELECT} {where} ORDER BY d.created_at DESC, d.id LIMIT %s OFFSET %s",
            (*params, limit, offset),
        ).fetchall()
    return [_document_from_row(r) for r in rows]


_REVIEW_QUEUE_STAGES = ("ocr_review", "translation_review", "chunk_review", "approval_for_prod")

_SUMMARY_KEYS = (
    "total_documents",
    "authoritative_documents",
    "legacy_documents",
    "completed_documents",
    "review_queue",
    "ocr_review_documents",
    "translation_review_documents",
    "chunk_review_documents",
    "translation_processing_documents",
    "chunking_documents",
    "ready_for_ingestion_documents",
    "failed_documents",
    "needs_reindex",
    "running_jobs",
)


def get_document_summary_counts(
    include_disabled: bool = False,
    groups: Optional[list[str]] = None,
) -> dict:
    """Return aggregate document counts for dashboard and migration planning."""
    if groups is not None and not _normalize_groups(groups):
        return {key: 0 for key in _SUMMARY_KEYS}
    clauses, params = _document_filters(include_disabled, groups)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    with get_connection() as conn:
        row = conn.execute(
            f"""
            SELECT
                COUNT(*) AS total_documents,
                0 AS authoritative_documents,
                COUNT(*) AS legacy_documents,
                COUNT(*) FILTER (WHERE cw.current_stage = 'completed') AS completed_documents,
                COUNT(*) FILTER (WHERE cw.current_stage IN
                    ('ocr_review', 'translation_review', 'chunk_review', 'approval_for_prod')) AS review_queue,
                COUNT(*) FILTER (WHERE cw.current_stage = 'ocr_review') AS ocr_review_documents,
                COUNT(*) FILTER (WHERE cw.current_stage = 'translation_review') AS translation_review_documents,
                COUNT(*) FILTER (WHERE cw.current_stage = 'chunk_review') AS chunk_review_documents,
                COUNT(*) FILTER (WHERE cw.current_stage = 'translation_processing') AS translation_processing_documents,
                COUNT(*) FILTER (WHERE cw.current_stage = 'chunking') AS chunking_documents,
                COUNT(*) FILTER (WHERE cw.current_stage = 'ready_for_ingestion') AS ready_for_ingestion_documents,
                COUNT(*) FILTER (WHERE cw.current_stage = 'failed') AS failed_documents,
                COUNT(*) FILTER (WHERE COALESCE((d.metadata->>'reindex_required')::boolean, FALSE)) AS needs_reindex,
                COALESCE(SUM((SELECT COUNT(*) FROM workflows j
                              WHERE j.document_id = d.id AND j.status = 'running')), 0) AS running_jobs
            FROM documents d
            JOIN catalogs c ON c.id = d.catalog_id
            JOIN groups g ON g.id = c.group_id
            LEFT JOIN document_current_workflow cw ON cw.document_id = d.id
            {where}
            """,
            params,
        ).fetchone()
    return {key: int(row[key] or 0) for key in _SUMMARY_KEYS}


def get_document_counts_by_group(
    include_disabled: bool = False,
    groups: Optional[list[str]] = None,
) -> list[dict]:
    """Return document counts grouped by group (state), for dashboard breakdowns."""
    if groups is not None and not _normalize_groups(groups):
        return []
    clauses, params = _document_filters(include_disabled, groups)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    with get_connection() as conn:
        rows = conn.execute(
            f"""
            SELECT
                lower(g.name) AS "group",
                COUNT(*) AS count,
                COUNT(*) FILTER (WHERE cw.current_stage = 'completed') AS success,
                COUNT(*) FILTER (WHERE cw.current_stage = 'failed') AS failed,
                COUNT(*) FILTER (WHERE cw.current_stage IN
                    ('ocr_review', 'translation_review', 'chunk_review', 'approval_for_prod')) AS dev_approval
            FROM documents d
            JOIN catalogs c ON c.id = d.catalog_id
            JOIN groups g ON g.id = c.group_id
            LEFT JOIN document_current_workflow cw ON cw.document_id = d.id
            {where}
            GROUP BY lower(g.name)
            ORDER BY count DESC, "group" ASC
            """,
            params,
        ).fetchall()
    return [
        {
            "group": row["group"],
            "count": int(row["count"] or 0),
            "success": int(row["success"] or 0),
            "failed": int(row["failed"] or 0),
            "dev_approval": int(row["dev_approval"] or 0),
        }
        for row in rows
    ]


def set_document_disabled(workflow_id: str, is_disabled: bool = True):
    """Soft delete a document (filtered from all queries by default).

    Unlike hard delete, this preserves the document record for audit purposes.
    Use X-Include-Disabled: true header in API to see disabled documents.
    """
    with get_connection() as conn:
        doc_uuid = _resolve_document_uuid(conn, workflow_id)
        if doc_uuid:
            conn.execute(
                "UPDATE documents SET is_disabled = %s WHERE id = %s::uuid",
                (bool(is_disabled), doc_uuid),
            )


def delete_document(workflow_id: str):
    """Delete a document record and everything that cascades from it."""
    with get_connection() as conn:
        doc_uuid = _resolve_document_uuid(conn, workflow_id)
        if doc_uuid:
            conn.execute("DELETE FROM documents WHERE id = %s::uuid", (doc_uuid,))


def purge_document(workflow_id: str, keep_audit: bool = True) -> dict:
    """Hard delete every row belonging to a document.

    Unlike :func:`set_document_disabled` this is irreversible - there is
    nothing left for ``POST /documents/{id}/restore`` to bring back.

    Audit rows are kept by default: they are the record that the deletion
    happened, not part of the document itself. Their ``document_id`` becomes
    NULL; the ``workflow_id`` and ``group`` snapshots stay.

    Returns a per-table count of deleted rows.
    """
    deleted: dict[str, int] = {}
    with get_connection() as conn:
        doc_uuid = _resolve_document_uuid(conn, workflow_id)
        if doc_uuid is None:
            return deleted
        root = _root_workflow_id(conn, doc_uuid) or workflow_id

        def _count(sql: str) -> int:
            return int(conn.execute(sql, (doc_uuid,)).fetchone()["n"])

        counts = {
            "chunks": _count("SELECT count(*) AS n FROM chunks WHERE document_id = %s::uuid"),
            "pages": _count("SELECT count(*) AS n FROM pages WHERE document_id = %s::uuid"),
            "document_artifacts": _count(
                "SELECT count(*) AS n FROM workflow_artifacts a JOIN workflows w ON w.id = a.workflow_id "
                "WHERE w.document_id = %s::uuid"
            ),
            "document_jobs": _count("SELECT count(*) AS n FROM workflows WHERE document_id = %s::uuid"),
        }
        if not keep_audit:
            row = conn.execute(
                "DELETE FROM audit_logs WHERE document_id = %s::uuid OR workflow_id = %s",
                (doc_uuid, root),
            )
            if row.rowcount > 0:
                deleted["audit_logs"] = row.rowcount
        conn.execute("DELETE FROM documents WHERE id = %s::uuid", (doc_uuid,))
        for table, count in counts.items():
            if count > 0:
                deleted[table] = count
        deleted["documents"] = 1
    return deleted


def get_document_count(stage: Optional[str] = None) -> int:
    """Get count of documents, optionally filtered by stage."""
    with get_connection() as conn:
        if stage:
            row = conn.execute(
                """
                SELECT COUNT(*) AS cnt FROM documents d
                JOIN document_current_workflow cw ON cw.document_id = d.id
                WHERE cw.current_stage = %s
                """,
                (stage,),
            ).fetchone()
        else:
            row = conn.execute("SELECT COUNT(*) AS cnt FROM documents").fetchone()
    return int(row["cnt"]) if row else 0


def update_document_fields(workflow_id: str, **updates: object) -> Optional[dict]:
    """Update selected document fields and return the document.

    Flat names are mapped onto the tables: ``stage``/``error_message`` act on the
    current workflow, ``code``/``name``/``aliases`` on the document columns,
    a document's kind is fixed by its catalog at upload, and flags,
    period and uploader go into ``metadata``. Names derived from other rows (counts, artifact and job
    pointers, completion timestamps) are accepted and ignored.
    """
    with get_connection() as conn:
        doc_uuid = _resolve_document_uuid(conn, workflow_id)
        if doc_uuid is None:
            return None
        if not updates:
            return _fetch_document(conn, doc_uuid)

        doc_sets: list[str] = []
        doc_params: list = []
        source_patch: dict[str, Any] = {}
        meta_patch: dict[str, Any] = {}
        meta_remove: list[str] = []
        wf_sets: list[str] = []
        wf_params: list = []

        for key, value in updates.items():
            if key == "stage":
                wf_sets.append("current_stage = %s")
                wf_params.append(value)
            elif key == "error_message":
                wf_sets.append("error_message = %s")
                wf_params.append(value)
            elif key == "source_type":
                source_patch["source_type"] = value
            elif key == "canonical_input_type":
                source_patch["canonical_input_type"] = value
            elif key == "filepath":
                source_patch["storage_uri"] = value
            elif key in ("filename", "source_filename"):
                if key == "filename" or "filename" not in updates:
                    source_patch["original_filename"] = value
            elif key in ("document_id", "canonical_document_id", "source_file_fingerprint"):
                if isinstance(value, str) and _FINGERPRINT_RE.match(value):
                    source_patch["fingerprint"] = value
            elif key == "name":
                if value:
                    doc_sets.append("name = %s")
                    doc_params.append(value)
            elif key == "code":
                if value:
                    doc_sets.append("code = %s")
                    doc_params.append(value)
            elif key == "aliases":
                doc_sets.append("aliases = %s")
                doc_params.append([a for a in (value or []) if a])
            elif key in ("reindex_required", "catalog_visible", "network_visible"):
                meta_patch[key] = bool(value)
            elif key == "reindex_reason":
                if value:
                    meta_patch["reindex_reason"] = value
                else:
                    meta_remove.append("reindex_reason")
            elif key == "stop_after_ocr":
                conn.execute(
                    "UPDATE workflows SET stop_after_ocr = %s WHERE id = ("
                    "SELECT id FROM workflows WHERE document_id = %s::uuid "
                    "ORDER BY (workflow_type IN ('pipeline', 'ocr_only')) DESC, started_at ASC, id ASC LIMIT 1)",
                    (bool(value), doc_uuid),
                )
            elif key in ("valid_from", "valid_to"):
                meta_patch[key] = value.isoformat() if isinstance(value, date) else value
            elif key == "uploaded_by_user_id" and value:
                has_uploader = conn.execute(
                    "SELECT metadata ? 'uploaded_by_user_id' AS has FROM documents WHERE id = %s::uuid",
                    (doc_uuid,),
                ).fetchone()["has"]
                if not has_uploader:
                    meta_patch["uploaded_by_user_id"] = upsert_user(
                        str(value),
                        updates.get("uploaded_by_username"),
                        updates.get("uploaded_by_email"),
                        updates.get("uploaded_by_roles"),
                        conn=conn,
                    )

        if source_patch:
            doc_sets.append("source = source || %s::jsonb")
            doc_params.append(Jsonb({k: v for k, v in source_patch.items() if v is not None}))
        if meta_patch or meta_remove:
            expr = "metadata"
            for removed in meta_remove:
                expr = f"({expr} - '{removed}')"
            if meta_patch:
                expr = f"{expr} || %s::jsonb"
                doc_params.append(Jsonb(meta_patch))
            doc_sets.append(f"metadata = {expr}")

        if wf_sets:
            conn.execute(
                f"UPDATE workflows SET {', '.join(wf_sets)} WHERE id = ("
                "SELECT id FROM document_current_workflow WHERE document_id = %s::uuid)",
                (*wf_params, doc_uuid),
            )
        if doc_sets:
            try:
                with conn.transaction():
                    conn.execute(
                        f"UPDATE documents SET {', '.join(doc_sets)} WHERE id = %s::uuid",
                        (*doc_params, doc_uuid),
                    )
            except psycopg.errors.UniqueViolation as exc:
                if exc.diag.constraint_name == "documents_catalog_id_code_key":
                    raise DuplicateDocumentCode(
                        "Another document in the target catalog already uses this code"
                    ) from exc
                raise
        elif wf_sets:
            conn.execute("UPDATE documents SET updated_at = now() WHERE id = %s::uuid", (doc_uuid,))
        return _fetch_document(conn, doc_uuid)


def mark_document_reindex_required(
    workflow_id: str,
    required: bool = True,
    reason: Optional[str] = None,
) -> Optional[dict]:
    return update_document_fields(
        workflow_id,
        reindex_required=bool(required),
        reindex_reason=reason if required else None,
    )


def set_document_validity(
    workflow_id: str,
    valid_from: str,
    valid_to: str,
) -> Optional[dict]:
    """Record the period this document's chunks are searchable in. Both ends
    are `YYYY-MM-DD`; validation belongs to the caller
    (`pipeline/document_validity.parse_period`), not to this SQL layer."""
    return update_document_fields(workflow_id, valid_from=valid_from, valid_to=valid_to)


def mark_prod_ready_requested(
    workflow_id: str,
    user_id: Optional[str] = None,
    username: Optional[str] = None,
) -> Optional[dict]:
    """Flag that a state_admin has asked a super_admin to review this document
    for prod promotion. Purely informational - does not fire the approve_prod
    signal itself; the super_admin still makes that call separately.

    Recorded as a ``promote_prod`` workflow in ``waiting_review``."""
    with get_connection() as conn:
        doc_uuid = _resolve_document_uuid(conn, workflow_id)
        if doc_uuid is None:
            return None
        root = _root_workflow_id(conn, doc_uuid) or workflow_id
        user_uuid = upsert_user(user_id, username, None, None, conn=conn) if user_id else None
        conn.execute(
            """
            UPDATE workflows SET status = 'running'
            WHERE document_id = %s::uuid AND workflow_type = 'promote_prod' AND status = 'waiting_review'
            """,
            (doc_uuid,),
        )
        conn.execute(
            """
            INSERT INTO workflows (document_id, workflow_type, temporal_workflow_id, status,
                                   current_stage, started_by_user_id)
            VALUES (%s::uuid, 'promote_prod', %s, 'waiting_review', 'approval_for_prod', %s::uuid)
            """,
            (doc_uuid, f"{root}-prod-request", user_uuid),
        )
        return _fetch_document(conn, doc_uuid)


def clear_prod_ready_requested(workflow_id: str) -> Optional[dict]:
    """Reset the request flag once prod approval actually happens (or the
    document leaves the approval_for_prod gate for any other reason)."""
    with get_connection() as conn:
        doc_uuid = _resolve_document_uuid(conn, workflow_id)
        if doc_uuid is None:
            return None
        conn.execute(
            """
            UPDATE workflows SET status = 'running'
            WHERE document_id = %s::uuid AND workflow_type = 'promote_prod' AND status = 'waiting_review'
            """,
            (doc_uuid,),
        )
        return _fetch_document(conn, doc_uuid)


# =============================================================================
# Workflows (exposed as "jobs")
# =============================================================================

_JOB_SELECT = """
    SELECT w.id, w.document_id, w.workflow_type, w.temporal_workflow_id, w.status,
           w.current_stage, w.config, w.error_message, w.started_at, w.completed_at,
           root.temporal_workflow_id AS doc_workflow_id
    FROM workflows w
    LEFT JOIN LATERAL (
        SELECT r.temporal_workflow_id FROM workflows r
        WHERE r.document_id = w.document_id
        ORDER BY (r.workflow_type IN ('pipeline', 'ocr_only')) DESC, r.started_at ASC, r.id ASC
        LIMIT 1
    ) root ON TRUE
"""


def _job_from_row(row: Optional[dict]) -> Optional[dict]:
    if row is None:
        return None
    config = row["config"] or {}
    job = {
        "id": row["id"],
        "workflow_id": row["doc_workflow_id"],
        "job_type": row["workflow_type"],
        "temporal_workflow_id": row["temporal_workflow_id"],
        "temporal_run_id": config.get("temporal_run_id"),
        "status": row["status"],
        "current_stage": row["current_stage"],
        "started_at": row["started_at"],
        "completed_at": row["completed_at"],
        "error_message": row["error_message"],
        "config": config,
        "config_json": json.dumps(config) if config else None,
    }
    return {k: _clean(v) for k, v in job.items()}


def create_document_job(
    workflow_id: str,
    job_type: str,
    temporal_workflow_id: Optional[str] = None,
    temporal_run_id: Optional[str] = None,
    status: str = "running",
    current_stage: Optional[str] = None,
    config: Optional[dict] = None,
    actor: Optional[dict] = None,
) -> str:
    """Record a workflow run for a document; returns the workflow UUID.

    A ``promote_prod`` run adopts the document's pending request row (see
    ``mark_prod_ready_requested``) instead of adding a second one.
    """
    cfg = dict(config or {})
    if temporal_run_id:
        cfg["temporal_run_id"] = temporal_run_id
    with get_connection() as conn:
        doc_uuid = _require_document_uuid(conn, workflow_id)
        root = _root_workflow_id(conn, doc_uuid) or workflow_id
        user_uuid = None
        if actor and actor.get("user_id"):
            user_uuid = upsert_user(
                actor["user_id"], actor.get("username"), actor.get("email"), actor.get("roles"),
                conn=conn,
            )
        if job_type == "promote_prod":
            adopted = conn.execute(
                """
                UPDATE workflows SET temporal_workflow_id = %s, status = %s,
                    current_stage = COALESCE(%s, current_stage),
                    config = config || %s::jsonb
                WHERE id = (
                    SELECT id FROM workflows
                    WHERE document_id = %s::uuid AND workflow_type = 'promote_prod'
                      AND status = 'waiting_review'
                    ORDER BY started_at DESC LIMIT 1)
                RETURNING id
                """,
                (temporal_workflow_id or root, status, current_stage, Jsonb(cfg), doc_uuid),
            ).fetchone()
            if adopted:
                conn.execute("UPDATE documents SET updated_at = now() WHERE id = %s::uuid", (doc_uuid,))
                return str(adopted["id"])
        row = conn.execute(
            """
            INSERT INTO workflows (document_id, workflow_type, temporal_workflow_id, status,
                                   current_stage, stop_after_ocr, config, started_by_user_id)
            VALUES (%s::uuid, %s, %s, %s,
                    COALESCE(%s, 'registered'), %s, %s, %s::uuid)
            RETURNING id
            """,
            (
                doc_uuid, job_type, temporal_workflow_id or root, status, current_stage,
                bool(cfg.get("stop_after_ocr")), Jsonb(cfg), user_uuid,
            ),
        ).fetchone()
        conn.execute("UPDATE documents SET updated_at = now() WHERE id = %s::uuid", (doc_uuid,))
        return str(row["id"])


def update_document_job(job_id: str, **updates: object) -> Optional[dict]:
    if not updates:
        return get_document_job(job_id)
    if not _is_uuid(str(job_id)):
        return None
    allowed_fields = {
        "status", "current_stage", "completed_at", "error_message", "temporal_run_id", "config_json"
    }
    set_clauses = []
    values: list[object] = []
    for key, value in updates.items():
        if key not in allowed_fields:
            continue
        if key == "status":
            set_clauses.append("status = %s")
        elif key == "current_stage":
            set_clauses.append("current_stage = %s")
        elif key == "completed_at":
            set_clauses.append("completed_at = %s::timestamptz")
        elif key == "temporal_run_id":
            set_clauses.append("config = config || jsonb_build_object('temporal_run_id', %s::text)")
        elif key == "config_json":
            if isinstance(value, str):
                value = json.loads(value)
            set_clauses.append("config = config || %s::jsonb")
            value = Jsonb(value or {})
        else:
            set_clauses.append(f"{key} = %s")
        values.append(value)
    if not set_clauses:
        return get_document_job(job_id)
    values.append(str(job_id))
    with get_connection() as conn:
        conn.execute(f"UPDATE workflows SET {', '.join(set_clauses)} WHERE id = %s::uuid", values)
    return get_document_job(job_id)


def get_document_job(job_id: str) -> Optional[dict]:
    if not _is_uuid(str(job_id)):
        return None
    with get_connection() as conn:
        return _job_from_row(
            conn.execute(_JOB_SELECT + " WHERE w.id = %s::uuid", (str(job_id),)).fetchone()
        )


def get_latest_document_job(workflow_id: str) -> Optional[dict]:
    with get_connection() as conn:
        doc_uuid = _resolve_document_uuid(conn, workflow_id)
        if doc_uuid is None:
            return None
        return _job_from_row(
            conn.execute(
                _JOB_SELECT + " WHERE w.document_id = %s::uuid ORDER BY w.started_at DESC, w.id DESC LIMIT 1",
                (doc_uuid,),
            ).fetchone()
        )


def list_document_jobs(workflow_id: str, limit: int = 50) -> list[dict]:
    with get_connection() as conn:
        doc_uuid = _resolve_document_uuid(conn, workflow_id)
        if doc_uuid is None:
            return []
        rows = conn.execute(
            _JOB_SELECT + " WHERE w.document_id = %s::uuid ORDER BY w.started_at DESC, w.id DESC LIMIT %s",
            (doc_uuid, limit),
        ).fetchall()
    return [_job_from_row(r) for r in rows]


def list_operations_queue(
    limit: int = 100,
    offset: int = 0,
    include_disabled: bool = False,
    groups: Optional[list[str]] = None,
) -> tuple[list[dict], int]:
    """List documents needing action, optionally scoped to tenant groups.

    ``groups``:
      - ``None`` - unrestricted (super admin / bypass)
      - ``[]`` - restricted user with no states -> empty result
      - non-empty - only those document.group values
    """
    if groups is not None and not _normalize_groups(groups):
        return [], 0
    clauses, params = _document_filters(include_disabled, groups)
    clauses.append(
        "(cw.current_stage IN ('ocr_review', 'translation_review', 'chunk_review', "
        "'ready_for_ingestion', 'approval_for_prod', 'failed') "
        "OR COALESCE((d.metadata->>'reindex_required')::boolean, FALSE) "
        "OR cw.status = 'running')"
    )
    where = f"WHERE {' AND '.join(clauses)}"
    from_sql = """
        FROM documents d
        JOIN catalogs c ON c.id = d.catalog_id
        JOIN groups g ON g.id = c.group_id
        JOIN document_current_workflow cw ON cw.document_id = d.id
    """
    with get_connection() as conn:
        total_row = conn.execute(f"SELECT COUNT(*) AS count {from_sql} {where}", params).fetchone()
        rows = conn.execute(
            f"""
            SELECT
                d.id AS id,
                (SELECT r.temporal_workflow_id FROM workflows r WHERE r.document_id = d.id
                 ORDER BY (r.workflow_type IN ('pipeline', 'ocr_only')) DESC, r.started_at ASC, r.id ASC
                 LIMIT 1) AS workflow_id,
                d.source->>'original_filename' AS filename,
                cw.current_stage AS stage,
                lower(g.name) AS "group",
                cw.error_message,
                COALESCE((d.metadata->>'reindex_required')::boolean, FALSE) AS reindex_required,
                d.metadata->>'reindex_reason' AS reindex_reason,
                cw.id AS job_id,
                cw.workflow_type AS job_type,
                cw.status AS job_status,
                cw.started_at AS started_at
            {from_sql}
            {where}
            ORDER BY cw.started_at DESC, d.id
            LIMIT %s OFFSET %s
            """,
            (*params, limit, offset),
        ).fetchall()
    return [_clean_row(r) for r in rows], int(total_row["count"] if total_row else 0)


def list_runs(
    limit: int = 100,
    offset: int = 0,
    status: Optional[str] = None,
    groups: Optional[list[str]] = None,
) -> list[dict]:
    """List workflows with document title fields for the Runs UI.

    Args:
        groups: If provided, only return runs whose document group is in
                   this list. Empty list returns no rows. None = unrestricted.
    """
    where_parts: list[str] = []
    params: list = []
    if status:
        where_parts.append("w.status = %s")
        params.append(status)
    if groups is not None:
        normalized = _normalize_groups(groups)
        if not normalized:
            return []
        where_parts.append("lower(g.name) = ANY(%s)")
        params.append(normalized)
    where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
    with get_connection() as conn:
        rows = conn.execute(
            f"""
            SELECT w.id, w.document_id, w.workflow_type, w.temporal_workflow_id, w.status,
                   w.current_stage, w.config, w.error_message, w.started_at, w.completed_at,
                   (SELECT r.temporal_workflow_id FROM workflows r WHERE r.document_id = w.document_id
                    ORDER BY (r.workflow_type IN ('pipeline', 'ocr_only')) DESC, r.started_at ASC, r.id ASC
                    LIMIT 1) AS doc_workflow_id,
                   d.source->>'original_filename' AS filename,
                   d.name AS name,
                   d.source->>'original_filename' AS source_filename,
                   cw.current_stage AS document_stage,
                   lower(g.name) AS "group"
            FROM workflows w
            JOIN documents d ON d.id = w.document_id
            JOIN catalogs c ON c.id = d.catalog_id
            JOIN groups g ON g.id = c.group_id
            LEFT JOIN document_current_workflow cw ON cw.document_id = d.id
            {where_sql}
            ORDER BY w.started_at DESC, w.id DESC
            LIMIT %s OFFSET %s
            """,
            (*params, limit, offset),
        ).fetchall()
    result = []
    for row in rows:
        job = _job_from_row(row)
        job["document_uuid"] = str(row["document_id"])
        for extra in ("filename", "name", "source_filename", "document_stage", "group"):
            job[extra] = row[extra]
        result.append(job)
    return result


# =============================================================================
# Artifacts
# =============================================================================

def _artifact_from_row(row: Optional[dict]) -> Optional[dict]:
    if row is None:
        return None
    metadata = row["metadata"] or {}
    artifact = {
        "id": row["id"],
        "workflow_id": row["doc_workflow_id"],
        "job_id": row["workflow_pk"],
        "artifact_type": row["artifact_type"],
        "stage": row["stage"],
        "storage_uri": row["storage_uri"],
        "mime_type": row["mime_type"],
        "filename": row["filename"],
        "size_bytes": row["size_bytes"],
        "metadata_json": json.dumps(metadata) if metadata else None,
        "created_at": row["created_at"],
    }
    return {k: _clean(v) for k, v in artifact.items()}


_ARTIFACT_SELECT = """
    SELECT a.id, a.workflow_id AS workflow_pk, a.artifact_type, a.stage, a.storage_uri,
           a.mime_type, a.filename, a.size_bytes, a.metadata, a.created_at,
           (SELECT r.temporal_workflow_id FROM workflows r WHERE r.document_id = w.document_id
            ORDER BY (r.workflow_type IN ('pipeline', 'ocr_only')) DESC, r.started_at ASC, r.id ASC
            LIMIT 1) AS doc_workflow_id
    FROM workflow_artifacts a
    JOIN workflows w ON w.id = a.workflow_id
"""


def add_document_artifact(
    workflow_id: str,
    artifact_type: str,
    storage_uri: str,
    stage: Optional[str] = None,
    job_id: Optional[str] = None,
    mime_type: Optional[str] = None,
    filename: Optional[str] = None,
    size_bytes: Optional[int] = None,
    metadata: Optional[dict] = None,
) -> str:
    """Record a file produced by a workflow; returns the artifact UUID.

    Belongs to ``job_id`` when given, else to the document's current workflow.
    """
    with get_connection() as conn:
        doc_uuid = _require_document_uuid(conn, workflow_id)
        wf_uuid = None
        if job_id and _is_uuid(str(job_id)):
            row = conn.execute(
                "SELECT id FROM workflows WHERE id = %s::uuid AND document_id = %s::uuid",
                (str(job_id), doc_uuid),
            ).fetchone()
            wf_uuid = str(row["id"]) if row else None
        if wf_uuid is None:
            row = conn.execute(
                "SELECT id FROM document_current_workflow WHERE document_id = %s::uuid", (doc_uuid,)
            ).fetchone()
            if row is None:
                row = conn.execute(
                    """
                    INSERT INTO workflows (document_id, workflow_type, temporal_workflow_id)
                    VALUES (%s::uuid, 'pipeline', %s) RETURNING id
                    """,
                    (doc_uuid, workflow_id),
                ).fetchone()
            wf_uuid = str(row["id"])
        created = conn.execute(
            """
            INSERT INTO workflow_artifacts (workflow_id, artifact_type, stage, storage_uri,
                                            mime_type, filename, size_bytes, metadata)
            VALUES (%s::uuid, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (
                wf_uuid, artifact_type, stage, storage_uri, mime_type, filename, size_bytes,
                Jsonb(metadata or {}),
            ),
        ).fetchone()
        return str(created["id"])


def list_document_artifacts(workflow_id: str) -> list[dict]:
    with get_connection() as conn:
        doc_uuid = _resolve_document_uuid(conn, workflow_id)
        if doc_uuid is None:
            return []
        rows = conn.execute(
            _ARTIFACT_SELECT + " WHERE w.document_id = %s::uuid ORDER BY a.created_at DESC, a.id DESC",
            (doc_uuid,),
        ).fetchall()
    return [_artifact_from_row(r) for r in rows]


def get_document_artifact(workflow_id: str, artifact_id: str) -> Optional[dict]:
    if not _is_uuid(str(artifact_id)):
        return None
    with get_connection() as conn:
        doc_uuid = _resolve_document_uuid(conn, workflow_id)
        if doc_uuid is None:
            return None
        return _artifact_from_row(
            conn.execute(
                _ARTIFACT_SELECT + " WHERE w.document_id = %s::uuid AND a.id = %s::uuid",
                (doc_uuid, str(artifact_id)),
            ).fetchone()
        )


# =============================================================================
# Vector index state (documents.metadata.index_status)
# =============================================================================

def _index_entry_to_legacy(workflow_id: str, key: str, entry: dict) -> dict:
    details = entry.get("details") or {}
    return {
        "workflow_id": workflow_id,
        "index_name": entry.get("index_name") or key,
        "doc_id": entry.get("doc_id"),
        "chunk_count_indexed": entry.get("chunk_count_indexed") or 0,
        "last_indexed_at": entry.get("last_indexed_at"),
        "last_verified_at": entry.get("last_verified_at"),
        "schema_version": entry.get("schema_version"),
        "status": entry.get("status") or "unknown",
        "details_json": json.dumps(details) if details else None,
    }


def upsert_document_index_status(
    workflow_id: str,
    index_name: str,
    doc_id: Optional[str] = None,
    chunk_count_indexed: Optional[int] = None,
    last_indexed_at: Optional[str] = None,
    last_verified_at: Optional[str] = None,
    schema_version: Optional[str] = None,
    status: str = "unknown",
    details: Optional[dict] = None,
) -> dict:
    """Merge one index's state into ``documents.metadata.index_status``.

    The key is ``prod`` for the PROD collection names and ``dev`` otherwise;
    the collection name is kept inside the entry. ``None`` arguments leave the
    stored value alone, except ``status`` which always replaces it.
    """
    key = _index_key_for(index_name)
    with get_connection() as conn:
        doc_uuid = _require_document_uuid(conn, workflow_id)
        row = conn.execute(
            "SELECT metadata->'index_status'->%s::text AS entry FROM documents WHERE id = %s::uuid FOR UPDATE",
            (key, doc_uuid),
        ).fetchone()
        existing = dict(row["entry"] or {})
        incoming = {
            "index_name": index_name,
            "doc_id": doc_id,
            "chunk_count_indexed": chunk_count_indexed,
            "last_indexed_at": last_indexed_at,
            "last_verified_at": last_verified_at,
            "schema_version": schema_version,
            "details": details or None,
        }
        merged = {**existing, **{k: v for k, v in incoming.items() if v is not None}, "status": status}
        validated = IndexState.model_validate(merged).model_dump()
        conn.execute(
            "UPDATE documents SET metadata = jsonb_set("
            "metadata || jsonb_build_object('index_status', COALESCE(metadata->'index_status', '{}'::jsonb)), "
            "ARRAY['index_status', %s::text], %s::jsonb) "
            "WHERE id = %s::uuid",
            (key, Jsonb(validated), doc_uuid),
        )
        root = _root_workflow_id(conn, doc_uuid) or workflow_id
    return _index_entry_to_legacy(root, key, validated)


def list_document_index_status(workflow_id: str) -> list[dict]:
    doc = get_document(workflow_id)
    if not doc:
        return []
    entries = [
        _index_entry_to_legacy(doc["workflow_id"], key, entry)
        for key, entry in doc["index_status"].items()
    ]
    entries.sort(
        key=lambda e: (e["last_verified_at"] or "", e["last_indexed_at"] or ""), reverse=True
    )
    return entries


def get_document_index_status(workflow_id: str, index_name: str) -> Optional[dict]:
    for entry in list_document_index_status(workflow_id):
        if entry["index_name"] == index_name:
            return entry
    return None


def list_index_summaries(include_disabled: bool = False) -> list[dict]:
    clauses, params = _document_filters(include_disabled, None)
    where = f"AND {' AND '.join(clauses)}" if clauses else ""
    with get_connection() as conn:
        rows = conn.execute(
            f"""
            SELECT
                COALESCE(e.value->>'index_name', e.key) AS index_name,
                COUNT(*) AS documents,
                SUM(COALESCE((e.value->>'chunk_count_indexed')::int, 0)) AS indexed_chunks,
                SUM(CASE WHEN COALESCE((d.metadata->>'reindex_required')::boolean, FALSE)
                         THEN 1 ELSE 0 END) AS stale_documents,
                MAX(e.value->>'last_indexed_at') AS last_indexed_at,
                array_agg(DISTINCT COALESCE(e.value->>'status', 'unknown')) AS statuses
            FROM documents d, jsonb_each(COALESCE(d.metadata->'index_status', '{{}}'::jsonb)) e
            WHERE TRUE {where}
            GROUP BY COALESCE(e.value->>'index_name', e.key)
            ORDER BY index_name
            """,
            params,
        ).fetchall()
    result = []
    for row in rows:
        item = _clean_row(row)
        item["indexed_chunks"] = int(item["indexed_chunks"] or 0)
        item["stale_documents"] = int(item["stale_documents"] or 0)
        item["statuses"] = sorted(s for s in row["statuses"] if s)
        result.append(item)
    return result


def find_document_by_doc_identifier(identifier: str) -> Optional[dict]:
    """Resolve a vector-store/chat doc identifier to a document.

    Matches the document UUID, a Temporal workflow id, the fingerprint, the
    filename, an index ``doc_id``, or the legacy ``md5(fingerprint)`` doc id.
    """
    if not identifier:
        return None
    with get_connection() as conn:
        return _fetch_document(conn, _resolve_document_uuid(conn, identifier))


def resolve_chunk_provenance(
    *,
    doc_id: Optional[str] = None,
    chunk_num: Optional[int] = None,
) -> Optional[dict]:
    """Resolve workflow + chunk metadata for provenance links."""
    if not doc_id or chunk_num is None:
        return None

    doc = find_document_by_doc_identifier(doc_id)
    if not doc:
        return None

    workflow_id = doc["workflow_id"]
    chunk = get_chunk(workflow_id, int(chunk_num))
    if not chunk:
        return None

    text = chunk.get("edited_text") or chunk.get("original_text") or ""
    excerpt = text[:320] + ("..." if len(text) > 320 else "")
    page_start = int(chunk.get("page_start") or 1)
    page_end = int(chunk.get("page_end") or page_start)
    doc_name = doc.get("name") or doc.get("filename") or workflow_id

    return {
        "workflow_id": workflow_id,
        "doc_id": doc.get("document_id"),
        "filename": workflow_id,
        "doc_name": doc_name,
        "chunk_num": int(chunk_num),
        "page_start": page_start,
        "page_end": page_end,
        "page_range": f"{page_start}-{page_end}" if page_end != page_start else str(page_start),
        "section": "",
        "excerpt": excerpt,
    }


# =============================================================================
# Audit Log Functions
# =============================================================================

_AUDIT_ENTITY_TYPES = {"document", "page", "chunk", "setting", "catalog"}


def log_audit(
    workflow_id: str,
    document_id: str,
    action_type: str,
    entity_type: Optional[str] = None,
    entity_id: Optional[int] = None,
    field_name: Optional[str] = None,
    old_value: Optional[str] = None,
    new_value: Optional[str] = None,
    metadata: Optional[dict] = None,
    actor_user_id: Optional[str] = None,
    actor_username: Optional[str] = None,
    actor_email: Optional[str] = None,
    actor_roles: Optional[str] = None,
) -> str:
    """
    Log an audit entry.

    Args:
        workflow_id: The Temporal workflow ID (or document identifier). Ids that
            start with ``__`` mark system-level entries and are not stored.
        document_id: Unused; kept for call-site compatibility. The document is
            resolved from ``workflow_id``.
        action_type: Type of action (stage_change, page_edit, chunk_edit, approval, reset)
        entity_type: Type of entity (document, page, chunk, setting, catalog)
        entity_id: Page number or chunk number the action applied to
        field_name: Name of the field that changed
        old_value: Previous value (as string or JSON)
        new_value: New value (as string or JSON)
        metadata: Additional context as dict
        actor_*: Identity of the human/system that performed the action

    Returns:
        The UUID of the created audit log entry
    """
    entity = entity_type if entity_type in _AUDIT_ENTITY_TYPES else None
    system_entry = (workflow_id or "").startswith("__")
    roles = _csv_roles(actor_roles)
    with get_connection() as conn:
        doc_uuid = None if system_entry else _resolve_document_uuid(conn, workflow_id)
        group_uuid = None
        stored_workflow_id = None if system_entry else workflow_id
        if doc_uuid:
            doc_row = conn.execute(
                "SELECT c.group_id FROM documents d JOIN catalogs c ON c.id = d.catalog_id "
                "WHERE d.id = %s::uuid",
                (doc_uuid,),
            ).fetchone()
            group_uuid = str(doc_row["group_id"])
            if _is_uuid(workflow_id):
                stored_workflow_id = _root_workflow_id(conn, doc_uuid) or workflow_id
        if stored_workflow_id is None and entity not in ("setting", "catalog"):
            stored_workflow_id = workflow_id
        actor_uuid = (
            upsert_user(actor_user_id, actor_username, actor_email, roles, conn=conn)
            if actor_user_id else None
        )
        row = conn.execute(
            """
            INSERT INTO audit_logs (document_id, workflow_id, group_id, action_type, entity_type,
                                    entity_number, field_name, old_value, new_value, metadata,
                                    actor_user_id, actor)
            VALUES (%s::uuid, %s, %s::uuid, %s, %s, %s, %s, %s, %s, %s, %s::uuid, %s)
            RETURNING id
            """,
            (
                doc_uuid, stored_workflow_id, group_uuid, action_type, entity, entity_id,
                field_name, old_value, new_value, Jsonb(metadata or {}), actor_uuid,
                Jsonb({"username": actor_username or None, "email": actor_email or None, "roles": roles})
                if (actor_user_id or actor_username or actor_email or roles) else Jsonb({}),
            ),
        ).fetchone()
        return str(row["id"])


# Keycloak attaches these to every account regardless of what the user can
# actually do here. Rendering them made the actor column a wall of noise
# ("...manage-account,manage-account-links,offline_access,uma_authorization...")
# that buried the one role a reviewer cares about.
_NOISE_ROLES = {
    "manage-account",
    "manage-account-links",
    "offline_access",
    "uma_authorization",
    "view-profile",
    "account",
}

# Most-privileged first, so a multi-role account is labelled by what it can do.
_ROLE_PRECEDENCE = (
    "superadmin",
    "super_admin",
    "master_admin",
    "admin",
    "content_curator",
    "reviewer",
    "contributor",
    "viewer",
)


def meaningful_roles(raw: Optional[str]) -> list[str]:
    """Application roles from a stored role list, Keycloak built-ins removed."""
    parts = [p.strip() for p in (raw or "").split(",") if p.strip()]
    kept = [p for p in parts if p.lower() not in _NOISE_ROLES and not p.lower().startswith("default-roles")]
    ranked = [r for r in _ROLE_PRECEDENCE if r in {k.lower() for k in kept}]
    if ranked:
        # Preserve original casing of the first matching role.
        first = next(k for k in kept if k.lower() == ranked[0])
        return [first]
    return kept[:1]


def _normalize_audit_row(row) -> dict:
    entry = dict(row)
    email = (entry.get("actor_email") or "").strip()
    username = (entry.get("actor_username") or "").strip()
    roles = meaningful_roles(entry.get("actor_roles"))
    role_label = roles[0] if roles else ""

    who = email or username
    if who and role_label:
        entry["actor"] = f"{who} ({role_label})"
    elif who:
        entry["actor"] = who
    else:
        entry["actor"] = "system"

    # Split out so the UI can render identity and role separately rather than
    # parsing the combined string back apart.
    entry["actor_display"] = who or "System"
    entry["actor_role"] = role_label
    entry["is_system"] = not who
    return entry


_AUDIT_SELECT = """
    SELECT a.id, a.workflow_id, a.action_type, a.entity_type, a.entity_number, a.field_name,
           a.old_value, a.new_value, a.metadata, a.created_at, a.actor, ag.name AS audit_group,
           au.keycloak_user_id AS actor_keycloak_id,
           d.source->>'fingerprint' AS doc_fingerprint,
           d.source->>'original_filename' AS filename,
           d.name AS document_name,
           dg.name AS doc_group,
           uu.username AS uploaded_by_username, uu.email AS uploaded_by_email
    FROM audit_logs a
    LEFT JOIN documents d ON d.id = a.document_id
    LEFT JOIN groups ag ON ag.id = a.group_id
    LEFT JOIN catalogs dc ON dc.id = d.catalog_id
    LEFT JOIN groups dg ON dg.id = dc.group_id
    LEFT JOIN users au ON au.id = a.actor_user_id
    LEFT JOIN users uu ON uu.id = NULLIF(d.metadata->>'uploaded_by_user_id', '')::uuid
"""


def _audit_row_to_legacy(row: dict) -> dict:
    actor = row["actor"] or {}
    metadata = row["metadata"] or {}
    entity_type = row["entity_type"]
    if entity_type == "setting":
        workflow_id, document_id = "__system__", "__settings__"
    elif entity_type == "catalog" and not row["workflow_id"]:
        workflow_id, document_id = "__catalog__", "__catalog__"
    else:
        workflow_id = row["workflow_id"]
        document_id = row["doc_fingerprint"] or workflow_id
    roles = actor.get("roles") or []
    entry = {
        "id": row["id"],
        "workflow_id": workflow_id,
        "document_id": document_id,
        "action_type": row["action_type"],
        "entity_type": entity_type,
        "entity_id": row["entity_number"],
        "field_name": row["field_name"],
        "old_value": row["old_value"],
        "new_value": row["new_value"],
        "metadata": json.dumps(metadata) if metadata else None,
        "timestamp": row["created_at"],
        "actor_user_id": row["actor_keycloak_id"],
        "actor_username": actor.get("username"),
        "actor_email": actor.get("email"),
        "actor_roles": ",".join(roles) if roles else None,
        "filename": row["filename"],
        "name": row["document_name"],
        "group": row["doc_group"] or row["audit_group"],
        "uploaded_by_username": row["uploaded_by_username"],
        "uploaded_by_email": row["uploaded_by_email"],
    }
    return _normalize_audit_row({k: _clean(v) for k, v in entry.items()})


def _audit_scope(workflow_id: str, conn) -> tuple[str, list]:
    doc_uuid = _resolve_document_uuid(conn, workflow_id)
    if doc_uuid:
        root = _root_workflow_id(conn, doc_uuid) or workflow_id
        return "(a.document_id = %s::uuid OR a.workflow_id = %s)", [doc_uuid, root]
    return "a.workflow_id = %s", [workflow_id]


def get_audit_logs(
    workflow_id: str,
    action_type: Optional[str] = None,
    limit: int = 100,
    offset: int = 0
) -> list[dict]:
    """
    Get audit logs for a document.

    Args:
        workflow_id: The Temporal workflow ID (or document UUID)
        action_type: Optional filter by action type
        limit: Maximum number of entries to return
        offset: Offset for pagination

    Returns:
        List of audit log entries as dicts
    """
    with get_connection() as conn:
        where, params = _audit_scope(workflow_id, conn)
        if action_type:
            where += " AND a.action_type = %s"
            params.append(action_type)
        rows = conn.execute(
            f"{_AUDIT_SELECT} WHERE {where} ORDER BY a.created_at DESC, a.id LIMIT %s OFFSET %s",
            (*params, limit, offset),
        ).fetchall()
    return [_audit_row_to_legacy(r) for r in rows]


def get_audit_log_count(workflow_id: str, action_type: Optional[str] = None) -> int:
    """Get count of audit logs for a document."""
    with get_connection() as conn:
        where, params = _audit_scope(workflow_id, conn)
        if action_type:
            where += " AND a.action_type = %s"
            params.append(action_type)
        row = conn.execute(f"SELECT COUNT(*) AS cnt FROM audit_logs a WHERE {where}", params).fetchone()
    return int(row["cnt"]) if row else 0


def _global_audit_filters(
    action_type: Optional[str], groups: Optional[list[str]]
) -> tuple[list[str], list]:
    where_parts: list[str] = ["a.action_type <> %s"]
    params: list = [CATALOG_VERSION_ACTION]
    if action_type:
        where_parts.append("a.action_type = %s")
        params.append(action_type)
    if groups is not None:
        where_parts.append("lower(COALESCE(ag.name, dg.name, %s)) = ANY(%s)")
        params.extend([_default_group(), _normalize_groups(groups)])
    return where_parts, params


def get_all_audit_logs(
    action_type: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
    groups: Optional[list[str]] = None,
) -> list[dict]:
    """
    Get all audit logs across all documents.

    Args:
        action_type: Optional filter by action type
        limit: Maximum number of entries to return
        offset: Offset for pagination
        groups: Tenant scope (None = all, [] = none)

    Returns:
        List of audit log entries as dicts, with document filename included
    """
    if groups is not None and not _normalize_groups(groups):
        return []
    where_parts, params = _global_audit_filters(action_type, groups)
    with get_connection() as conn:
        rows = conn.execute(
            f"{_AUDIT_SELECT} WHERE {' AND '.join(where_parts)} "
            "ORDER BY a.created_at DESC, a.id LIMIT %s OFFSET %s",
            (*params, limit, offset),
        ).fetchall()
    return [_audit_row_to_legacy(r) for r in rows]


def get_all_audit_log_count(
    action_type: Optional[str] = None,
    groups: Optional[list[str]] = None,
) -> int:
    """Get total count of all audit logs (optionally tenant-scoped)."""
    if groups is not None and not _normalize_groups(groups):
        return 0
    where_parts, params = _global_audit_filters(action_type, groups)
    with get_connection() as conn:
        row = conn.execute(
            "SELECT COUNT(*) AS cnt FROM audit_logs a "
            "LEFT JOIN documents d ON d.id = a.document_id "
            "LEFT JOIN groups ag ON ag.id = a.group_id "
            "LEFT JOIN catalogs dc ON dc.id = d.catalog_id "
            "LEFT JOIN groups dg ON dg.id = dc.group_id "
            f"WHERE {' AND '.join(where_parts)}",
            params,
        ).fetchone()
    return int(row["cnt"]) if row else 0


# =============================================================================
# Page Functions
# =============================================================================

_PAGE_SELECT = """
    SELECT p.id, p.document_id, p.page_number, p.original_markdown, p.edited_markdown,
           p.translations, p.metadata, p.created_at, p.updated_at
    FROM pages p
"""


def _primary_translation(translations: dict) -> tuple[Optional[str], dict]:
    if not translations:
        return None, {}
    lang = max(
        translations,
        key=lambda k: (translations[k].get("translated_at") or "", k == "en"),
    )
    return lang, translations[lang]


def _page_from_row(row: Optional[dict], workflow_id: str) -> Optional[dict]:
    if row is None:
        return None
    meta = row["metadata"] or {}
    translations = row["translations"] or {}
    lang, tr = _primary_translation(translations)
    page = {
        "id": row["id"],
        "workflow_id": workflow_id,
        "page_number": row["page_number"],
        "original_markdown": row["original_markdown"],
        "edited_markdown": row["edited_markdown"],
        "is_reviewed": bool(meta.get("is_reviewed")),
        "reviewer_notes": meta.get("reviewer_notes"),
        "detected_language": meta.get("detected_language"),
        "translated_markdown": tr.get("translated_markdown"),
        "edited_translation": tr.get("edited_translation"),
        "translation_reviewed": bool(tr.get("is_reviewed")),
        "translation_notes": tr.get("reviewer_notes"),
        "translation_provider": tr.get("provider"),
        "translation_model": tr.get("model"),
        "translation_target_language": lang,
        "translated_at": tr.get("translated_at"),
        "translations": translations,
        "metadata": meta,
    }
    return {k: _clean(v) for k, v in page.items()}


_TRANSLATION_FLAT_KEYS = (
    "translated_markdown", "edited_translation", "translation_notes",
    "translation_provider", "translation_model", "translated_at",
)


def _translation_from_flat(page: dict) -> Optional[tuple[str, dict]]:
    if not any(page.get(k) not in (None, "") for k in _TRANSLATION_FLAT_KEYS) and not page.get(
        "translation_reviewed"
    ):
        return None
    lang = page.get("translation_target_language") or "en"
    entry = PageTranslation(
        translated_markdown=page.get("translated_markdown"),
        edited_translation=page.get("edited_translation"),
        is_reviewed=bool(page.get("translation_reviewed")),
        reviewer_notes=page.get("translation_notes"),
        provider=page.get("translation_provider"),
        model=page.get("translation_model"),
        translated_at=page.get("translated_at"),
    ).model_dump()
    return lang, entry


def save_pages(workflow_id: str, pages: list[dict]):
    """
    Save all pages for a document (bulk upsert).

    Each call replaces the whole row, so the caller passes every field. Page
    ``translations`` are kept as stored unless the page carries ``translations``
    (replaces them) or flat ``translated_*`` fields (merged in under
    ``translation_target_language``); a page whose OCR text changed loses its
    translations because they no longer describe it.
    """
    with get_connection() as conn:
        doc_uuid = _require_document_uuid(conn, workflow_id)
        existing = {
            r["page_number"]: r
            for r in conn.execute(
                "SELECT page_number, original_markdown, translations FROM pages "
                "WHERE document_id = %s::uuid",
                (doc_uuid,),
            ).fetchall()
        }
        for page in pages:
            number = page.get("page_number")
            current = existing.get(number)
            meta = dict(page.get("metadata") or {})
            for flat_key in ("is_reviewed", "reviewer_notes", "detected_language"):
                if flat_key in page:
                    meta[flat_key] = page[flat_key]
            meta["is_reviewed"] = bool(meta.get("is_reviewed"))
            page_meta = PageMetadata.model_validate(
                {k: v for k, v in meta.items() if v is not None}
            ).model_dump(exclude_none=True)

            translations = dict(current["translations"]) if current else {}
            if current and current["original_markdown"] != page.get("original_markdown"):
                translations = {}
            if isinstance(page.get("translations"), dict):
                translations = {
                    lang: PageTranslation.model_validate(entry).model_dump()
                    for lang, entry in page["translations"].items()
                }
            else:
                flat = _translation_from_flat(page)
                if flat:
                    translations[flat[0]] = flat[1]

            conn.execute(
                """
                INSERT INTO pages (document_id, page_number, original_markdown, edited_markdown,
                                   translations, metadata)
                VALUES (%s::uuid, %s, %s, %s, %s, %s)
                ON CONFLICT (document_id, page_number) DO UPDATE SET
                    original_markdown = EXCLUDED.original_markdown,
                    edited_markdown = EXCLUDED.edited_markdown,
                    translations = EXCLUDED.translations,
                    metadata = EXCLUDED.metadata
                """,
                (
                    doc_uuid, number, page.get("original_markdown"), page.get("edited_markdown"),
                    Jsonb(translations), Jsonb(page_meta),
                ),
            )


def persist_document_content(workflow_id: str, pages: list[dict], chunks: list[dict]):
    """Backward-compatible helper to persist pages and chunks in one call."""
    save_pages(workflow_id, pages)
    save_chunks(workflow_id, chunks)


def get_pages(workflow_id: str) -> list[dict]:
    """Get all pages for a document."""
    with get_connection() as conn:
        doc_uuid = _resolve_document_uuid(conn, workflow_id)
        if doc_uuid is None:
            return []
        root = _root_workflow_id(conn, doc_uuid) or workflow_id
        rows = conn.execute(
            _PAGE_SELECT + " WHERE p.document_id = %s::uuid ORDER BY p.page_number", (doc_uuid,)
        ).fetchall()
    return [_page_from_row(r, root) for r in rows]


def count_translated_pages(workflow_id: str) -> int:
    """Count pages with translation text without loading full page bodies."""
    with get_connection() as conn:
        doc_uuid = _resolve_document_uuid(conn, workflow_id)
        if doc_uuid is None:
            return 0
        row = conn.execute(
            """
            SELECT COUNT(*) AS n FROM pages p
            WHERE p.document_id = %s::uuid
              AND EXISTS (
                SELECT 1 FROM jsonb_each(p.translations) t
                WHERE nullif(btrim(t.value->>'translated_markdown'), '') IS NOT NULL
                   OR nullif(btrim(t.value->>'edited_translation'), '') IS NOT NULL
              )
            """,
            (doc_uuid,),
        ).fetchone()
    return int(row["n"] if row else 0)


def get_saved_page_numbers(workflow_id: str) -> list[int]:
    """Get saved page numbers for a document without loading full page content."""
    with get_connection() as conn:
        doc_uuid = _resolve_document_uuid(conn, workflow_id)
        if doc_uuid is None:
            return []
        rows = conn.execute(
            "SELECT page_number FROM pages WHERE document_id = %s::uuid ORDER BY page_number",
            (doc_uuid,),
        ).fetchall()
    return [int(row["page_number"]) for row in rows]


def get_page(workflow_id: str, page_num: int) -> Optional[dict]:
    """Get a specific page."""
    with get_connection() as conn:
        doc_uuid = _resolve_document_uuid(conn, workflow_id)
        if doc_uuid is None:
            return None
        root = _root_workflow_id(conn, doc_uuid) or workflow_id
        row = conn.execute(
            _PAGE_SELECT + " WHERE p.document_id = %s::uuid AND p.page_number = %s",
            (doc_uuid, page_num),
        ).fetchone()
    return _page_from_row(row, root)


def update_page(
    workflow_id: str,
    page_num: int,
    edited_markdown: Optional[str] = None,
    is_reviewed: Optional[bool] = None,
    reviewer_notes: Optional[str] = None,
    edited_translation: Optional[str] = None,
    translation_reviewed: Optional[bool] = None,
    translation_notes: Optional[str] = None,
) -> Optional[dict]:
    """Update a page. Returns updated page or None if not found."""
    with get_connection() as conn:
        doc_uuid = _resolve_document_uuid(conn, workflow_id)
        if doc_uuid is None:
            return None
        existing = conn.execute(
            "SELECT translations FROM pages WHERE document_id = %s::uuid AND page_number = %s FOR UPDATE",
            (doc_uuid, page_num),
        ).fetchone()
        if not existing:
            return None

        assignments: list[str] = []
        params: list = []
        if edited_markdown is not None:
            assignments.append("edited_markdown = %s")
            params.append(edited_markdown)

        meta_patch: dict[str, Any] = {}
        if is_reviewed is not None:
            meta_patch["is_reviewed"] = bool(is_reviewed)
        if reviewer_notes is not None:
            meta_patch["reviewer_notes"] = reviewer_notes
        if meta_patch:
            assignments.append("metadata = metadata || %s::jsonb")
            params.append(Jsonb(meta_patch))

        tr_patch: dict[str, Any] = {}
        if edited_translation is not None:
            tr_patch["edited_translation"] = edited_translation
        if translation_reviewed is not None:
            tr_patch["is_reviewed"] = bool(translation_reviewed)
        if translation_notes is not None:
            tr_patch["reviewer_notes"] = translation_notes
        if tr_patch:
            lang, _ = _primary_translation(existing["translations"] or {})
            lang = lang or "en"
            assignments.append(
                "translations = jsonb_set(translations, ARRAY[%s::text], "
                "COALESCE(translations->%s::text, '{}'::jsonb) || %s::jsonb, TRUE)"
            )
            params.extend([lang, lang, Jsonb(tr_patch)])

        if assignments:
            conn.execute(
                f"UPDATE pages SET {', '.join(assignments)} "
                "WHERE document_id = %s::uuid AND page_number = %s",
                (*params, doc_uuid, page_num),
            )
    return get_page(workflow_id, page_num)


def reset_page(workflow_id: str, page_num: int) -> Optional[dict]:
    """Reset a page to its original OCR markdown."""
    with get_connection() as conn:
        doc_uuid = _resolve_document_uuid(conn, workflow_id)
        if doc_uuid is not None:
            conn.execute(
                """
                UPDATE pages SET
                    edited_markdown = NULL,
                    metadata = (metadata - 'reviewer_notes') || '{"is_reviewed": false}'::jsonb
                WHERE document_id = %s::uuid AND page_number = %s
                """,
                (doc_uuid, page_num),
            )
    return get_page(workflow_id, page_num)


# =============================================================================
# Chunk Functions
# =============================================================================

_CHUNK_SELECT = """
    SELECT c.id, c.document_id, c.chunk_number, c.original_text, c.edited_text, c.token_count,
           c.page_start, c.page_end, c.source_spans, c.section_title, c.content_type,
           c.is_reference, c.is_reviewed, c.is_excluded, c.reviewer_notes,
           c.created_at, c.updated_at, d.metadata->'chunking' AS chunking
    FROM chunks c
    JOIN documents d ON d.id = c.document_id
"""


def _chunk_from_row(row: Optional[dict], workflow_id: str) -> Optional[dict]:
    if row is None:
        return None
    spans = row["source_spans"] or []
    page_numbers = sorted({int(s["page_number"]) for s in spans if "page_number" in s}) or list(
        range(int(row["page_start"]), int(row["page_end"]) + 1)
    )
    run = row.get("chunking") or {}
    chunk = {
        "id": row["id"],
        "workflow_id": workflow_id,
        "chunk_number": row["chunk_number"],
        "original_text": row["original_text"],
        "edited_text": row["edited_text"],
        "token_count": row["token_count"],
        "page_start": row["page_start"],
        "page_end": row["page_end"],
        "source_page_numbers_json": json.dumps(page_numbers),
        "source_spans_json": json.dumps(spans),
        "section_title": row["section_title"],
        "content_type": row["content_type"],
        "is_reference": bool(row["is_reference"]),
        "chunking_provider": run.get("provider"),
        "chunking_model": run.get("model"),
        "chunking_config_json": json.dumps(run["config"]) if run.get("config") else None,
        "chunking_run_id": f"run-{run['run_number']}" if run.get("run_number") else None,
        "chunk_version": run.get("run_number") or 1,
        "is_reviewed": bool(row["is_reviewed"]),
        "is_excluded": bool(row["is_excluded"]),
        "reviewer_notes": row["reviewer_notes"],
    }
    return {k: _clean(v) for k, v in chunk.items()}


def _load_json_value(raw: Any, default: Any) -> Any:
    if raw is None or raw == "":
        return default
    if isinstance(raw, str):
        return json.loads(raw)
    return raw


def save_chunks(workflow_id: str, chunks: list[dict]):
    """
    Save all chunks for a document.

    Re-chunking runs in one transaction: the document's chunks are deleted, the
    new ones inserted, and ``metadata.chunking`` (with ``run_number``
    incremented) updated together.
    """
    with get_connection() as conn:
        doc_uuid = _require_document_uuid(conn, workflow_id)
        previous = conn.execute(
            "SELECT metadata->'chunking'->>'run_number' AS run_number FROM documents "
            "WHERE id = %s::uuid FOR UPDATE",
            (doc_uuid,),
        ).fetchone()
        run_number = int(previous["run_number"] or 0) + 1
        conn.execute("DELETE FROM chunks WHERE document_id = %s::uuid", (doc_uuid,))
        for chunk in chunks:
            spans = _load_json_value(
                chunk.get("source_spans") if chunk.get("source_spans") is not None
                else chunk.get("source_spans_json"),
                [],
            )
            page_start = int(chunk.get("page_start") or 1)
            page_end = max(int(chunk.get("page_end") or page_start), page_start)
            conn.execute(
                """
                INSERT INTO chunks (document_id, chunk_number, original_text, edited_text,
                                    token_count, page_start, page_end, source_spans,
                                    section_title, content_type, is_reference, is_reviewed,
                                    is_excluded, reviewer_notes)
                VALUES (%s::uuid, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    doc_uuid,
                    chunk.get("chunk_number"),
                    chunk.get("original_text") or "",
                    chunk.get("edited_text"),
                    chunk.get("token_count") or 0,
                    page_start,
                    page_end,
                    Jsonb(spans),
                    chunk.get("section_title"),
                    chunk.get("content_type"),
                    bool(chunk.get("is_reference")),
                    bool(chunk.get("is_reviewed")),
                    bool(chunk.get("is_excluded")),
                    chunk.get("reviewer_notes"),
                ),
            )
        meta_patch: dict[str, Any] = {}
        if chunks:
            first = chunks[0]
            meta_patch["chunking"] = ChunkingRun(
                run_number=run_number,
                provider=first.get("chunking_provider"),
                model=first.get("chunking_model"),
                config=_load_json_value(first.get("chunking_config_json"), {}),
                created_at=_iso_now(),
            ).model_dump()
        if meta_patch:
            conn.execute(
                "UPDATE documents SET metadata = metadata || %s::jsonb WHERE id = %s::uuid",
                (Jsonb(meta_patch), doc_uuid),
            )
        else:
            conn.execute("UPDATE documents SET updated_at = now() WHERE id = %s::uuid", (doc_uuid,))


def get_chunks(workflow_id: str, include_excluded: bool = False) -> list[dict]:
    """Get all chunks for a document."""
    with get_connection() as conn:
        doc_uuid = _resolve_document_uuid(conn, workflow_id)
        if doc_uuid is None:
            return []
        root = _root_workflow_id(conn, doc_uuid) or workflow_id
        excluded = "" if include_excluded else "AND NOT c.is_excluded"
        rows = conn.execute(
            f"{_CHUNK_SELECT} WHERE c.document_id = %s::uuid {excluded} ORDER BY c.chunk_number",
            (doc_uuid,),
        ).fetchall()
    return [_chunk_from_row(r, root) for r in rows]


def search_chunks(
    *,
    query: str = "",
    limit: int = 100,
    offset: int = 0,
    include_excluded: bool = False,
    stage: Optional[str] = None,
    groups: Optional[list[str]] = None,
) -> tuple[list[dict], int]:
    """Search chunks across documents for maintainer workflows.

    groups: If provided, only return chunks whose owning document's group
    is in this list (tenant scoping). ``None`` means no group restriction.
    """
    where_clauses = ["TRUE"]
    params: list = []

    if not include_excluded:
        where_clauses.append("NOT c.is_excluded")
    if stage:
        where_clauses.append("cw.current_stage = %s")
        params.append(stage)
    if groups is not None:
        normalized = _normalize_groups(groups)
        if not normalized:
            where_clauses.append("FALSE")
        else:
            where_clauses.append("lower(g.name) = ANY(%s)")
            params.append(normalized)
    if query and query.strip():
        where_clauses.append("lower(coalesce(c.edited_text, c.original_text)) LIKE %s")
        params.append(f"%{query.strip().lower()}%")

    base_from_sql = f"""
        FROM chunks c
        JOIN documents d ON d.id = c.document_id
        JOIN catalogs cat ON cat.id = d.catalog_id
        JOIN groups g ON g.id = cat.group_id
        LEFT JOIN document_current_workflow cw ON cw.document_id = d.id
        WHERE {' AND '.join(where_clauses)}
    """
    with get_connection() as conn:
        total_row = conn.execute(f"SELECT COUNT(*) AS cnt {base_from_sql}", params).fetchone()
        total = int(total_row["cnt"] if total_row and total_row["cnt"] is not None else 0)
        rows = conn.execute(
            f"""
            SELECT
                c.id, c.chunk_number, c.original_text, c.edited_text, c.token_count,
                c.page_start, c.page_end, c.source_spans, c.section_title, c.content_type,
                c.is_reference, c.is_reviewed, c.is_excluded, c.reviewer_notes,
                d.metadata->'chunking' AS chunking,
                d.source->>'fingerprint' AS document_id,
                d.source->>'original_filename' AS filename,
                d.name AS name,
                cw.current_stage AS stage,
                d.updated_at AS document_updated_at,
                (SELECT r.temporal_workflow_id FROM workflows r WHERE r.document_id = d.id
                 ORDER BY (r.workflow_type IN ('pipeline', 'ocr_only')) DESC, r.started_at ASC, r.id ASC
                 LIMIT 1) AS root_workflow_id
            {base_from_sql}
            ORDER BY d.updated_at DESC, d.id, c.chunk_number
            LIMIT %s OFFSET %s
            """,
            [*params, int(limit), int(offset)],
        ).fetchall()

    chunks: list[dict] = []
    for row in rows:
        row = dict(row)
        chunk = _chunk_from_row(row, row["root_workflow_id"])
        for extra in ("document_id", "filename", "name", "stage", "document_updated_at"):
            chunk[extra] = _clean(row[extra])
        chunks.append(chunk)
    return chunks, total


def get_chunk(workflow_id: str, chunk_num: int) -> Optional[dict]:
    """Get a specific chunk."""
    with get_connection() as conn:
        doc_uuid = _resolve_document_uuid(conn, workflow_id)
        if doc_uuid is None:
            return None
        root = _root_workflow_id(conn, doc_uuid) or workflow_id
        row = conn.execute(
            _CHUNK_SELECT + " WHERE c.document_id = %s::uuid AND c.chunk_number = %s",
            (doc_uuid, chunk_num),
        ).fetchone()
    return _chunk_from_row(row, root)


def update_chunk(
    workflow_id: str,
    chunk_num: int,
    edited_text: Optional[str] = None,
    is_reviewed: Optional[bool] = None,
    is_excluded: Optional[bool] = None,
    reviewer_notes: Optional[str] = None
) -> Optional[dict]:
    """Update a chunk. Returns updated chunk or None if not found."""
    with get_connection() as conn:
        doc_uuid = _resolve_document_uuid(conn, workflow_id)
        if doc_uuid is None:
            return None
        existing = conn.execute(
            "SELECT id FROM chunks WHERE document_id = %s::uuid AND chunk_number = %s",
            (doc_uuid, chunk_num),
        ).fetchone()
        if not existing:
            return None

        updates = []
        values: list = []
        if edited_text is not None:
            updates.append("edited_text = %s")
            values.append(edited_text)
        if is_reviewed is not None:
            updates.append("is_reviewed = %s")
            values.append(bool(is_reviewed))
        if is_excluded is not None:
            updates.append("is_excluded = %s")
            values.append(bool(is_excluded))
        if reviewer_notes is not None:
            updates.append("reviewer_notes = %s")
            values.append(reviewer_notes)
        if updates:
            conn.execute(
                f"UPDATE chunks SET {', '.join(updates)} WHERE document_id = %s::uuid AND chunk_number = %s",
                (*values, doc_uuid, chunk_num),
            )
    return get_chunk(workflow_id, chunk_num)


def reset_chunk(workflow_id: str, chunk_num: int) -> Optional[dict]:
    """Reset a chunk to its original text."""
    with get_connection() as conn:
        doc_uuid = _resolve_document_uuid(conn, workflow_id)
        if doc_uuid is not None:
            conn.execute(
                """
                UPDATE chunks SET edited_text = NULL, is_reviewed = FALSE, is_excluded = FALSE,
                                  reviewer_notes = NULL
                WHERE document_id = %s::uuid AND chunk_number = %s
                """,
                (doc_uuid, chunk_num),
            )
    return get_chunk(workflow_id, chunk_num)


# =============================================================================
# Settings Functions
# =============================================================================

_SETTING_GROUPS: dict[str, tuple[str, type]] = {
    "searchMethod": ("retrieval", RetrievalSettings),
    "alpha": ("retrieval", RetrievalSettings),
    "rankingMethod": ("retrieval", RetrievalSettings),
    "hybridRrfK": ("retrieval", RetrievalSettings),
    "indexName": ("candidates", CandidateSettings),
    "efSearch": ("candidates", CandidateSettings),
    "candidateCap": ("candidates", CandidateSettings),
    "candidateMultiplier": ("candidates", CandidateSettings),
    "limit": ("results", ResultSettings),
    "maxChunksPerDoc": ("results", ResultSettings),
    "excludeReference": ("results", ResultSettings),
    "showHighlights": ("results", ResultSettings),
    "rerankMode": ("results", ResultSettings),
    "useE5Prefix": ("query", QuerySettings),
    "queryExpansionProfile": ("query", QuerySettings),
}

_GROUP_MODELS: dict[str, type] = {group: model for group, model in _SETTING_GROUPS.values()}

_SETTINGS_ROW_SQL = (
    "SELECT retrieval, candidates, results, query FROM search_settings WHERE singleton"
)


def _settings_row(conn, lock: bool = False) -> dict:
    sql = _SETTINGS_ROW_SQL + (" FOR UPDATE" if lock else "")
    row = conn.execute(sql).fetchone()
    if row is None:
        conn.execute("INSERT INTO search_settings DEFAULT VALUES ON CONFLICT (singleton) DO NOTHING")
        row = conn.execute(sql).fetchone()
    return row


def _flatten_settings(row: dict) -> dict:
    flat: dict[str, Any] = {}
    for group, model in _GROUP_MODELS.items():
        flat.update(model.model_validate(row[group] or {}).model_dump())
    return flat


def get_search_settings() -> dict:
    """Get all search settings as a flat dict (stored values merged over defaults)."""
    with get_connection() as conn:
        return _flatten_settings(_settings_row(conn))


def _setting_text(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def update_search_settings(settings: dict) -> dict:
    """
    Update multiple search settings at once.

    Args:
        settings: Dict with keys like searchMethod, limit, alpha, etc.

    Returns:
        Updated search settings

    Raises:
        ValueError: a value fails the setting's validation rules.
    """
    unknown = sorted(set(settings) - set(_SETTING_GROUPS))
    if unknown:
        raise ValueError(f"Unknown search settings: {', '.join(unknown)}")
    changes: list[tuple[str, str, str]] = []
    with get_connection() as conn:
        row = _settings_row(conn, lock=True)
        before = _flatten_settings(row)
        for group, model in _GROUP_MODELS.items():
            patch = {k: v for k, v in settings.items() if _SETTING_GROUPS[k][0] == group}
            if not patch:
                continue
            try:
                validated = model.model_validate({**model.model_validate(row[group] or {}).model_dump(), **patch})
            except Exception as exc:
                raise ValueError(str(exc)) from exc
            stored = {**(row[group] or {}), **{k: getattr(validated, k) for k in patch}}
            conn.execute(
                f"UPDATE search_settings SET {group} = {group} || %s::jsonb WHERE singleton",
                (Jsonb({k: getattr(validated, k) for k in patch}),),
            )
            for key in patch:
                if before[key] != stored[key]:
                    changes.append((f"{group}.{key}", _setting_text(before[key]), _setting_text(stored[key])))
        for field_name, old_value, new_value in changes:
            conn.execute(
                """
                INSERT INTO audit_logs (action_type, entity_type, field_name, old_value, new_value)
                VALUES ('settings_change', 'setting', %s, %s, %s)
                """,
                (field_name, old_value, new_value),
            )
        return _flatten_settings(
            conn.execute(_SETTINGS_ROW_SQL).fetchone()
        )


def get_settings_audit_logs(limit: int = 50, offset: int = 0) -> list[dict]:
    """Get audit logs for settings changes."""
    with get_connection() as conn:
        rows = conn.execute(
            f"{_AUDIT_SELECT} WHERE a.entity_type = 'setting' "
            "ORDER BY a.created_at DESC, a.id LIMIT %s OFFSET %s",
            (limit, offset),
        ).fetchall()
    return [_audit_row_to_legacy(r) for r in rows]


def get_settings_audit_count() -> int:
    """Get count of settings audit logs."""
    with get_connection() as conn:
        row = conn.execute(
            "SELECT COUNT(*) AS cnt FROM audit_logs WHERE entity_type = 'setting'"
        ).fetchone()
    return int(row["cnt"]) if row else 0
