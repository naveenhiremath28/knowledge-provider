#!/usr/bin/env python3
"""One-off migration of the legacy SQLite store into Postgres.

Run in the maintenance window, after the Postgres migrations are applied and
with the api and worker stopped. The SQLite file is opened read-only and is
never modified, so rolling back is redeploying the previous images on it.

    python scripts/migrate_sqlite_to_postgres.py --dry-run
    python scripts/migrate_sqlite_to_postgres.py

Every migrated row gets a deterministic UUID derived from its legacy key, so a
second run finds the rows already there and changes nothing. A document is
migrated in one transaction. ``--dry-run`` runs everything and rolls it back.

Mapping (Schema v2): one legacy ``documents`` row becomes one document plus
its main workflow (``temporal_workflow_id`` = the legacy ``workflow_id``); each
``document_jobs`` row becomes a workflow. The legacy ``instance`` becomes a
``groups`` row (``bv`` is ``/global``, others ``/states/<CODE>``, Keycloak id
filled in by the first login sync) and the group's ``catalogs`` row for the
document's kind. ``scheme_code`` / ``scheme_name`` / ``scheme_aliases_json``
(or the ``scheme_catalog_entries`` registry entry for the code) become the
document's ``code`` / ``name`` / ``aliases``; the validity period, uploader and
vector index state move into ``documents.metadata``. Not migrated, and listed in
the report: documents of kind ``document`` (no catalog), demo documents
(``is_demo``), registry entries no document uses (bootstrap-only), and
``document_manifest_entries``.
"""

import argparse
import json
import os
import re
import sqlite3
import sys
import uuid
from collections import Counter
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import psycopg  # noqa: E402
from psycopg.rows import dict_row  # noqa: E402
from psycopg.types.json import Jsonb  # noqa: E402

from pipeline import db, migrate  # noqa: E402
from pipeline.models import (  # noqa: E402
    ChunkingRun,
    DocumentMetadata,
    DocumentSource,
    IndexState,
    PageMetadata,
    PageTranslation,
)
from pipeline.network_constants import KIND_TO_CATALOG_TYPE, catalog_type_for_kind  # noqa: E402

_NAMESPACE = uuid.UUID("3f2b6c0e-5d1a-4a53-9a77-0c6a5c1d7e10")
_FINGERPRINT_RE = re.compile(r"^[0-9a-f]{32}$")
_CODE_RE = re.compile(r"^[a-z0-9]+(-[a-z0-9]+)*$")
_STAGES = {
    "registered", "ocr_processing", "ocr_review", "translation_processing",
    "translation_review", "chunking", "chunk_review", "ready_for_ingestion",
    "ingesting", "publishing_to_network", "approval_for_prod", "ingesting_prod",
    "completed", "failed",
}
_WORKFLOW_TYPES = {
    "pipeline", "ocr_only", "ocr_retry", "translation_retry", "chunking_retry",
    "reingest", "promote_prod",
}
_WORKFLOW_STATUSES = {"running", "waiting_review", "completed", "failed"}
_REVIEW_STAGES = {
    "ocr_review", "translation_review", "chunk_review", "ready_for_ingestion", "approval_for_prod",
}
_ENTITY_TYPES = {"document", "page", "chunk", "setting", "catalog"}

_LEGACY_SETTING_KEYS = {
    "search_method": "searchMethod",
    "search_limit": "limit",
    "search_alpha": "alpha",
    "search_ranking_method": "rankingMethod",
    "search_show_highlights": "showHighlights",
    "search_ef_search": "efSearch",
    "search_index_name": "indexName",
    "search_candidate_cap": "candidateCap",
    "search_candidate_multiplier": "candidateMultiplier",
    "search_max_chunks_per_doc": "maxChunksPerDoc",
    "search_use_e5_prefix": "useE5Prefix",
    "search_exclude_reference": "excludeReference",
    "search_query_expansion_profile": "queryExpansionProfile",
    "search_rerank_mode": "rerankMode",
    "search_hybrid_rrfk": "hybridRrfK",
}
_BOOL_SETTINGS = {"showHighlights", "useE5Prefix", "excludeReference"}
_INT_SETTINGS = {
    "limit", "efSearch", "candidateCap", "candidateMultiplier", "maxChunksPerDoc", "hybridRrfK",
}
_FLOAT_SETTINGS = {"alpha"}


class Report:
    """Per-table counts: rows read from SQLite and what became of them."""

    def __init__(self) -> None:
        self.tables: dict[str, Counter] = {}
        self.skip_reasons: list[dict[str, Any]] = []
        self.notes: list[str] = []

    def count(self, table: str, outcome: str, n: int = 1) -> None:
        self.tables.setdefault(table, Counter())[outcome] += n

    def skip(self, table: str, key: str, reason: str) -> None:
        self.count(table, "skipped")
        self.skip_reasons.append({"table": table, "key": key, "reason": reason})

    def mismatches(self) -> list[str]:
        problems = []
        for table, counts in sorted(self.tables.items()):
            accounted = counts["inserted"] + counts["existing"] + counts["skipped"] + counts["dropped"]
            if counts["read"] != accounted:
                problems.append(f"{table}: read {counts['read']} but accounted for {accounted}")
        return problems

    def as_dict(self) -> dict[str, Any]:
        return {
            "tables": {t: dict(c) for t, c in sorted(self.tables.items())},
            "skipped": self.skip_reasons,
            "notes": self.notes,
            "mismatches": self.mismatches(),
        }

    def render(self) -> str:
        lines = [f"{'table':<28}{'read':>8}{'inserted':>10}{'existing':>10}{'skipped':>9}{'dropped':>9}"]
        for table, c in sorted(self.tables.items()):
            lines.append(
                f"{table:<28}{c['read']:>8}{c['inserted']:>10}{c['existing']:>10}"
                f"{c['skipped']:>9}{c['dropped']:>9}"
            )
        for item in self.skip_reasons:
            lines.append(f"  skipped {item['table']} {item['key']}: {item['reason']}")
        lines.extend(f"  note: {n}" for n in self.notes)
        problems = self.mismatches()
        lines.append("verification: OK" if not problems else "verification: FAILED")
        lines.extend(f"  {p}" for p in problems)
        return "\n".join(lines)


def det_id(kind: str, key: str) -> str:
    return str(uuid.uuid5(_NAMESPACE, f"{kind}:{key}"))


def parse_ts(value: Any) -> Optional[datetime]:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def parse_date(value: Any) -> Optional[date]:
    if not value:
        return None
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def load_json(text: Any, default: Any) -> Any:
    if text in (None, ""):
        return default
    try:
        return json.loads(text)
    except (TypeError, ValueError):
        return default


def truthy(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    return bool(int(value))


def _default_instance() -> str:
    return (os.environ.get("DEFAULT_INSTANCE") or "default").strip().lower() or "default"


def _workflow_status_for_stage(stage: str) -> str:
    if stage == "completed":
        return "completed"
    if stage == "failed":
        return "failed"
    return "waiting_review" if stage in _REVIEW_STAGES else "running"


def _index_key(index_name: str) -> str:
    prod_names = {
        n.strip()
        for n in (
            os.environ.get("PROD_VECTOR_DB_COLLECTION_NAME"),
            os.environ.get("PROD_SCHEME_VECTOR_DB_COLLECTION_NAME"),
        )
        if n and n.strip()
    }
    return "prod" if index_name in prod_names else "dev"


def _has_table(sq: sqlite3.Connection, name: str) -> bool:
    return sq.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (name,)
    ).fetchone() is not None


def _rows(sq: sqlite3.Connection, table: str, where: str = "", params: tuple = ()) -> list[sqlite3.Row]:
    if not _has_table(sq, table):
        return []
    return sq.execute(f"SELECT * FROM {table} {where}", params).fetchall()


def _get(row: sqlite3.Row, key: str, default: Any = None) -> Any:
    return row[key] if key in row.keys() else default


# -----------------------------------------------------------------------------
# Catalog
# -----------------------------------------------------------------------------

def load_catalog_registry(sq, report: Report) -> dict[str, dict[str, Any]]:
    """Legacy ``scheme_catalog_entries`` by code. Their name and aliases are merged
    into the documents holding the code; see ``settle_catalog_registry``."""
    registry: dict[str, dict[str, Any]] = {}
    for row in _rows(sq, "scheme_catalog_entries"):
        report.count("scheme_catalog_entries", "read")
        code = (row["scheme_code"] or "").strip().lower()
        if not _CODE_RE.match(code):
            report.skip("scheme_catalog_entries", str(row["scheme_code"]), "code is not a valid slug")
            continue
        aliases = [a for a in load_json(_get(row, "scheme_aliases_json"), []) if isinstance(a, str) and a]
        registry[code] = {"name": row["scheme_name"] or code, "aliases": aliases}
    return registry


def settle_catalog_registry(registry: dict[str, dict[str, Any]], consumed: set[str], report: Report) -> None:
    """Registry entries a migrated document used count as inserted (merged into the
    document); the rest have no document and are dropped."""
    unused = []
    for code in sorted(registry):
        if code in consumed:
            report.count("scheme_catalog_entries", "inserted")
        else:
            report.count("scheme_catalog_entries", "dropped")
            unused.append(code)
    if unused:
        report.notes.append(
            f"scheme_catalog_entries without a document were not migrated: {', '.join(unused)}"
        )


def migrate_catalog_version(sq, conn, report: Report) -> None:
    rows = _rows(sq, "scheme_catalog_meta")
    legacy_version = int(rows[0]["version"] or 0) if rows else 0
    if rows:
        report.count("scheme_catalog_meta", "read")
        report.count("scheme_catalog_meta", "inserted")
    for n in range(1, legacy_version + 1):
        conn.execute(
            """
            INSERT INTO audit_logs (id, action_type, entity_type, metadata)
            VALUES (%s::uuid, %s, 'catalog', %s) ON CONFLICT (id) DO NOTHING
            """,
            (det_id("catalog-bump", str(n)), db.CATALOG_VERSION_ACTION,
             Jsonb({"reason": "sqlite_migration:carried_over"})),
        )
    conn.execute(
        """
        INSERT INTO audit_logs (id, action_type, entity_type, metadata)
        VALUES (%s::uuid, %s, 'catalog', %s) ON CONFLICT (id) DO NOTHING
        """,
        (det_id("catalog-bump", "migration"), db.CATALOG_VERSION_ACTION,
         Jsonb({"reason": "sqlite_migration"})),
    )


def _instance_of(doc: sqlite3.Row) -> str:
    return (_get(doc, "instance") or _default_instance()).strip().lower() or _default_instance()


def _group_for(conn, instance: str) -> str:
    """The ``groups`` row of a tenant code, created with a deterministic id."""
    path = db._tenant_path(instance)
    row = conn.execute(
        """
        INSERT INTO groups (id, name, path) VALUES (%s::uuid, %s, %s)
        ON CONFLICT (path) DO UPDATE SET name = groups.name RETURNING id
        """,
        (det_id("group", path), db._tenant_code_from_path(path), path),
    ).fetchone()
    return str(row["id"])


def _catalog_for(conn, group_id: str, kind: str) -> str:
    """The ``(group, kind)`` catalog, created with a deterministic id."""
    catalog_type = catalog_type_for_kind(kind)
    row = conn.execute(
        """
        INSERT INTO catalogs (id, group_id, type, name)
        SELECT %s::uuid, g.id, %s, g.name || ' ' || %s FROM groups g WHERE g.id = %s::uuid
        ON CONFLICT (group_id, type) DO UPDATE SET type = EXCLUDED.type RETURNING id
        """,
        (det_id("catalog", f"{group_id}:{catalog_type}"), catalog_type, kind, group_id),
    ).fetchone()
    return str(row["id"])


def _slug(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", (text or "").lower()).strip("-")[:40].strip("-")


def _unique_code(conn, catalog_id: str, base: str) -> str:
    code, n = base, 1
    while conn.execute(
        "SELECT 1 FROM documents WHERE catalog_id = %s::uuid AND code = %s", (catalog_id, code)
    ).fetchone():
        n += 1
        code = f"{base}-{n}"
    return code


# -----------------------------------------------------------------------------
# Documents
# -----------------------------------------------------------------------------

def _user_for(conn, user_id, username, email, roles) -> Optional[str]:
    if not (user_id or "").strip():
        return None
    return db.upsert_user(user_id, username, email, roles, conn=conn)


def _build_index_status(sq, workflow_id: str) -> dict[str, dict]:
    entries: dict[str, dict] = {}
    for row in _rows(sq, "document_index_status", "WHERE workflow_id = ?", (workflow_id,)):
        key = _index_key(row["index_name"])
        details = load_json(row["details_json"], {})
        state = IndexState(
            status=row["status"] if row["status"] in ("unknown", "indexed", "missing") else "unknown",
            index_name=row["index_name"],
            doc_id=_get(row, "doc_id") or _get(row, "marqo_doc_id"),
            chunk_count_indexed=max(int(row["chunk_count_indexed"] or 0), 0),
            last_indexed_at=row["last_indexed_at"],
            last_verified_at=row["last_verified_at"],
            schema_version=row["schema_version"],
            details=details if isinstance(details, dict) else {},
        ).model_dump()
        current = entries.get(key)
        if current is None or (state["last_indexed_at"] or "") >= (current["last_indexed_at"] or ""):
            entries[key] = state
    return entries


def _build_workflows(
    sq, doc: sqlite3.Row, started_by: Optional[str], report: Report
) -> tuple[list[dict], dict[Any, str]]:
    workflow_id = doc["workflow_id"]
    stage = doc["stage"] if doc["stage"] in _STAGES else "registered"
    created_at = parse_ts(doc["created_at"]) or datetime.now(timezone.utc)
    stamps = {
        key: value
        for key, value in (
            ("ocr", _get(doc, "ocr_completed_at")),
            ("translation", _get(doc, "translation_completed_at")),
            ("chunks", _get(doc, "chunks_completed_at")),
        )
        if value
    }

    workflows: list[dict] = []
    job_ids: dict[Any, str] = {}
    has_root = False
    for job in _rows(sq, "document_jobs", "WHERE workflow_id = ? ORDER BY started_at, id", (workflow_id,)):
        job_type = job["job_type"]
        if job_type not in _WORKFLOW_TYPES:
            report.skip("document_jobs", f"{workflow_id}#{job['id']}", f"unknown job_type {job_type!r}")
            continue
        temporal_id = job["temporal_workflow_id"] or workflow_id
        is_root = job_type in ("pipeline", "ocr_only") and temporal_id == workflow_id and not has_root
        has_root = has_root or is_root
        wf_id = det_id("workflow", f"{workflow_id}:job:{job['id']}")
        job_ids[job["id"]] = wf_id
        status = job["status"] if job["status"] in _WORKFLOW_STATUSES else "failed"
        job_stage = job["current_stage"] if job["current_stage"] in _STAGES else stage
        config = load_json(job["config_json"], {})
        config = config if isinstance(config, dict) else {}
        if job["temporal_run_id"]:
            config["temporal_run_id"] = job["temporal_run_id"]
        workflows.append({
            "id": wf_id, "type": job_type, "temporal_id": temporal_id, "status": status,
            "stage": job_stage, "stamps": stamps if is_root else {},
            "stop_after_ocr": truthy(_get(doc, "stop_after_ocr")) and is_root,
            "error_message": job["error_message"], "config": config,
            "started_by": started_by if is_root else None,
            "started_at": parse_ts(job["started_at"]) or created_at,
            "completed_at": parse_ts(job["completed_at"]), "job_id": job["id"],
            "inserted_as": "document_jobs",
        })

    if not has_root:
        workflows.append({
            "id": det_id("workflow", f"{workflow_id}:root"),
            "type": "ocr_only" if truthy(_get(doc, "stop_after_ocr")) else "pipeline",
            "temporal_id": workflow_id, "status": _workflow_status_for_stage(stage), "stage": stage,
            "stamps": stamps, "stop_after_ocr": truthy(_get(doc, "stop_after_ocr")),
            "error_message": doc["error_message"], "config": {}, "started_by": started_by,
            "started_at": created_at,
            "completed_at": parse_ts(_get(doc, "ingested_at")) if stage == "completed" else None,
            "job_id": None, "inserted_as": None,
        })

    requested_at = parse_ts(_get(doc, "prod_ready_requested_at"))
    if requested_at and stage not in ("completed", "ingesting_prod"):
        workflows.append({
            "id": det_id("workflow", f"{workflow_id}:prod-request"), "type": "promote_prod",
            "temporal_id": f"{workflow_id}-prod-request", "status": "waiting_review",
            "stage": "approval_for_prod", "stamps": {}, "stop_after_ocr": False,
            "error_message": None, "config": {}, "started_by": None, "started_at": requested_at,
            "completed_at": None, "job_id": None, "inserted_as": None,
            "requested_by": (
                _get(doc, "prod_ready_requested_by_user_id"),
                _get(doc, "prod_ready_requested_by_username"),
            ),
        })

    workflows.sort(key=lambda w: w["started_at"])
    latest = workflows[-1]
    latest["stage"] = stage
    latest["error_message"] = doc["error_message"]
    return workflows, job_ids


def _skip_document(sq, conn, doc: sqlite3.Row, report: Report, audit_by_workflow: dict[str, list], reason: str) -> None:
    """Leave a document behind. Its history rows are counted as skipped; its audit
    entries are kept, tied to the tenant's group but to no document."""
    workflow_id = doc["workflow_id"]
    report.skip("documents", workflow_id, reason)
    for table in ("document_jobs", "document_artifacts", "pages", "chunks"):
        report.count(table, "skipped", len(_rows(sq, table, "WHERE workflow_id = ?", (workflow_id,))))
    for audit in audit_by_workflow.pop(workflow_id, []):
        insert_audit(conn, audit, None, _instance_of(doc), report)


def migrate_document(
    sq, conn, doc: sqlite3.Row, report: Report, audit_by_workflow: dict[str, list],
    registry: Optional[dict[str, dict[str, Any]]] = None, consumed: Optional[set[str]] = None,
) -> bool:
    registry = registry if registry is not None else {}
    consumed = consumed if consumed is not None else set()
    workflow_id = doc["workflow_id"]
    doc_uuid = det_id("document", workflow_id)
    exists = conn.execute("SELECT 1 FROM documents WHERE id = %s::uuid", (doc_uuid,)).fetchone()
    if exists:
        report.count("documents", "existing")
        for table, label in (
            ("document_jobs", "document_jobs"), ("document_artifacts", "document_artifacts"),
            ("pages", "pages"), ("chunks", "chunks"),
        ):
            report.count(label, "existing", len(_rows(sq, table, "WHERE workflow_id = ?", (workflow_id,))))
        return False

    kind = (_get(doc, "document_kind") or "document").strip().lower()
    if truthy(_get(doc, "is_demo")):
        _skip_document(sq, conn, doc, report, audit_by_workflow, "demo document (is_demo)")
        return False
    if kind not in KIND_TO_CATALOG_TYPE:
        _skip_document(sq, conn, doc, report, audit_by_workflow, f"document_kind {kind!r} has no catalog")
        return False

    fingerprint = next(
        (
            c for c in (
                _get(doc, "source_file_fingerprint"), _get(doc, "canonical_document_id"), doc["document_id"]
            ) if c and _FINGERPRINT_RE.match(c)
        ),
        None,
    )
    if fingerprint is None:
        import hashlib

        fingerprint = hashlib.md5((doc["document_id"] or workflow_id).encode()).hexdigest()
        report.notes.append(f"{workflow_id}: no hex fingerprint, derived md5 of document_id")

    chunk_rows = _rows(sq, "chunks", "WHERE workflow_id = ? ORDER BY chunk_number", (workflow_id,))

    chunking = None
    if chunk_rows:
        first = chunk_rows[0]
        chunking = ChunkingRun(
            run_number=max(max(int(r["chunk_version"] or 1) for r in chunk_rows), 1),
            provider=_get(first, "chunking_provider"),
            model=_get(first, "chunking_model"),
            config=load_json(_get(first, "chunking_config_json"), {}),
        )

    valid_from, valid_to = parse_date(_get(doc, "valid_from")), parse_date(_get(doc, "valid_to"))
    if valid_from and valid_to and valid_from > valid_to:
        report.notes.append(f"{workflow_id}: valid_from after valid_to, valid_to cleared")
        valid_to = None

    uploader = _user_for(
        conn, _get(doc, "uploaded_by_user_id"), _get(doc, "uploaded_by_username"),
        _get(doc, "uploaded_by_email"), _get(doc, "uploaded_by_roles"),
    )

    metadata = DocumentMetadata(
        valid_from=valid_from.isoformat() if valid_from else None,
        valid_to=valid_to.isoformat() if valid_to else None,
        uploaded_by_user_id=uploader,
        index_status=_build_index_status(sq, workflow_id),
        catalog_visible=truthy(_get(doc, "catalog_visible"), True),
        network_visible=truthy(_get(doc, "network_visible"), True),
        reindex_required=truthy(_get(doc, "reindex_required")),
        reindex_reason=_get(doc, "reindex_reason") or None,
        chunking=chunking,
    ).model_dump(exclude_none=True)
    source = DocumentSource(
        fingerprint=fingerprint,
        original_filename=_get(doc, "source_filename") or doc["filename"],
        storage_uri=doc["filepath"],
        source_type=_get(doc, "source_type"),
        canonical_input_type=_get(doc, "canonical_input_type"),
    ).model_dump(exclude_none=True)

    catalog_id = _catalog_for(conn, _group_for(conn, _instance_of(doc)), kind)
    code = (_get(doc, "scheme_code") or "").strip().lower()
    entry = registry.get(code) if kind == "scheme" and code else None
    if entry is not None:
        consumed.add(code)
    name = (entry or {}).get("name") or _get(doc, "scheme_name") or _get(doc, "display_name") or None
    aliases = (entry or {}).get("aliases") or [
        a for a in load_json(_get(doc, "scheme_aliases_json"), []) if isinstance(a, str) and a
    ]
    if code and not _CODE_RE.match(code):
        report.notes.append(f"{workflow_id}: scheme_code {code!r} is not a valid slug, code derived")
        code = ""
    if not code:
        code = _slug(name or "") or _slug(Path(doc["filename"] or "").stem)
        if not _CODE_RE.match(code):
            code = f"doc-{fingerprint[:8]}"
        report.notes.append(f"{workflow_id}: no usable scheme_code, derived {code!r}")
    unique = _unique_code(conn, catalog_id, code)
    if unique != code:
        report.notes.append(f"{workflow_id}: code {code!r} already used in its catalog, stored as {unique!r}")
        code = unique

    created_at = parse_ts(doc["created_at"]) or datetime.now(timezone.utc)
    updated_at = parse_ts(doc["updated_at"]) or created_at
    conn.execute(
        """
        INSERT INTO documents (id, catalog_id, code, name, aliases, source, is_disabled, metadata,
                               created_at, updated_at)
        VALUES (%s::uuid, %s::uuid, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            doc_uuid, catalog_id, code, name or code, aliases, Jsonb(source),
            truthy(_get(doc, "is_disabled")), Jsonb(metadata), created_at, updated_at,
        ),
    )
    report.count("documents", "inserted")

    workflows, job_ids = _build_workflows(sq, doc, uploader, report)
    root = next((w for w in workflows if w["type"] in ("pipeline", "ocr_only")), workflows[0])
    for w in workflows:
        started_by = w["started_by"]
        if w.get("requested_by"):
            started_by = _user_for(conn, w["requested_by"][0], w["requested_by"][1], None, None)
        conn.execute(
            """
            INSERT INTO workflows (id, document_id, workflow_type, temporal_workflow_id, status,
                                   current_stage, stage_timestamps, stop_after_ocr, error_message,
                                   config, started_by_user_id, started_at, completed_at, updated_at)
            VALUES (%s::uuid, %s::uuid, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s::uuid, %s, %s, %s)
            """,
            (
                w["id"], doc_uuid, w["type"], w["temporal_id"], w["status"], w["stage"],
                Jsonb(w["stamps"]), w["stop_after_ocr"], w["error_message"], Jsonb(w["config"]),
                started_by, w["started_at"], w["completed_at"], w["completed_at"] or w["started_at"],
            ),
        )
        if w["inserted_as"]:
            report.count("document_jobs", "inserted")

    for art in _rows(sq, "document_artifacts", "WHERE workflow_id = ? ORDER BY id", (workflow_id,)):
        size = art["size_bytes"]
        art_metadata = load_json(art["metadata_json"], {})
        conn.execute(
            """
            INSERT INTO workflow_artifacts (id, workflow_id, artifact_type, stage, storage_uri,
                                            mime_type, filename, size_bytes, metadata, created_at)
            VALUES (%s::uuid, %s::uuid, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                det_id("artifact", f"{workflow_id}:{art['id']}"),
                job_ids.get(art["job_id"], root["id"]),
                art["artifact_type"], art["stage"] if art["stage"] in _STAGES else None,
                art["storage_uri"], art["mime_type"], art["filename"],
                size if size is not None and size >= 0 else None,
                Jsonb(art_metadata if isinstance(art_metadata, dict) else {}),
                parse_ts(art["created_at"]) or created_at,
            ),
        )
        report.count("document_artifacts", "inserted")

    for page in _rows(sq, "pages", "WHERE workflow_id = ? ORDER BY page_number", (workflow_id,)):
        if int(page["page_number"] or 0) < 1:
            report.skip("pages", f"{workflow_id}#{page['page_number']}", "page_number < 1")
            continue
        translations = {}
        if any(
            _get(page, k) not in (None, "")
            for k in ("translated_markdown", "edited_translation", "translation_notes")
        ) or truthy(_get(page, "translation_reviewed")):
            translations[_get(page, "translation_target_language") or "en"] = PageTranslation(
                translated_markdown=_get(page, "translated_markdown"),
                edited_translation=_get(page, "edited_translation"),
                is_reviewed=truthy(_get(page, "translation_reviewed")),
                reviewer_notes=_get(page, "translation_notes"),
                provider=_get(page, "translation_provider"),
                model=_get(page, "translation_model"),
                translated_at=_get(page, "translated_at"),
            ).model_dump()
        page_meta = PageMetadata(
            is_reviewed=truthy(_get(page, "is_reviewed")),
            reviewer_notes=_get(page, "reviewer_notes"),
            detected_language=_get(page, "detected_language"),
        ).model_dump(exclude_none=True)
        conn.execute(
            """
            INSERT INTO pages (id, document_id, page_number, original_markdown, edited_markdown,
                               translations, metadata)
            VALUES (%s::uuid, %s::uuid, %s, %s, %s, %s, %s)
            """,
            (
                det_id("page", f"{workflow_id}:{page['page_number']}"), doc_uuid, page["page_number"],
                page["original_markdown"], page["edited_markdown"], Jsonb(translations),
                Jsonb(page_meta),
            ),
        )
        report.count("pages", "inserted")

    for chunk in chunk_rows:
        if int(chunk["chunk_number"] or 0) < 1:
            report.skip("chunks", f"{workflow_id}#{chunk['chunk_number']}", "chunk_number < 1")
            continue
        page_start = max(int(chunk["page_start"] or 1), 1)
        spans = load_json(_get(chunk, "source_spans_json"), [])
        conn.execute(
            """
            INSERT INTO chunks (id, document_id, chunk_number, original_text, edited_text,
                                token_count, page_start, page_end, source_spans, section_title,
                                content_type, is_reference, is_reviewed, is_excluded, reviewer_notes)
            VALUES (%s::uuid, %s::uuid, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                det_id("chunk", f"{workflow_id}:{chunk['chunk_number']}"), doc_uuid,
                chunk["chunk_number"], chunk["original_text"] or "", chunk["edited_text"],
                max(int(chunk["token_count"] or 0), 0), page_start,
                max(int(chunk["page_end"] or page_start), page_start),
                Jsonb(spans if isinstance(spans, list) else []), chunk["section_title"],
                chunk["content_type"], truthy(_get(chunk, "is_reference")),
                truthy(_get(chunk, "is_reviewed")), truthy(_get(chunk, "is_excluded")),
                chunk["reviewer_notes"],
            ),
        )
        report.count("chunks", "inserted")

    for audit in audit_by_workflow.pop(workflow_id, []):
        insert_audit(conn, audit, doc_uuid, source_instance=_instance_of(doc), report=report)
    return True


# -----------------------------------------------------------------------------
# Audit, settings, OTPs, webhooks
# -----------------------------------------------------------------------------

def _legacy_audit_field(field_name: Optional[str], entity: Optional[str]) -> Optional[str]:
    if entity == "setting" and field_name in _LEGACY_SETTING_KEYS:
        flat = _LEGACY_SETTING_KEYS[field_name]
        return f"{db._SETTING_GROUPS[flat][0]}.{flat}"
    return field_name


def insert_audit(conn, audit: sqlite3.Row, doc_uuid: Optional[str], source_instance: Optional[str], report: Report) -> None:
    entity = audit["entity_type"] if audit["entity_type"] in _ENTITY_TYPES else None
    workflow_id = audit["workflow_id"]
    if audit["document_id"] == "__settings__":
        entity, workflow_id = "setting", None
    elif workflow_id == "__catalog__":
        entity, workflow_id = "catalog", None
    metadata = load_json(audit["metadata"], None)
    if not isinstance(metadata, dict):
        metadata = {"raw": audit["metadata"]} if audit["metadata"] else {}
    roles = [r.strip() for r in (_get(audit, "actor_roles") or "").split(",") if r.strip()]
    actor_present = any((_get(audit, "actor_user_id"), _get(audit, "actor_username"), _get(audit, "actor_email"), roles))
    actor_uuid = _user_for(
        conn, _get(audit, "actor_user_id"), _get(audit, "actor_username"), _get(audit, "actor_email"), roles
    )
    inserted = conn.execute(
        """
        INSERT INTO audit_logs (id, document_id, workflow_id, group_id, action_type, entity_type,
                                entity_number, field_name, old_value, new_value, metadata,
                                actor_user_id, actor, created_at)
        VALUES (%s::uuid, %s::uuid, %s, %s::uuid, %s, %s, %s, %s, %s, %s, %s,
                %s::uuid, %s, %s)
        ON CONFLICT (id) DO NOTHING RETURNING id
        """,
        (
            det_id("audit", str(audit["id"])), doc_uuid, workflow_id,
            _group_for(conn, source_instance.strip().lower()) if (source_instance or "").strip() else None,
            audit["action_type"], entity, audit["entity_id"],
            _legacy_audit_field(audit["field_name"], entity), audit["old_value"], audit["new_value"],
            Jsonb(metadata), actor_uuid,
            Jsonb({
                "username": _get(audit, "actor_username") or None,
                "email": _get(audit, "actor_email") or None,
                "roles": roles,
            }) if actor_present else Jsonb({}),
            parse_ts(audit["timestamp"]) or datetime.now(timezone.utc),
        ),
    ).fetchone()
    report.count("audit_logs", "inserted" if inserted else "existing")


def migrate_settings(sq, conn, report: Report) -> None:
    rows = _rows(sq, "settings")
    for _ in rows:
        report.count("settings", "read")
    current = conn.execute(
        "SELECT retrieval, candidates, results, query FROM search_settings WHERE singleton"
    ).fetchone()
    if current is None:
        conn.execute("INSERT INTO search_settings DEFAULT VALUES")
        current = {"retrieval": {}, "candidates": {}, "results": {}, "query": {}}
    if any(current[g] for g in ("retrieval", "candidates", "results", "query")):
        for _ in rows:
            report.count("settings", "existing")
        report.notes.append("search_settings already configured in Postgres, legacy settings not applied")
        return

    groups: dict[str, dict[str, Any]] = {}
    for row in rows:
        flat = _LEGACY_SETTING_KEYS.get(row["key"])
        if flat is None:
            report.skip("settings", row["key"], "unknown setting key")
            continue
        raw = row["value"]
        try:
            value: Any = (
                raw == "true" if flat in _BOOL_SETTINGS
                else int(raw) if flat in _INT_SETTINGS
                else float(raw) if flat in _FLOAT_SETTINGS
                else raw
            )
        except ValueError:
            report.skip("settings", row["key"], f"unparseable value {raw!r}")
            continue
        group, model = db._SETTING_GROUPS[flat]
        try:
            model.model_validate({flat: value})
        except Exception as exc:
            report.skip("settings", row["key"], f"invalid value: {exc}")
            continue
        groups.setdefault(group, {})[flat] = value
        report.count("settings", "inserted")
    for group, values in groups.items():
        conn.execute(
            f"UPDATE search_settings SET {group} = {group} || %s::jsonb WHERE singleton", (Jsonb(values),)
        )


def migrate_email_otps(sq, conn, report: Report) -> None:
    now = datetime.now(timezone.utc)
    for row in _rows(sq, "email_otps"):
        report.count("email_otps", "read")
        expires = parse_ts(row["expires_at"])
        if expires is None or expires <= now:
            report.count("email_otps", "dropped")
            continue
        inserted = conn.execute(
            """
            INSERT INTO email_otps (email, code_hash, expires_at, attempts, last_sent_at)
            VALUES (%s, %s, %s, %s, %s) ON CONFLICT (email) DO NOTHING RETURNING id
            """,
            (row["email"].strip().lower(), row["code_hash"], expires, max(int(row["attempts"] or 0), 0),
             parse_ts(row["last_sent_at"]) or now),
        ).fetchone()
        report.count("email_otps", "inserted" if inserted else "existing")


def migrate_webhook_records(sq, conn, report: Report) -> None:
    for row in _rows(sq, "webhook_records"):
        report.count("webhook_records", "read")
        payload = load_json(row["payload"], None)
        if not isinstance(payload, dict):
            report.skip("webhook_records", f"{row['source']}/{row['external_id']}", "payload is not a JSON object")
            continue
        try:
            record_id = str(uuid.UUID(row["id"]))
        except (ValueError, TypeError):
            record_id = det_id("webhook", row["id"] or f"{row['source']}:{row['external_id']}")
        published = row["is_published"]
        inserted = conn.execute(
            """
            INSERT INTO webhook_records (id, source, external_id, title, slug, status, is_published,
                                         category, payload, payload_hash, created_at, updated_at)
            VALUES (%s::uuid, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (source, external_id) DO NOTHING RETURNING id
            """,
            (
                record_id, row["source"], row["external_id"], row["title"], row["slug"], row["status"],
                None if published is None else bool(published), row["category"], Jsonb(payload),
                row["payload_hash"], parse_ts(row["created_at"]) or datetime.now(timezone.utc),
                parse_ts(row["updated_at"]) or datetime.now(timezone.utc),
            ),
        ).fetchone()
        report.count("webhook_records", "inserted" if inserted else "existing")


# -----------------------------------------------------------------------------
# Driver
# -----------------------------------------------------------------------------

def run_migration(sqlite_path: str, database_url: str, dry_run: bool = False) -> Report:
    report = Report()
    sq = sqlite3.connect(f"file:{sqlite_path}?mode=ro", uri=True)
    sq.row_factory = sqlite3.Row
    try:
        if not dry_run:
            migrate.apply_migrations(database_url)
        elif not migrate.is_current(database_url):
            raise SystemExit("Postgres migrations are not applied; apply them before a dry run.")

        with psycopg.connect(database_url, autocommit=not dry_run, row_factory=dict_row) as conn:
            conn.execute("SET timezone = 'UTC'")
            documents = _rows(sq, "documents", "ORDER BY created_at, workflow_id")
            known_workflows = {d["workflow_id"] for d in documents}
            for table in ("document_jobs", "document_artifacts", "pages", "chunks"):
                if _has_table(sq, table):
                    total = sq.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                    stray = sq.execute(
                        f"SELECT COUNT(*) FROM {table} WHERE workflow_id NOT IN "
                        "(SELECT workflow_id FROM documents)"
                    ).fetchone()[0]
                    report.count(table, "read", total)
                    if stray:
                        report.count(table, "skipped", stray)
                        report.notes.append(f"{table}: {stray} rows belong to no document and were not migrated")

            audit_by_workflow: dict[str, list] = {}
            orphan_audit: list = []
            for audit in _rows(sq, "audit_logs", "ORDER BY id"):
                report.count("audit_logs", "read")
                if audit["workflow_id"] in known_workflows and audit["document_id"] != "__settings__":
                    audit_by_workflow.setdefault(audit["workflow_id"], []).append(audit)
                else:
                    orphan_audit.append(audit)

            registry = load_catalog_registry(sq, report)
            consumed: set[str] = set()

            for doc in documents:
                report.count("documents", "read")
                with conn.transaction():
                    migrate_document(sq, conn, doc, report, audit_by_workflow, registry, consumed)
            settle_catalog_registry(registry, consumed, report)

            skipped = Counter(i["reason"] for i in report.skip_reasons if i["table"] == "documents")
            for reason, n in sorted(skipped.items()):
                report.notes.append(f"documents not migrated: {n} x {reason}")

            with conn.transaction():
                for audits in audit_by_workflow.values():
                    orphan_audit.extend(audits)
                for audit in orphan_audit:
                    insert_audit(conn, audit, None, None, report)
                migrate_settings(sq, conn, report)
                migrate_email_otps(sq, conn, report)
                migrate_webhook_records(sq, conn, report)
                migrate_catalog_version(sq, conn, report)

            manifest = _rows(sq, "document_manifest_entries")
            if manifest:
                report.count("document_manifest_entries", "read", len(manifest))
                report.count("document_manifest_entries", "dropped", len(manifest))
                report.notes.append("document_manifest_entries is not part of the schema and was not migrated")

            if dry_run:
                conn.rollback()
    finally:
        sq.close()
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--sqlite-path", default=os.environ.get("DOCUMENT_DB_PATH", "/data/documents.db"))
    parser.add_argument("--database-url", default=os.environ.get("DATABASE_URL", db.DEFAULT_DATABASE_URL))
    parser.add_argument("--dry-run", action="store_true", help="run everything, then roll back")
    parser.add_argument("--report", help="write the report as JSON to this path")
    args = parser.parse_args()

    if not os.path.exists(args.sqlite_path):
        print(f"SQLite database not found at {args.sqlite_path}", file=sys.stderr)
        return 2

    report = run_migration(args.sqlite_path, args.database_url, dry_run=args.dry_run)
    print(("DRY RUN - nothing was written\n" if args.dry_run else "") + report.render())
    if args.report:
        Path(args.report).write_text(json.dumps(report.as_dict(), indent=2, default=str))
    return 1 if report.mismatches() else 0


if __name__ == "__main__":
    sys.exit(main())
