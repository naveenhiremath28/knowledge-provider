"""scripts/migrate_sqlite_to_postgres.py: a legacy SQLite file lands in Postgres.

The fixture file is built with the legacy schema DDL, the way a production
database looked at cutover.
"""

import hashlib
import importlib.util
import json
import sqlite3
from pathlib import Path

import pytest

_SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "migrate_sqlite_to_postgres.py"

LEGACY_DDL = """
CREATE TABLE documents (
    workflow_id TEXT PRIMARY KEY, document_id TEXT NOT NULL, canonical_document_id TEXT,
    filename TEXT NOT NULL, display_name TEXT, source_filename TEXT, source_manifest_name TEXT,
    source_file_fingerprint TEXT, filepath TEXT NOT NULL, stage TEXT NOT NULL DEFAULT 'registered',
    page_count INTEGER DEFAULT 0, chunk_count INTEGER DEFAULT 0, error_message TEXT,
    is_demo INTEGER DEFAULT 0, created_at TEXT, updated_at TEXT, ocr_completed_at TEXT,
    translation_completed_at TEXT, chunks_completed_at TEXT, ingested_at TEXT, source_type TEXT,
    canonical_input_type TEXT, stop_after_ocr INTEGER DEFAULT 0, reindex_required INTEGER DEFAULT 0,
    reindex_reason TEXT, original_artifact_id INTEGER, normalized_artifact_id INTEGER,
    latest_job_id INTEGER, is_disabled INTEGER DEFAULT 0, instance TEXT, uploaded_by_user_id TEXT,
    uploaded_by_username TEXT, uploaded_by_email TEXT, uploaded_by_roles TEXT,
    prod_ready_requested_at TEXT, prod_ready_requested_by_user_id TEXT,
    prod_ready_requested_by_username TEXT, valid_from TEXT, valid_to TEXT,
    document_kind TEXT DEFAULT 'document', scheme_code TEXT, scheme_name TEXT,
    scheme_aliases_json TEXT, tool_routing TEXT, catalog_visible INTEGER DEFAULT 1,
    network_visible INTEGER DEFAULT 1
);
CREATE TABLE audit_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT, workflow_id TEXT NOT NULL, document_id TEXT NOT NULL,
    action_type TEXT NOT NULL, entity_type TEXT, entity_id INTEGER, field_name TEXT, old_value TEXT,
    new_value TEXT, metadata TEXT, timestamp TEXT NOT NULL, actor_user_id TEXT, actor_username TEXT,
    actor_email TEXT, actor_roles TEXT
);
CREATE TABLE pages (
    id INTEGER PRIMARY KEY AUTOINCREMENT, workflow_id TEXT NOT NULL, page_number INTEGER NOT NULL,
    original_markdown TEXT, edited_markdown TEXT, is_reviewed INTEGER DEFAULT 0, reviewer_notes TEXT,
    detected_language TEXT, translated_markdown TEXT, edited_translation TEXT,
    translation_reviewed INTEGER DEFAULT 0, translation_notes TEXT, translation_provider TEXT,
    translation_model TEXT, translation_target_language TEXT, translated_at TEXT,
    UNIQUE(workflow_id, page_number)
);
CREATE TABLE chunks (
    id INTEGER PRIMARY KEY AUTOINCREMENT, workflow_id TEXT NOT NULL, chunk_number INTEGER NOT NULL,
    original_text TEXT, edited_text TEXT, token_count INTEGER DEFAULT 0, page_start INTEGER DEFAULT 1,
    page_end INTEGER DEFAULT 1, source_page_numbers_json TEXT, source_spans_json TEXT,
    section_title TEXT, content_type TEXT, is_reference INTEGER DEFAULT 0, chunking_provider TEXT,
    chunking_model TEXT, chunking_config_json TEXT, chunking_run_id TEXT, chunk_version INTEGER DEFAULT 1,
    is_reviewed INTEGER DEFAULT 0, is_excluded INTEGER DEFAULT 0, reviewer_notes TEXT,
    UNIQUE(workflow_id, chunk_number)
);
CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT NOT NULL, description TEXT, updated_at TEXT NOT NULL);
CREATE TABLE document_jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT, workflow_id TEXT NOT NULL, job_type TEXT NOT NULL,
    temporal_workflow_id TEXT, temporal_run_id TEXT, status TEXT NOT NULL DEFAULT 'running',
    current_stage TEXT, started_at TEXT NOT NULL, completed_at TEXT, error_message TEXT, config_json TEXT
);
CREATE TABLE document_artifacts (
    id INTEGER PRIMARY KEY AUTOINCREMENT, workflow_id TEXT NOT NULL, job_id INTEGER,
    artifact_type TEXT NOT NULL, stage TEXT, storage_uri TEXT NOT NULL, mime_type TEXT, filename TEXT,
    size_bytes INTEGER, metadata_json TEXT, created_at TEXT NOT NULL
);
CREATE TABLE document_index_status (
    workflow_id TEXT NOT NULL, index_name TEXT NOT NULL, doc_id TEXT, chunk_count_indexed INTEGER DEFAULT 0,
    last_indexed_at TEXT, last_verified_at TEXT, schema_version TEXT, status TEXT NOT NULL DEFAULT 'unknown',
    details_json TEXT, PRIMARY KEY (workflow_id, index_name)
);
CREATE TABLE document_manifest_entries (
    id INTEGER PRIMARY KEY AUTOINCREMENT, manifest_filename TEXT NOT NULL UNIQUE, title_en TEXT,
    imported_at TEXT NOT NULL
);
CREATE TABLE email_otps (
    email TEXT PRIMARY KEY, code_hash TEXT NOT NULL, expires_at TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL, last_sent_at TEXT NOT NULL
);
CREATE TABLE webhook_records (
    id TEXT PRIMARY KEY, source TEXT NOT NULL DEFAULT 'webhook', external_id TEXT NOT NULL, title TEXT,
    slug TEXT, status TEXT, is_published INTEGER, category TEXT, payload TEXT NOT NULL, payload_hash TEXT,
    created_at TEXT NOT NULL, updated_at TEXT NOT NULL, UNIQUE (source, external_id)
);
CREATE TABLE scheme_catalog_meta (
    id INTEGER PRIMARY KEY CHECK (id = 1), version INTEGER NOT NULL DEFAULT 0, updated_at TEXT, notes TEXT
);
CREATE TABLE scheme_catalog_entries (
    scheme_code TEXT PRIMARY KEY, scheme_name TEXT, scheme_aliases_json TEXT, instances_json TEXT,
    workflow_ids_json TEXT, collection_name TEXT, chunk_count INTEGER DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'disabled', network_visible INTEGER DEFAULT 0, promoted_at TEXT,
    content_hash TEXT, source TEXT, updated_at TEXT
);
"""

FP1 = hashlib.md5(b"scheme guideline").hexdigest()
FP2 = hashlib.md5(b"in flight").hexdigest()
WF1 = f"doc-{FP1}"
WF2 = f"doc-{FP2}"
WF3 = "doc-broken"
FUTURE = "2099-01-01T00:00:00"


def _load_script():
    spec = importlib.util.spec_from_file_location("migrate_sqlite_to_postgres", _SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _insert(conn, table, **values):
    keys = ", ".join(values)
    marks = ", ".join("?" for _ in values)
    conn.execute(f"INSERT INTO {table} ({keys}) VALUES ({marks})", tuple(values.values()))


@pytest.fixture
def legacy_db(tmp_path):
    path = tmp_path / "documents.db"
    conn = sqlite3.connect(path)
    conn.executescript(LEGACY_DDL)

    _insert(
        conn, "documents", workflow_id=WF1, document_id=FP1, canonical_document_id=FP1,
        filename="guideline.pdf", display_name="Guideline", source_filename="guideline.pdf",
        source_file_fingerprint=FP1, filepath="minio://documents/guideline.pdf", stage="completed",
        page_count=2, chunk_count=2, created_at="2026-03-01T10:00:00", updated_at="2026-03-05T10:00:00",
        ocr_completed_at="2026-03-01T11:00:00", translation_completed_at="2026-03-02T11:00:00",
        chunks_completed_at="2026-03-03T11:00:00", ingested_at="2026-03-04T11:00:00",
        source_type="document", canonical_input_type="pdf", instance="mh", uploaded_by_user_id="kc-up",
        uploaded_by_username="uma", uploaded_by_email="uma@x.in", uploaded_by_roles="contributor,reviewer",
        valid_from="2026-03-01", valid_to="2027-03-01", document_kind="scheme", scheme_code="pm-kmy",
        scheme_name="PM Kisan Maandhan", scheme_aliases_json=json.dumps(["PMKMY", "kmy"]),
        catalog_visible=1, network_visible=0, reindex_required=1, reindex_reason="edit",
    )
    _insert(
        conn, "documents", workflow_id=WF2, document_id=FP2, filename="inflight.pdf",
        filepath="/data/documents/inflight.pdf", stage="approval_for_prod", page_count=1,
        created_at="2026-04-01T10:00:00", updated_at="2026-04-01T12:00:00", instance="up",
        is_demo=1, prod_ready_requested_at="2026-04-02T09:00:00",
        prod_ready_requested_by_user_id="kc-req", prod_ready_requested_by_username="rita",
        ocr_completed_at="2026-04-01T11:00:00",
    )
    _insert(
        conn, "documents", workflow_id=WF3, document_id="not-a-hash", filename="broken.pdf",
        filepath="/tmp/broken.pdf", stage="failed", error_message="OCR blew up",
        created_at="2026-05-01T10:00:00", updated_at="2026-05-01T10:30:00", instance=" ",
        document_kind="document", scheme_code="Bad Code", valid_from="2026-06-01", valid_to="2026-01-01",
    )

    _insert(conn, "document_jobs", id=1, workflow_id=WF1, job_type="pipeline", temporal_workflow_id=WF1,
            status="completed", current_stage="completed", started_at="2026-03-01T10:00:00",
            completed_at="2026-03-04T11:00:00", config_json=json.dumps({"auto_approve": False}),
            temporal_run_id="run-1")
    _insert(conn, "document_jobs", id=2, workflow_id=WF1, job_type="reingest",
            temporal_workflow_id=f"{WF1}-reingest-1", status="completed", current_stage="ingesting",
            started_at="2026-03-10T10:00:00", completed_at="2026-03-10T10:05:00")
    _insert(conn, "document_jobs", id=3, workflow_id=WF1, job_type="mystery",
            started_at="2026-03-11T10:00:00")

    _insert(conn, "document_artifacts", workflow_id=WF1, job_id=1, artifact_type="original_upload",
            stage="registered", storage_uri="minio://documents/guideline.pdf", size_bytes=100,
            metadata_json=json.dumps({"uploaded_via": "upload_endpoint"}), created_at="2026-03-01T10:00:01")
    _insert(conn, "document_artifacts", workflow_id=WF1, job_id=None, artifact_type="normalized_pdf",
            stage="ocr_processing", storage_uri="minio://documents/n.pdf", size_bytes=-1,
            created_at="2026-03-01T10:30:00")
    _insert(conn, "document_artifacts", workflow_id=WF1, job_id=2, artifact_type="vector_payload_export",
            stage="ingesting", storage_uri="minio://documents/v.json", created_at="2026-03-10T10:01:00")

    _insert(conn, "pages", workflow_id=WF1, page_number=1, original_markdown="namaste", is_reviewed=1,
            detected_language="hi", translated_markdown="hello", translation_provider="azure",
            translation_model="gpt", translation_target_language="en", translated_at="2026-03-02T10:00:00")
    _insert(conn, "pages", workflow_id=WF1, page_number=2, original_markdown="english page",
            detected_language="en", reviewer_notes="fine")
    _insert(conn, "pages", workflow_id="doc-purged", page_number=1, original_markdown="stray")

    _insert(conn, "chunks", workflow_id=WF1, chunk_number=1, original_text="chunk one", token_count=5,
            page_start=1, page_end=2, source_spans_json=json.dumps([{"page_number": 1, "start_char": 0, "end_char": 5}]),
            chunking_provider="recursive", chunking_model="rs", chunking_config_json='{"size": 450}',
            chunk_version=3, is_reviewed=1)
    _insert(conn, "chunks", workflow_id=WF1, chunk_number=2, original_text="chunk two", edited_text="two",
            page_start=2, page_end=1, is_excluded=1)

    _insert(conn, "document_index_status", workflow_id=WF1, index_name="documents-index", doc_id="abc",
            chunk_count_indexed=1, last_indexed_at="2026-03-04T11:00:00", status="indexed",
            details_json='{"backend": "qdrant"}')
    _insert(conn, "document_index_status", workflow_id=WF1, index_name="schemes-index", doc_id="abc",
            chunk_count_indexed=2, last_indexed_at="2026-03-10T10:05:00", status="indexed")

    _insert(conn, "audit_logs", workflow_id=WF1, document_id=FP1, action_type="stage_change",
            entity_type="document", field_name="stage", old_value="chunk_review", new_value="completed",
            metadata='{"page_count": 2}', timestamp="2026-03-04T11:00:00")
    _insert(conn, "audit_logs", workflow_id=WF1, document_id=FP1, action_type="page_edit",
            entity_type="page", entity_id=1, metadata="not json", timestamp="2026-03-02T12:00:00",
            actor_user_id="kc-rev", actor_username="ravi", actor_email="ravi@x.in", actor_roles="reviewer")
    _insert(conn, "audit_logs", workflow_id="doc-purged", document_id="gone", action_type="purge_document",
            entity_type="document", timestamp="2026-02-01T00:00:00")
    _insert(conn, "audit_logs", workflow_id="__system__", document_id="__settings__",
            action_type="settings_change", entity_type="setting", field_name="search_alpha",
            old_value="0.7", new_value="0.6", timestamp="2026-01-01T00:00:00")
    _insert(conn, "audit_logs", workflow_id="__catalog__", document_id="__catalog__",
            action_type="catalog_rebuild", entity_type="catalog", timestamp="2026-01-02T00:00:00")

    for key, value in (
        ("search_alpha", "0.4"), ("search_limit", "20"), ("search_show_highlights", "false"),
        ("search_method", "LEXICAL"), ("search_index_name", "documents-index"), ("search_mystery", "x"),
        ("search_ef_search", "lots"),
    ):
        _insert(conn, "settings", key=key, value=value, updated_at="2026-01-01T00:00:00")

    _insert(conn, "email_otps", email="old@x.in", code_hash="h", expires_at="2020-01-01T00:00:00+00:00",
            created_at="2020-01-01T00:00:00+00:00", last_sent_at="2020-01-01T00:00:00+00:00")
    _insert(conn, "email_otps", email="LIVE@x.in", code_hash="h2", expires_at=FUTURE + "+00:00",
            attempts=2, created_at="2026-01-01T00:00:00+00:00", last_sent_at="2026-01-01T00:00:00+00:00")

    _insert(conn, "webhook_records", id="3c8a2f0e-6b1d-4f6e-9d0a-5f8f0b2e7a11", external_id="c-1",
            title="T", slug="t", status="published", is_published=1, category="Agri",
            payload='{"contentID": "c-1"}', payload_hash="h", created_at="2026-01-01T00:00:00",
            updated_at="2026-01-02T00:00:00")

    _insert(conn, "scheme_catalog_meta", id=1, version=5, updated_at="2026-02-01T00:00:00", notes="x")
    _insert(conn, "scheme_catalog_entries", scheme_code="cdp", scheme_name="Crop Diversification Programme",
            scheme_aliases_json='["CDP"]', status="live", source="bootstrap")
    _insert(conn, "scheme_catalog_entries", scheme_code="pm-kmy", scheme_name="Registry name",
            scheme_aliases_json='["registry"]', status="live", source="pipeline")
    _insert(conn, "document_manifest_entries", manifest_filename="a.csv", imported_at="2026-01-01T00:00:00")
    _insert(conn, "document_manifest_entries", manifest_filename="b.csv", imported_at="2026-01-01T00:00:00")
    conn.commit()
    conn.close()
    return str(path)


def _counts(db):
    with db.get_connection() as conn:
        return {
            table: conn.execute(f"SELECT count(*) AS n FROM {table}").fetchone()["n"]
            for table in (
                "documents", "workflows", "workflow_artifacts", "pages", "chunks", "audit_logs",
                "users", "catalog_entries", "email_otps", "webhook_records",
            )
        }


@pytest.mark.db
@pytest.mark.unit
class TestSqliteToPostgresMigration:
    def test_dry_run_writes_nothing_but_reports_everything(self, db_connection, postgres_url, legacy_db):
        script = _load_script()
        before = _counts(db_connection)
        report = script.run_migration(legacy_db, postgres_url, dry_run=True)
        assert _counts(db_connection) == before
        assert report.tables["documents"]["inserted"] == 3
        assert report.mismatches() == []

    def test_a_full_run_maps_every_table(self, db_connection, postgres_url, legacy_db):
        db = db_connection
        report = _load_script().run_migration(legacy_db, postgres_url)

        assert report.mismatches() == []
        tables = report.as_dict()["tables"]
        assert tables["documents"]["inserted"] == 3
        assert tables["document_jobs"] == {"read": 3, "inserted": 2, "skipped": 1}
        assert tables["pages"] == {"read": 3, "inserted": 2, "skipped": 1}
        assert tables["chunks"]["inserted"] == 2
        assert tables["document_manifest_entries"] == {"read": 2, "dropped": 2}
        assert tables["email_otps"] == {"read": 2, "inserted": 1, "dropped": 1}
        assert tables["settings"]["skipped"] == 2

        doc = db.get_document(WF1)
        assert doc["stage"] == "completed"
        assert doc["instance"] == "mh"
        assert doc["document_id"] == FP1
        assert doc["display_name"] == "Guideline"
        assert (doc["valid_from"], doc["valid_to"]) == ("2026-03-01", "2027-03-01")
        assert (doc["scheme_code"], doc["document_kind"]) == ("pm-kmy", "scheme")
        assert doc["network_visible"] is False and doc["reindex_required"] is True
        assert doc["uploaded_by_username"] == "uma"
        assert doc["uploaded_by_roles"] == "contributor,reviewer"
        assert doc["created_at"] == "2026-03-01T10:00:00"
        assert doc["ocr_completed_at"] == "2026-03-01T11:00:00"
        assert doc["chunk_count"] == 2
        assert doc["page_count"] == 2
        assert set(doc["index_status"]) == {"dev"}
        assert doc["index_status"]["dev"]["index_name"] == "schemes-index"

        jobs = {j["job_type"]: j for j in db.list_document_jobs(WF1)}
        assert set(jobs) == {"pipeline", "reingest"}
        assert jobs["pipeline"]["temporal_workflow_id"] == WF1
        assert jobs["pipeline"]["temporal_run_id"] == "run-1"
        assert jobs["reingest"]["temporal_workflow_id"] == f"{WF1}-reingest-1"
        assert doc["workflow_id"] == WF1

        artifacts = {a["artifact_type"]: a for a in db.list_document_artifacts(WF1)}
        assert artifacts["original_upload"]["job_id"] == jobs["pipeline"]["id"]
        assert artifacts["vector_payload_export"]["job_id"] == jobs["reingest"]["id"]
        assert artifacts["normalized_pdf"]["job_id"] == jobs["pipeline"]["id"]
        assert artifacts["normalized_pdf"]["size_bytes"] is None
        assert doc["original_artifact_id"] == artifacts["original_upload"]["id"]

        pages = db.get_pages(WF1)
        assert pages[0]["translated_markdown"] == "hello"
        assert pages[0]["translation_target_language"] == "en"
        assert pages[0]["is_reviewed"] is True and pages[0]["detected_language"] == "hi"
        assert pages[1]["translations"] == {} and pages[1]["reviewer_notes"] == "fine"

        chunks = db.get_chunks(WF1, include_excluded=True)
        assert [c["chunk_version"] for c in chunks] == [3, 3]
        assert chunks[0]["chunking_config_json"] == '{"size": 450}'
        assert chunks[1]["page_end"] == 2 and chunks[1]["is_excluded"] is True

        entries = {e["code"]: e for e in db.list_catalog_entries()}
        assert entries["cdp"]["source"] == "bootstrap" and entries["cdp"]["status"] == "live"
        assert entries["pm-kmy"]["name"] == "Registry name"
        assert entries["pm-kmy"]["status"] == "pending_reindex"
        assert entries["pm-kmy"]["workflow_ids"] == [WF1]
        assert db.get_catalog_meta()["version"] == 6

        users = {u: db.get_user_by_keycloak_id(u) for u in ("kc-up", "kc-rev", "kc-req")}
        assert all(users.values())

        logs = db.get_audit_logs(WF1)
        assert {e["action_type"] for e in logs} == {"stage_change", "page_edit"}
        edit = next(e for e in logs if e["action_type"] == "page_edit")
        assert edit["actor"] == "ravi@x.in (reviewer)"
        assert edit["metadata"] == json.dumps({"raw": "not json"})
        purged = db.get_audit_logs("doc-purged")
        assert [e["action_type"] for e in purged] == ["purge_document"]
        settings_log = db.get_settings_audit_logs()
        assert settings_log[0]["field_name"] == "retrieval.alpha"

        settings = db.get_search_settings()
        assert settings["alpha"] == 0.4 and settings["limit"] == 20
        assert settings["showHighlights"] is False and settings["searchMethod"] == "LEXICAL"
        assert settings["efSearch"] == 256

        assert db.get_email_otp("live@x.in")["attempts"] == 2
        assert db.get_email_otp("old@x.in") is None
        with db.get_connection() as conn:
            hook = conn.execute("SELECT * FROM webhook_records").fetchone()
        assert str(hook["id"]) == "3c8a2f0e-6b1d-4f6e-9d0a-5f8f0b2e7a11"
        assert hook["is_published"] is True and hook["payload"] == {"contentID": "c-1"}

    def test_in_flight_documents_keep_their_stage_and_prod_request(self, db_connection, postgres_url, legacy_db):
        db = db_connection
        _load_script().run_migration(legacy_db, postgres_url)

        doc = db.get_document(WF2)
        assert doc["stage"] == "approval_for_prod"
        assert doc["is_demo"] is True
        assert doc["prod_ready_requested_at"] == "2026-04-02T09:00:00"
        assert doc["prod_ready_requested_by_username"] == "rita"
        assert doc["instance"] == "up"
        assert doc["ocr_completed_at"] == "2026-04-01T11:00:00"
        types = sorted(j["job_type"] for j in db.list_document_jobs(WF2))
        assert types == ["pipeline", "promote_prod"]
        assert db.get_latest_document_job(WF2)["status"] == "waiting_review"
        assert db.get_document(doc["id"])["workflow_id"] == WF2
        assert db.get_document(FP2)["id"] == doc["id"]

    def test_unusable_values_are_repaired_and_noted(self, db_connection, postgres_url, legacy_db):
        db = db_connection
        report = _load_script().run_migration(legacy_db, postgres_url)

        doc = db.get_document(WF3)
        assert doc["stage"] == "failed" and doc["error_message"] == "OCR blew up"
        assert doc["document_id"] == hashlib.md5(b"not-a-hash").hexdigest()
        assert doc["instance"] == "default"
        assert doc["scheme_code"] is None
        assert doc["valid_from"] == "2026-06-01" and doc["valid_to"] is None
        notes = " ".join(report.notes)
        assert "valid_to cleared" in notes and "not a valid slug" in notes or "plain document" in notes

    def test_running_it_twice_changes_nothing(self, db_connection, postgres_url, legacy_db):
        script = _load_script()
        script.run_migration(legacy_db, postgres_url)
        first = _counts(db_connection)

        report = script.run_migration(legacy_db, postgres_url)

        assert _counts(db_connection) == first
        assert report.tables["documents"] == {"read": 3, "existing": 3}
        assert report.tables["audit_logs"]["inserted"] == 0
        assert report.tables["webhook_records"]["existing"] == 1
        assert report.mismatches() == []
        assert db_connection.get_catalog_meta()["version"] == 6

    def test_legacy_file_is_opened_read_only(self, postgres_url, legacy_db):
        script = _load_script()
        before = Path(legacy_db).read_bytes()
        script.run_migration(legacy_db, postgres_url)
        assert Path(legacy_db).read_bytes() == before
