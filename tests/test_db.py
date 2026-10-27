"""
Unit tests for pipeline/db.py against a real Postgres.

Tests cover:
- Document CRUD operations
- Page operations
- Chunk operations
- Audit logging
- Settings management
- Schema constraints, JSONB merges, workflows, the catalog view, identifier
  resolution and the migration runner
"""

import hashlib

import psycopg
import pytest


def _md5(text):
    return hashlib.md5(text.encode()).hexdigest()


class TestDocumentOperations:
    """Tests for document CRUD operations."""

    @pytest.mark.db
    @pytest.mark.unit
    def test_upsert_document_creates_new(self, db_connection):
        """Test creating a new document."""
        workflow_id = "test-wf-001"
        document_id = _md5("test-doc-001")

        db_connection.upsert_document(
            workflow_id=workflow_id,
            document_id=document_id,
            filename="test.pdf",
            filepath="/app/books/test.pdf",
            stage="registered"
        )

        doc = db_connection.get_document(workflow_id)
        assert doc is not None
        assert doc["workflow_id"] == workflow_id
        assert doc["document_id"] == document_id
        assert doc["filename"] == "test.pdf"
        assert doc["stage"] == "registered"

    @pytest.mark.db
    @pytest.mark.unit
    def test_upsert_document_updates_existing(self, db_connection):
        """Test updating an existing document."""
        workflow_id = "test-wf-002"

        # Create initial document
        db_connection.upsert_document(
            workflow_id=workflow_id,
            document_id="doc-002",
            filename="test.pdf",
            filepath="/app/books/test.pdf",
            stage="registered"
        )

        # Update it
        db_connection.upsert_document(
            workflow_id=workflow_id,
            document_id="doc-002",
            filename="test.pdf",
            filepath="/app/books/test.pdf",
            stage="ocr_processing"
        )

        doc = db_connection.get_document(workflow_id)
        assert doc["stage"] == "ocr_processing"

    @pytest.mark.db
    @pytest.mark.unit
    def test_get_document_not_found(self, db_connection):
        """Test getting a non-existent document returns None."""
        doc = db_connection.get_document("nonexistent-workflow")
        assert doc is None

    @pytest.mark.db
    @pytest.mark.unit
    def test_list_documents(self, db_connection):
        """Test listing documents."""
        # Create multiple documents
        for i in range(3):
            db_connection.upsert_document(
                workflow_id=f"list-test-{i}",
                document_id=f"doc-{i}",
                filename=f"test{i}.pdf",
                filepath=f"/app/books/test{i}.pdf",
                stage="registered"
            )

        docs = db_connection.list_documents()
        assert len(docs) >= 3

    @pytest.mark.db
    @pytest.mark.unit
    def test_list_documents_with_stage_filter(self, db_connection):
        """Test filtering documents by stage."""
        db_connection.upsert_document(
            workflow_id="filter-test-1",
            document_id="doc-filter-1",
            filename="test1.pdf",
            filepath="/app/books/test1.pdf",
            stage="completed"
        )
        db_connection.upsert_document(
            workflow_id="filter-test-2",
            document_id="doc-filter-2",
            filename="test2.pdf",
            filepath="/app/books/test2.pdf",
            stage="failed"
        )

        completed_docs = db_connection.list_documents(stage="completed")
        for doc in completed_docs:
            assert doc["stage"] == "completed"

    @pytest.mark.db
    @pytest.mark.unit
    def test_update_document_stage(self, db_connection):
        """Test updating document stage."""
        workflow_id = "stage-test"
        db_connection.upsert_document(
            workflow_id=workflow_id,
            document_id="doc-stage",
            filename="test.pdf",
            filepath="/app/books/test.pdf",
            stage="registered"
        )

        db_connection.update_document_stage(
            workflow_id=workflow_id,
            stage="ocr_review",
            page_count=5,
            chunk_count=10
        )

        doc = db_connection.get_document(workflow_id)
        assert doc["stage"] == "ocr_review"
        assert doc["page_count"] == 5
        assert doc["chunk_count"] == 10

    @pytest.mark.db
    @pytest.mark.unit
    def test_set_document_demo(self, db_connection):
        """Test marking document as demo."""
        workflow_id = "demo-test"
        db_connection.upsert_document(
            workflow_id=workflow_id,
            document_id="doc-demo",
            filename="test.pdf",
            filepath="/app/books/test.pdf",
            stage="registered"
        )

        db_connection.set_document_demo(workflow_id, True)
        doc = db_connection.get_document(workflow_id)
        assert doc["is_demo"] == 1

    @pytest.mark.db
    @pytest.mark.unit
    def test_set_document_disabled(self, db_connection):
        """Test soft-deleting document."""
        workflow_id = "disable-test"
        db_connection.upsert_document(
            workflow_id=workflow_id,
            document_id="doc-disable",
            filename="test.pdf",
            filepath="/app/books/test.pdf",
            stage="registered"
        )

        db_connection.set_document_disabled(workflow_id, True)
        doc = db_connection.get_document(workflow_id)
        assert doc["is_disabled"] == 1

    @pytest.mark.db
    @pytest.mark.unit
    def test_purge_document_removes_every_row(self, db_connection, sample_document):
        """Hard delete must leave nothing behind in any per-document table."""
        workflow_id = sample_document["workflow_id"]

        db_connection.save_pages(workflow_id, [
            {"page_number": 1, "original_markdown": "page one"},
            {"page_number": 2, "original_markdown": "page two"},
        ])
        db_connection.save_chunks(workflow_id, [
            {"chunk_number": 1, "original_text": "chunk one", "token_count": 10},
        ])
        db_connection.add_document_artifact(
            workflow_id=workflow_id,
            artifact_type="original",
            storage_uri="minio://documents/original.pdf",
            filename="original.pdf",
        )

        assert db_connection.get_pages(workflow_id)
        assert db_connection.get_chunks(workflow_id)
        assert db_connection.list_document_artifacts(workflow_id)

        deleted = db_connection.purge_document(workflow_id)

        assert db_connection.get_document(workflow_id) is None
        assert db_connection.get_pages(workflow_id) == []
        assert db_connection.get_chunks(workflow_id) == []
        assert db_connection.list_document_artifacts(workflow_id) == []
        assert deleted["pages"] == 2
        assert deleted["chunks"] == 1
        assert deleted["documents"] == 1

    @pytest.mark.db
    @pytest.mark.unit
    def test_purge_document_keeps_audit_by_default(self, db_connection, sample_document):
        """The audit trail is the record of the deletion, so it survives."""
        workflow_id = sample_document["workflow_id"]
        db_connection.log_audit(
            workflow_id=workflow_id,
            document_id=sample_document["document_id"],
            action_type="purge_document",
            entity_type="document",
        )

        db_connection.purge_document(workflow_id)

        logs = db_connection.get_audit_logs(workflow_id=workflow_id)
        assert len(logs) >= 1

    @pytest.mark.db
    @pytest.mark.unit
    def test_purge_document_can_drop_audit(self, db_connection, sample_document):
        workflow_id = sample_document["workflow_id"]
        db_connection.log_audit(
            workflow_id=workflow_id,
            document_id=sample_document["document_id"],
            action_type="purge_document",
            entity_type="document",
        )

        db_connection.purge_document(workflow_id, keep_audit=False)

        logs = db_connection.get_audit_logs(workflow_id=workflow_id)
        assert len(logs) == 0

    @pytest.mark.db
    @pytest.mark.unit
    def test_purge_document_is_idempotent(self, db_connection):
        """Purging an unknown or already-purged document must not raise."""
        assert db_connection.purge_document("never-existed") == {}


class TestDocumentValidityColumns:
    """The validity period columns on `documents`.

    db.py stores the dates it is handed and defaults nothing — the upload
    endpoint decides what a new document's period should be (see
    TestDocumentValidityEndpoints in test_api.py). These pin the storage half
    of that split.
    """

    @pytest.mark.db
    @pytest.mark.unit
    def test_a_period_is_stored_verbatim(self, db_connection):
        db_connection.upsert_document(
            workflow_id="wf-stamped",
            document_id="doc-stamped",
            filename="c.pdf",
            filepath="/books/c.pdf",
            valid_from="2026-09-23",
            valid_to="2027-09-23",
        )

        doc = db_connection.get_document("wf-stamped")

        assert doc["valid_from"] == "2026-09-23"
        assert doc["valid_to"] == "2027-09-23"

    @pytest.mark.db
    @pytest.mark.unit
    def test_a_document_created_without_a_period_stores_null(self, db_connection):
        # No default invented here — that is the caller's call.
        db_connection.upsert_document(
            workflow_id="wf-fresh",
            document_id="doc-fresh",
            filename="b.pdf",
            filepath="/books/b.pdf",
        )

        doc = db_connection.get_document("wf-fresh")

        assert doc["valid_from"] is None
        assert doc["valid_to"] is None

    @pytest.mark.db
    @pytest.mark.unit
    def test_set_document_validity_round_trips(self, db_connection):
        db_connection.upsert_document(
            workflow_id="wf-validity",
            document_id="doc-validity",
            filename="a.pdf",
            filepath="/books/a.pdf",
        )

        db_connection.set_document_validity("wf-validity", "2026-10-01", "2026-12-31")

        doc = db_connection.get_document("wf-validity")
        assert doc["valid_from"] == "2026-10-01"
        assert doc["valid_to"] == "2026-12-31"

    @pytest.mark.db
    @pytest.mark.unit
    def test_a_period_is_not_overwritten_by_a_later_upsert(self, db_connection):
        # INSERT-only: an approver's edit must survive a restart that
        # re-registers the same workflow.
        db_connection.upsert_document(
            workflow_id="wf-keep",
            document_id="doc-keep",
            filename="d.pdf",
            filepath="/books/d.pdf",
            valid_from="2026-01-01",
            valid_to="2026-06-30",
        )

        db_connection.upsert_document(
            workflow_id="wf-keep",
            document_id="doc-keep",
            filename="d.pdf",
            filepath="/books/d.pdf",
            stage="ocr_review",
        )

        doc = db_connection.get_document("wf-keep")
        assert doc["valid_from"] == "2026-01-01"
        assert doc["valid_to"] == "2026-06-30"


class TestPageOperations:
    """Tests for page CRUD operations."""

    @pytest.mark.db
    @pytest.mark.unit
    def test_persist_pages(self, db_connection, sample_document):
        """Test persisting pages to database."""
        pages = [
            {
                "page_number": 1,
                "original_markdown": "# Page 1",
                "detected_language": "en"
            },
            {
                "page_number": 2,
                "original_markdown": "# Page 2",
                "detected_language": "en"
            }
        ]

        db_connection.persist_document_content(
            workflow_id=sample_document["workflow_id"],
            pages=pages,
            chunks=[]
        )

        stored_pages = db_connection.get_pages(sample_document["workflow_id"])
        assert len(stored_pages) == 2
        assert stored_pages[0]["original_markdown"] == "# Page 1"

    @pytest.mark.db
    @pytest.mark.unit
    def test_get_page(self, db_connection, sample_document):
        """Test getting a specific page."""
        pages = [{"page_number": 1, "original_markdown": "Test", "detected_language": "en"}]
        db_connection.persist_document_content(
            workflow_id=sample_document["workflow_id"],
            pages=pages,
            chunks=[]
        )

        page = db_connection.get_page(sample_document["workflow_id"], 1)
        assert page is not None
        assert page["page_number"] == 1

    @pytest.mark.db
    @pytest.mark.unit
    def test_update_page(self, db_connection, sample_document):
        """Test updating a page."""
        pages = [{"page_number": 1, "original_markdown": "Original", "detected_language": "en"}]
        db_connection.persist_document_content(
            workflow_id=sample_document["workflow_id"],
            pages=pages,
            chunks=[]
        )

        db_connection.update_page(
            workflow_id=sample_document["workflow_id"],
            page_num=1,
            edited_markdown="Edited content",
            is_reviewed=True
        )

        page = db_connection.get_page(sample_document["workflow_id"], 1)
        assert page["edited_markdown"] == "Edited content"
        assert page["is_reviewed"] == 1

    @pytest.mark.db
    @pytest.mark.unit
    def test_reset_page(self, db_connection, sample_document):
        """Test resetting a page to original."""
        pages = [{"page_number": 1, "original_markdown": "Original", "detected_language": "en"}]
        db_connection.persist_document_content(
            workflow_id=sample_document["workflow_id"],
            pages=pages,
            chunks=[]
        )

        # Edit then reset
        db_connection.update_page(
            workflow_id=sample_document["workflow_id"],
            page_num=1,
            edited_markdown="Edited"
        )
        db_connection.reset_page(sample_document["workflow_id"], 1)

        page = db_connection.get_page(sample_document["workflow_id"], 1)
        assert page["edited_markdown"] is None


class TestChunkOperations:
    """Tests for chunk CRUD operations."""

    @pytest.mark.db
    @pytest.mark.unit
    def test_persist_chunks(self, db_connection, sample_document):
        """Test persisting chunks to database."""
        chunks = [
            {
                "chunk_number": 1,
                "original_text": "Chunk 1 text",
                "source_pages": [1],
                "token_count": 5
            },
            {
                "chunk_number": 2,
                "original_text": "Chunk 2 text",
                "source_pages": [1, 2],
                "token_count": 5
            }
        ]

        db_connection.persist_document_content(
            workflow_id=sample_document["workflow_id"],
            pages=[],
            chunks=chunks
        )

        stored_chunks = db_connection.get_chunks(sample_document["workflow_id"])
        assert len(stored_chunks) == 2

    @pytest.mark.db
    @pytest.mark.unit
    def test_get_chunks_excludes_excluded_by_default(self, db_connection, sample_document):
        """Test that excluded chunks are filtered out by default."""
        chunks = [
            {"chunk_number": 1, "original_text": "Included", "source_pages": [1], "token_count": 5},
            {"chunk_number": 2, "original_text": "Excluded", "source_pages": [1], "token_count": 5}
        ]
        db_connection.persist_document_content(
            workflow_id=sample_document["workflow_id"],
            pages=[],
            chunks=chunks
        )

        # Exclude chunk 2
        db_connection.update_chunk(
            workflow_id=sample_document["workflow_id"],
            chunk_num=2,
            is_excluded=True
        )

        # Default: should not include excluded
        chunks_filtered = db_connection.get_chunks(sample_document["workflow_id"])
        assert len(chunks_filtered) == 1

        # With include_excluded=True
        all_chunks = db_connection.get_chunks(sample_document["workflow_id"], include_excluded=True)
        assert len(all_chunks) == 2

    @pytest.mark.db
    @pytest.mark.unit
    def test_update_chunk(self, db_connection, sample_document):
        """Test updating a chunk."""
        chunks = [{"chunk_number": 1, "original_text": "Original", "source_pages": [1], "token_count": 5}]
        db_connection.persist_document_content(
            workflow_id=sample_document["workflow_id"],
            pages=[],
            chunks=chunks
        )

        db_connection.update_chunk(
            workflow_id=sample_document["workflow_id"],
            chunk_num=1,
            edited_text="Edited text",
            is_reviewed=True
        )

        chunk = db_connection.get_chunk(sample_document["workflow_id"], 1)
        assert chunk["edited_text"] == "Edited text"
        assert chunk["is_reviewed"] == 1


class TestAuditLogging:
    """Tests for audit logging functionality."""

    @pytest.mark.db
    @pytest.mark.unit
    def test_log_audit(self, db_connection, sample_document):
        """Test creating audit log entry."""
        db_connection.log_audit(
            workflow_id=sample_document["workflow_id"],
            document_id=sample_document["document_id"],
            action_type="test_action",
            entity_type="document",
            field_name="stage",
            old_value="old",
            new_value="new"
        )

        logs = db_connection.get_audit_logs(sample_document["workflow_id"])
        assert len(logs) >= 1
        assert logs[0]["action_type"] == "test_action"

    @pytest.mark.db
    @pytest.mark.unit
    def test_get_audit_logs_with_filter(self, db_connection, sample_document):
        """Test filtering audit logs by action type."""
        db_connection.log_audit(
            workflow_id=sample_document["workflow_id"],
            document_id=sample_document["document_id"],
            action_type="type_a"
        )
        db_connection.log_audit(
            workflow_id=sample_document["workflow_id"],
            document_id=sample_document["document_id"],
            action_type="type_b"
        )

        logs_a = db_connection.get_audit_logs(
            sample_document["workflow_id"],
            action_type="type_a"
        )
        assert all(log["action_type"] == "type_a" for log in logs_a)

    @pytest.mark.db
    @pytest.mark.unit
    def test_get_all_audit_logs(self, db_connection, sample_document):
        """Test getting global audit logs."""
        db_connection.log_audit(
            workflow_id=sample_document["workflow_id"],
            document_id=sample_document["document_id"],
            action_type="global_test"
        )

        logs = db_connection.get_all_audit_logs()
        assert len(logs) >= 1


class TestSettings:
    """Tests for settings management."""

    @pytest.mark.db
    @pytest.mark.unit
    def test_get_search_settings_defaults(self, db_connection):
        """Test getting default search settings."""
        settings = db_connection.get_search_settings()
        assert "searchMethod" in settings
        assert "limit" in settings
        assert "alpha" in settings

    @pytest.mark.db
    @pytest.mark.unit
    def test_update_search_settings(self, db_connection):
        """Test updating search settings."""
        db_connection.update_search_settings({"limit": 25})
        settings = db_connection.get_search_settings()
        assert settings["limit"] == 25

    @pytest.mark.db
    @pytest.mark.unit
    def test_settings_audit_log(self, db_connection):
        """Test that settings changes are logged."""
        db_connection.update_search_settings({"limit": 50})
        logs = db_connection.get_settings_audit_logs()
        assert len(logs) >= 1


def _new_doc(db, workflow_id, **kwargs):
    kwargs.setdefault("document_id", _md5(workflow_id))
    kwargs.setdefault("filename", f"{workflow_id}.pdf")
    kwargs.setdefault("filepath", f"minio://documents/{workflow_id}.pdf")
    return db.upsert_document(workflow_id=workflow_id, **kwargs)


class TestSchemaConstraints:
    """Rules the database itself enforces."""

    @pytest.mark.db
    @pytest.mark.unit
    def test_document_kind_must_match_its_catalog_entry_type(self, db_connection):
        with db_connection.get_connection() as conn:
            entry = db_connection.upsert_catalog_entry(type="scheme", code="x-scheme", name="X", conn=conn)
            with pytest.raises(psycopg.errors.ForeignKeyViolation):
                conn.execute(
                    "INSERT INTO documents (instance, source, document_kind, catalog_entry_id) "
                    "VALUES ('default', %s, 'advisory', %s::uuid)",
                    (psycopg.types.json.Jsonb({"fingerprint": _md5("k")}), entry["id"]),
                )

    @pytest.mark.db
    @pytest.mark.unit
    def test_source_requires_a_hex_fingerprint(self, db_connection):
        with db_connection.get_connection() as conn:
            with pytest.raises(psycopg.errors.CheckViolation):
                conn.execute(
                    "INSERT INTO documents (instance, source) VALUES ('default', %s)",
                    (psycopg.types.json.Jsonb({"fingerprint": "not-hex"}),),
                )

    @pytest.mark.db
    @pytest.mark.unit
    def test_stage_timestamps_only_allow_known_keys(self, db_connection):
        _new_doc(db_connection, "wf-ts")
        with db_connection.get_connection() as conn:
            with pytest.raises(psycopg.errors.CheckViolation):
                conn.execute(
                    "UPDATE workflows SET stage_timestamps = '{\"bogus\": \"x\"}'::jsonb "
                    "WHERE temporal_workflow_id = 'wf-ts'"
                )

    @pytest.mark.db
    @pytest.mark.unit
    def test_validity_period_must_not_end_before_it_starts(self, db_connection):
        with pytest.raises(psycopg.errors.CheckViolation):
            _new_doc(db_connection, "wf-bad-period", valid_from="2026-02-01", valid_to="2026-01-01")

    @pytest.mark.db
    @pytest.mark.unit
    def test_chunk_page_range_must_not_run_backwards(self, db_connection):
        _new_doc(db_connection, "wf-range")
        with pytest.raises(psycopg.errors.CheckViolation):
            with db_connection.get_connection() as conn:
                conn.execute(
                    "INSERT INTO chunks (document_id, chunk_number, original_text, page_start, page_end) "
                    "SELECT id, 1, 't', 3, 2 FROM documents"
                )

    @pytest.mark.db
    @pytest.mark.unit
    def test_audit_entry_needs_a_workflow_unless_system_level(self, db_connection):
        with db_connection.get_connection() as conn:
            conn.execute("INSERT INTO audit_logs (action_type, entity_type) VALUES ('x', 'setting')")
            with pytest.raises(psycopg.errors.CheckViolation):
                conn.execute("INSERT INTO audit_logs (action_type, entity_type) VALUES ('x', 'page')")

    @pytest.mark.db
    @pytest.mark.unit
    def test_search_settings_is_a_single_row(self, db_connection):
        with db_connection.get_connection() as conn:
            with pytest.raises(psycopg.errors.UniqueViolation):
                conn.execute("INSERT INTO search_settings DEFAULT VALUES")


class TestWorkflows:
    @pytest.mark.db
    @pytest.mark.unit
    def test_upsert_creates_the_main_workflow(self, db_connection):
        _new_doc(db_connection, "wf-main", stage="ocr_processing")
        jobs = db_connection.list_document_jobs("wf-main")
        assert [(j["job_type"], j["temporal_workflow_id"]) for j in jobs] == [("pipeline", "wf-main")]

    @pytest.mark.db
    @pytest.mark.unit
    def test_current_workflow_is_the_latest_by_start_time(self, db_connection):
        _new_doc(db_connection, "wf-cur", stage="completed")
        job_id = db_connection.create_document_job(
            "wf-cur", "chunking_retry", temporal_workflow_id="wf-cur-retry-chunking-1",
            current_stage="chunking",
        )
        doc = db_connection.get_document("wf-cur")
        assert doc["stage"] == "chunking"
        assert doc["latest_job_id"] == job_id
        assert doc["workflow_id"] == "wf-cur"

    @pytest.mark.db
    @pytest.mark.unit
    def test_stage_updates_follow_the_current_workflow(self, db_connection):
        _new_doc(db_connection, "wf-follow", stage="completed")
        db_connection.create_document_job("wf-follow", "ocr_retry", current_stage="ocr_processing")
        db_connection.update_document_stage("wf-follow", "ocr_review")
        by_type = {j["job_type"]: j["current_stage"] for j in db_connection.list_document_jobs("wf-follow")}
        assert by_type == {"pipeline": "completed", "ocr_retry": "ocr_review"}

    @pytest.mark.db
    @pytest.mark.unit
    def test_stage_timestamps_are_set_once(self, db_connection):
        _new_doc(db_connection, "wf-once")
        db_connection.update_document_stage("wf-once", "ocr_review")
        first = db_connection.get_document("wf-once")["ocr_completed_at"]
        assert first
        db_connection.update_document_stage("wf-once", "ocr_processing")
        db_connection.update_document_stage("wf-once", "ocr_review")
        assert db_connection.get_document("wf-once")["ocr_completed_at"] == first

    @pytest.mark.db
    @pytest.mark.unit
    def test_prod_request_is_a_waiting_promote_workflow_that_approval_adopts(self, db_connection):
        _new_doc(db_connection, "wf-prod", stage="approval_for_prod")
        doc = db_connection.mark_prod_ready_requested("wf-prod", user_id="kc-1", username="reviewer1")
        assert doc["prod_ready_requested_by_username"] == "reviewer1"
        latest = db_connection.get_latest_document_job("wf-prod")
        assert (latest["job_type"], latest["status"]) == ("promote_prod", "waiting_review")

        db_connection.create_document_job(
            "wf-prod", "promote_prod", temporal_workflow_id="wf-prod-promote-prod-1",
            current_stage="ingesting_prod",
        )
        jobs = [j for j in db_connection.list_document_jobs("wf-prod") if j["job_type"] == "promote_prod"]
        assert len(jobs) == 1
        assert jobs[0]["temporal_workflow_id"] == "wf-prod-promote-prod-1"
        assert db_connection.get_document("wf-prod")["prod_ready_requested_at"] is None

    @pytest.mark.db
    @pytest.mark.unit
    def test_job_config_merges(self, db_connection):
        _new_doc(db_connection, "wf-cfg")
        job = db_connection.get_latest_document_job("wf-cfg")
        db_connection.update_document_job(job["id"], config_json={"a": 1})
        db_connection.update_document_job(job["id"], config_json={"chunking_progress": {"percent": 5}})
        config = db_connection.get_document_job(job["id"])["config"]
        assert config == {"a": 1, "chunking_progress": {"percent": 5}}

    @pytest.mark.db
    @pytest.mark.unit
    def test_artifacts_belong_to_their_workflow_and_derive_pointers(self, db_connection):
        _new_doc(db_connection, "wf-art")
        job = db_connection.get_latest_document_job("wf-art")
        art_id = db_connection.add_document_artifact(
            "wf-art", "original_upload", "minio://documents/a.pdf", stage="registered", job_id=job["id"],
        )
        norm_id = db_connection.add_document_artifact(
            "wf-art", "normalized_pdf", "minio://documents/n.pdf", stage="ocr_processing",
        )
        doc = db_connection.get_document("wf-art")
        assert doc["original_artifact_id"] == art_id
        assert doc["normalized_artifact_id"] == norm_id
        artifact = db_connection.get_document_artifact(doc["id"], art_id)
        assert artifact["job_id"] == job["id"]
        assert artifact["workflow_id"] == "wf-art"
        assert db_connection.get_document_artifact("wf-art", "123") is None


class TestPageTranslations:
    @pytest.mark.db
    @pytest.mark.unit
    def test_an_ocr_save_keeps_existing_translations(self, db_connection):
        _new_doc(db_connection, "wf-tr")
        db_connection.save_pages("wf-tr", [{
            "page_number": 1, "original_markdown": "namaste", "detected_language": "hi",
            "translated_markdown": "hello", "translation_target_language": "en",
            "translation_provider": "p", "translation_model": "m",
        }])
        db_connection.save_pages("wf-tr", [{"page_number": 1, "original_markdown": "namaste"}])
        page = db_connection.get_page("wf-tr", 1)
        assert page["translated_markdown"] == "hello"
        assert page["translations"]["en"]["provider"] == "p"

    @pytest.mark.db
    @pytest.mark.unit
    def test_changed_ocr_text_drops_stale_translations(self, db_connection):
        _new_doc(db_connection, "wf-tr2")
        db_connection.save_pages("wf-tr2", [{
            "page_number": 1, "original_markdown": "old", "translated_markdown": "t",
            "translation_target_language": "en",
        }])
        db_connection.save_pages("wf-tr2", [{"page_number": 1, "original_markdown": "new"}])
        assert db_connection.get_page("wf-tr2", 1)["translations"] == {}

    @pytest.mark.db
    @pytest.mark.unit
    def test_reading_and_saving_pages_back_is_lossless(self, db_connection):
        _new_doc(db_connection, "wf-rt")
        db_connection.save_pages("wf-rt", [{
            "page_number": 1, "original_markdown": "a", "detected_language": "gu",
            "translated_markdown": "b", "translation_target_language": "en", "reviewer_notes": "n",
        }])
        before = db_connection.get_pages("wf-rt")
        db_connection.save_pages("wf-rt", before)
        assert db_connection.get_pages("wf-rt") == before

    @pytest.mark.db
    @pytest.mark.unit
    def test_translation_edits_target_one_language_entry(self, db_connection):
        _new_doc(db_connection, "wf-edit")
        db_connection.save_pages("wf-edit", [{
            "page_number": 1, "original_markdown": "a", "translated_markdown": "b",
            "translation_target_language": "en",
        }])
        db_connection.update_page(
            "wf-edit", 1, edited_translation="fixed", translation_reviewed=True, is_reviewed=True,
        )
        page = db_connection.get_page("wf-edit", 1)
        assert page["edited_translation"] == "fixed"
        assert page["translated_markdown"] == "b"
        assert page["translation_reviewed"] is True
        assert page["is_reviewed"] is True

    @pytest.mark.db
    @pytest.mark.unit
    def test_page_metadata_merges_instead_of_replacing(self, db_connection):
        _new_doc(db_connection, "wf-meta")
        db_connection.save_pages("wf-meta", [
            {"page_number": 1, "original_markdown": "a", "detected_language": "hi"}
        ])
        db_connection.update_page("wf-meta", 1, reviewer_notes="ok")
        page = db_connection.get_page("wf-meta", 1)
        assert page["detected_language"] == "hi"
        assert page["reviewer_notes"] == "ok"

    @pytest.mark.db
    @pytest.mark.unit
    def test_count_translated_pages(self, db_connection):
        _new_doc(db_connection, "wf-count")
        db_connection.save_pages("wf-count", [
            {"page_number": 1, "original_markdown": "a", "translated_markdown": "x",
             "translation_target_language": "en"},
            {"page_number": 2, "original_markdown": "b"},
        ])
        assert db_connection.count_translated_pages("wf-count") == 1


class TestChunkRuns:
    @pytest.mark.db
    @pytest.mark.unit
    def test_rechunking_replaces_chunks_and_bumps_the_run(self, db_connection):
        _new_doc(db_connection, "wf-run")
        first = [{"chunk_number": 1, "original_text": "a", "chunking_provider": "p1"}]
        db_connection.save_chunks("wf-run", first)
        old_id = db_connection.get_chunk("wf-run", 1)["id"]
        db_connection.save_chunks("wf-run", [
            {"chunk_number": 1, "original_text": "b", "chunking_provider": "p2",
             "chunking_config_json": '{"size": 450}'},
            {"chunk_number": 2, "original_text": "c", "chunking_provider": "p2"},
        ])
        doc = db_connection.get_document("wf-run")
        chunk = db_connection.get_chunk("wf-run", 1)
        assert doc["chunk_count"] == 2
        assert chunk["id"] != old_id
        assert chunk["chunk_version"] == 2
        assert chunk["chunking_provider"] == "p2"
        assert chunk["chunking_config_json"] == '{"size": 450}'

    @pytest.mark.db
    @pytest.mark.unit
    def test_a_failed_rechunk_leaves_the_previous_chunks(self, db_connection):
        _new_doc(db_connection, "wf-atomic")
        db_connection.save_chunks("wf-atomic", [{"chunk_number": 1, "original_text": "keep"}])
        with pytest.raises(psycopg.errors.UniqueViolation):
            db_connection.save_chunks("wf-atomic", [
                {"chunk_number": 1, "original_text": "x"},
                {"chunk_number": 1, "original_text": "dup"},
            ])
        assert db_connection.get_chunk("wf-atomic", 1)["original_text"] == "keep"
        assert db_connection.get_document("wf-atomic")["chunk_count"] == 1

    @pytest.mark.db
    @pytest.mark.unit
    def test_source_spans_round_trip_and_derive_page_numbers(self, db_connection):
        _new_doc(db_connection, "wf-spans")
        db_connection.save_chunks("wf-spans", [{
            "chunk_number": 1, "original_text": "a", "page_start": 2, "page_end": 3,
            "source_spans_json": '[{"page_number": 2, "start_char": 0, "end_char": 1}, '
                                 '{"page_number": 3, "start_char": 0, "end_char": 1}]',
        }])
        chunk = db_connection.get_chunk("wf-spans", 1)
        assert chunk["source_page_numbers_json"] == "[2, 3]"

    @pytest.mark.db
    @pytest.mark.unit
    def test_search_chunks_matches_edited_text_first(self, db_connection):
        _new_doc(db_connection, "wf-search", instance="mh")
        db_connection.save_chunks("wf-search", [
            {"chunk_number": 1, "original_text": "alpha beta", "edited_text": "gamma delta"},
        ])
        hits, total = db_connection.search_chunks(query="gamma")
        assert total == 1 and hits[0]["workflow_id"] == "wf-search"
        assert db_connection.search_chunks(query="alpha")[1] == 0
        assert db_connection.search_chunks(query="gamma", instances=["up"])[1] == 0


class TestIndexStatus:
    @pytest.mark.db
    @pytest.mark.unit
    def test_entries_merge_and_dev_prod_are_separate_keys(self, db_connection, monkeypatch):
        monkeypatch.setenv("PROD_VECTOR_DB_COLLECTION_NAME", "prod-docs")
        _new_doc(db_connection, "wf-idx")
        db_connection.upsert_document_index_status(
            "wf-idx", "documents-index", doc_id="abc", chunk_count_indexed=4,
            last_indexed_at="2026-01-01T00:00:00", status="indexed",
        )
        db_connection.upsert_document_index_status(
            "wf-idx", "documents-index", last_verified_at="2026-01-02T00:00:00", status="indexed",
        )
        db_connection.upsert_document_index_status("wf-idx", "prod-docs", status="missing")
        doc = db_connection.get_document("wf-idx")
        assert set(doc["index_status"]) == {"dev", "prod"}
        dev = db_connection.get_document_index_status("wf-idx", "documents-index")
        assert dev["chunk_count_indexed"] == 4
        assert dev["last_indexed_at"] == "2026-01-01T00:00:00"
        assert dev["last_verified_at"] == "2026-01-02T00:00:00"
        summaries = {s["index_name"]: s for s in db_connection.list_index_summaries()}
        assert summaries["documents-index"]["indexed_chunks"] == 4


class TestIdentifierResolution:
    @pytest.mark.db
    @pytest.mark.unit
    def test_every_documented_form_resolves(self, db_connection):
        fingerprint = _md5("content")
        _new_doc(db_connection, "doc-" + fingerprint, document_id=fingerprint, filename="pm.pdf")
        db_connection.create_document_job(
            "doc-" + fingerprint, "ocr_retry", temporal_workflow_id=f"doc-{fingerprint}-retry-ocr-9",
        )
        doc = db_connection.get_document("doc-" + fingerprint)
        for identifier in (
            doc["id"],
            "doc-" + fingerprint,
            f"doc-{fingerprint}-retry-ocr-9",
            fingerprint,
            "pm.pdf",
            _md5(fingerprint),
        ):
            found = db_connection.find_document_by_doc_identifier(identifier)
            assert found and found["id"] == doc["id"], identifier
        assert found["workflow_id"] == "doc-" + fingerprint
        assert db_connection.find_document_by_doc_identifier("nope") is None
        assert db_connection.get_document("00000000-0000-0000-0000-000000000000") is None


class TestAuditTrail:
    @pytest.mark.db
    @pytest.mark.unit
    def test_audit_outlives_the_document_and_keeps_tenant_scope(self, db_connection):
        _new_doc(db_connection, "wf-aud", instance="mh")
        db_connection.log_audit(
            workflow_id="wf-aud", document_id="x", action_type="purge_document",
            entity_type="document", actor_user_id="kc-9", actor_username="boss",
            actor_email="b@x.in", actor_roles="superadmin",
        )
        db_connection.purge_document("wf-aud")

        logs = db_connection.get_audit_logs("wf-aud")
        assert [entry["action_type"] for entry in logs] == ["purge_document"]
        assert logs[0]["actor"] == "b@x.in (superadmin)"
        assert logs[0]["actor_user_id"] == "kc-9"
        assert db_connection.get_all_audit_log_count(instances=["mh"]) == 1
        assert db_connection.get_all_audit_log_count(instances=["up"]) == 0

    @pytest.mark.db
    @pytest.mark.unit
    def test_system_entries_need_no_document(self, db_connection):
        db_connection.log_audit(
            workflow_id="__catalog__", document_id="__catalog__", action_type="catalog_rebuild",
            entity_type="catalog",
        )
        logs = db_connection.get_all_audit_logs(action_type="catalog_rebuild")
        assert logs[0]["workflow_id"] == "__catalog__"

    @pytest.mark.db
    @pytest.mark.unit
    def test_settings_changes_are_logged_per_key_with_group_names(self, db_connection):
        db_connection.update_search_settings({"alpha": 0.2, "limit": 3})
        fields = {entry["field_name"] for entry in db_connection.get_settings_audit_logs()}
        assert fields == {"retrieval.alpha", "results.limit"}
        assert db_connection.get_settings_audit_count() == 2
        db_connection.update_search_settings({"alpha": 0.2})
        assert db_connection.get_settings_audit_count() == 2


class TestSearchSettings:
    @pytest.mark.db
    @pytest.mark.unit
    def test_defaults_come_from_the_models(self, db_connection):
        settings = db_connection.get_search_settings()
        assert settings["alpha"] == 0.6
        assert len(settings) == 15

    @pytest.mark.db
    @pytest.mark.unit
    def test_values_are_validated(self, db_connection):
        with pytest.raises(ValueError):
            db_connection.update_search_settings({"alpha": 3})
        with pytest.raises(ValueError):
            db_connection.update_search_settings({"searchMethod": "FUZZY"})
        with pytest.raises(ValueError):
            db_connection.update_search_settings({"nonsense": 1})

    @pytest.mark.db
    @pytest.mark.unit
    def test_a_partial_update_keeps_the_rest_of_its_group(self, db_connection):
        db_connection.update_search_settings({"alpha": 0.3})
        db_connection.update_search_settings({"hybridRrfK": 10})
        settings = db_connection.get_search_settings()
        assert (settings["alpha"], settings["hybridRrfK"]) == (0.3, 10)


class TestCatalogView:
    @staticmethod
    def _scheme_doc(db, workflow_id, code, stage, **kwargs):
        _new_doc(db, workflow_id, stage=stage, **kwargs)
        db.update_document_fields(
            workflow_id, document_kind="scheme", scheme_code=code, scheme_name=code.upper(),
        )

    @pytest.mark.db
    @pytest.mark.unit
    def test_status_rules(self, db_connection):
        db = db_connection
        self._scheme_doc(db, "wf-live", "live-one", "completed")
        self._scheme_doc(db, "wf-stale", "stale-one", "completed")
        db.mark_document_reindex_required("wf-stale", True, "edit")
        self._scheme_doc(db, "wf-gate", "gate-one", "approval_for_prod")
        self._scheme_doc(db, "wf-wip", "wip-one", "chunk_review")
        self._scheme_doc(db, "wf-off", "off-one", "completed")
        db.set_document_disabled("wf-off", True)
        db.upsert_catalog_entry(type="scheme", code="boot-one", name="Boot", source="bootstrap")

        status = {e["code"]: e["status"] for e in db.list_catalog_entries()}
        assert status == {
            "live-one": "live",
            "stale-one": "pending_reindex",
            "gate-one": "pending_prod",
            "wip-one": "disabled",
            "off-one": "disabled",
            "boot-one": "live",
        }

    @pytest.mark.db
    @pytest.mark.unit
    def test_computed_columns(self, db_connection):
        db = db_connection
        self._scheme_doc(db, "wf-a", "multi", "completed", instance="mh")
        self._scheme_doc(db, "wf-b", "multi", "completed", instance="up")
        db.save_chunks("wf-a", [{"chunk_number": 1, "original_text": "x"}])
        db.save_chunks("wf-b", [{"chunk_number": 1, "original_text": "y"},
                                {"chunk_number": 2, "original_text": "z"}])
        db.update_document_fields("wf-b", network_visible=False)
        db.upsert_document_index_status("wf-a", "documents-index", last_indexed_at="2026-05-01T00:00:00")

        entry = db.get_catalog_entry("multi")
        assert entry["instances"] == ["mh", "up"]
        assert sorted(entry["workflow_ids"]) == ["wf-a", "wf-b"]
        assert entry["chunk_count"] == 3
        assert entry["network_visible"] is True
        assert entry["promoted_at"] is None

    @pytest.mark.db
    @pytest.mark.unit
    def test_entries_without_documents_default_to_the_default_instance(self, db_connection):
        db_connection.upsert_catalog_entry(type="scheme", code="lonely", name="Lonely", source="import")
        entry = db_connection.get_catalog_entry("lonely")
        assert entry["instances"] == ["default"]
        assert entry["status"] == "disabled"
        assert entry["network_visible"] is False

    @pytest.mark.db
    @pytest.mark.unit
    def test_other_entry_types_stay_out_of_the_scheme_listing(self, db_connection):
        _new_doc(db_connection, "wf-adv", stage="completed")
        db_connection.update_document_fields(
            "wf-adv", document_kind="advisory", scheme_code="kharif", scheme_name="Kharif",
        )
        assert db_connection.list_catalog_entries() == []
        assert [e["code"] for e in db_connection.list_catalog_entries(type="advisory")] == ["kharif"]

    @pytest.mark.db
    @pytest.mark.unit
    def test_changing_the_kind_moves_or_unlinks_the_entry(self, db_connection):
        _new_doc(db_connection, "wf-move")
        db_connection.update_document_fields(
            "wf-move", document_kind="scheme", scheme_code="movable", scheme_name="Movable",
        )
        db_connection.update_document_fields("wf-move", document_kind="advisory")
        doc = db_connection.get_document("wf-move")
        assert (doc["document_kind"], doc["scheme_code"], doc["scheme_name"]) == (
            "advisory", "movable", "Movable",
        )
        db_connection.update_document_fields("wf-move", document_kind="document")
        doc = db_connection.get_document("wf-move")
        assert doc["scheme_code"] is None and doc["catalog_entry_id"] is None

    @pytest.mark.db
    @pytest.mark.unit
    def test_version_counts_bumps(self, db_connection):
        assert db_connection.get_catalog_meta()["version"] == 0
        assert db_connection.bump_catalog_version("a") == 1
        assert db_connection.bump_catalog_version("b") == 2
        meta = db_connection.get_catalog_meta()
        assert (meta["version"], meta["notes"]) == (2, "b")


class TestUsersAndOtps:
    @pytest.mark.db
    @pytest.mark.unit
    def test_upsert_user_refreshes_and_survives_an_email_clash(self, db_connection):
        first = db_connection.upsert_user("kc-a", "ann", "ann@x.in", ["reviewer"])
        again = db_connection.upsert_user("kc-a", "ann2", None, ["admin"])
        assert first == again
        user = db_connection.get_user_by_keycloak_id("kc-a")
        assert (user["username"], user["email"], user["roles"]) == ("ann2", "ann@x.in", ["admin"])
        other = db_connection.upsert_user("kc-b", "bob", "ANN@x.in", [])
        assert other and other != first
        assert db_connection.get_user_by_keycloak_id("kc-b")["email"] is None

    @pytest.mark.db
    @pytest.mark.unit
    def test_uploader_is_a_user_row(self, db_connection):
        _new_doc(db_connection, "wf-up", uploaded_by_user_id="kc-u", uploaded_by_username="uma",
                 uploaded_by_email="u@x.in", uploaded_by_roles="contributor,reviewer")
        doc = db_connection.get_document("wf-up")
        assert (doc["uploaded_by_user_id"], doc["uploaded_by_username"]) == ("kc-u", "uma")
        assert doc["uploaded_by_roles"] == "contributor,reviewer"

    @pytest.mark.db
    @pytest.mark.unit
    def test_otp_attempts_increment_atomically(self, db_connection):
        db_connection.store_email_otp(
            email="A@X.in", code_hash="h", expires_at="2099-01-01T00:00:00+00:00",
            created_at="x", last_sent_at="2026-01-01T00:00:00+00:00",
        )
        assert db_connection.increment_email_otp_attempts("a@x.in") == 1
        assert db_connection.increment_email_otp_attempts("a@x.in") == 2
        row = db_connection.get_email_otp("a@x.in")
        assert row["attempts"] == 2
        assert row["expires_at"].endswith("+00:00")
        db_connection.store_email_otp(
            email="a@x.in", code_hash="h2", expires_at="2099-01-01T00:00:00+00:00",
            created_at="x", last_sent_at="2026-01-02T00:00:00+00:00",
        )
        assert db_connection.get_email_otp("a@x.in")["attempts"] == 0
        db_connection.delete_email_otp("a@x.in")
        assert db_connection.get_email_otp("a@x.in") is None


class TestOperationsViews:
    @pytest.mark.db
    @pytest.mark.unit
    def test_summary_counts_and_queue_use_the_current_workflow(self, db_connection):
        _new_doc(db_connection, "wf-q1", stage="ocr_review", instance="mh")
        _new_doc(db_connection, "wf-q2", stage="completed", instance="up")
        db_connection.create_document_job("wf-q2", "reingest", current_stage="ingesting")
        counts = db_connection.get_document_summary_counts()
        assert counts["total_documents"] == 2
        assert counts["ocr_review_documents"] == 1
        assert counts["completed_documents"] == 0
        assert counts["running_jobs"] == 3
        assert db_connection.get_document_summary_counts(instances=[])["total_documents"] == 0
        by_instance = {r["instance"]: r for r in db_connection.get_document_counts_by_instance()}
        assert by_instance["mh"]["dev_approval"] == 1

        rows, total = db_connection.list_operations_queue()
        assert total == 2
        assert {r["workflow_id"]: r["job_type"] for r in rows} == {
            "wf-q1": "pipeline", "wf-q2": "reingest",
        }
        assert db_connection.list_operations_queue(instances=["up"])[1] == 1

    @pytest.mark.db
    @pytest.mark.unit
    def test_demo_and_disabled_documents_are_hidden_by_default(self, db_connection):
        _new_doc(db_connection, "wf-demo")
        _new_doc(db_connection, "wf-off")
        db_connection.set_document_demo("wf-demo", True)
        db_connection.set_document_disabled("wf-off", True)
        assert db_connection.list_documents() == []
        assert len(db_connection.list_documents(include_demo=True, include_disabled=True)) == 2

    @pytest.mark.db
    @pytest.mark.unit
    def test_reconcile_moves_a_stalled_document_into_review(self, db_connection):
        _new_doc(db_connection, "wf-rec", stage="ocr_processing")
        db_connection.save_pages("wf-rec", [{"page_number": 1, "original_markdown": "a"}])
        result = db_connection.reconcile_materialized_state("wf-rec")
        assert result["updated"] is True and result["stage"] == "ocr_review"
        assert db_connection.get_latest_document_job("wf-rec")["status"] == "waiting_review"

    @pytest.mark.db
    @pytest.mark.unit
    def test_webhook_upsert_skips_unchanged_payloads(self, db_connection):
        db = db_connection
        assert db.upsert_webhook_records("webhook", [{"contentID": "c1", "title": "T", "isPublished": True}]) == 1
        assert db.upsert_webhook_records("webhook", [{"contentID": "c1", "title": "T", "isPublished": True}]) == 1
        with db.get_connection() as conn:
            rows = conn.execute("SELECT title, is_published FROM webhook_records").fetchall()
        assert rows == [{"title": "T", "is_published": True}]


class TestMigrationRunner:
    @pytest.mark.db
    @pytest.mark.unit
    def test_applied_migrations_are_recorded_and_not_reapplied(self, db_connection, postgres_url):
        from pipeline import migrate

        assert migrate.apply_migrations(postgres_url) == []
        assert migrate.is_current(postgres_url)
        assert migrate.current_version(postgres_url) == migrate.list_migrations()[-1][0]

    @pytest.mark.db
    @pytest.mark.unit
    def test_migration_files_are_numbered_and_unversioned_by_schema_name(self):
        from pipeline import migrate

        names = [path.name for _, path in migrate.list_migrations()]
        assert names[0] == "0001_initial_schema.sql"

    @pytest.mark.db
    @pytest.mark.unit
    def test_a_new_migration_runs_once(self, db_connection, postgres_url, tmp_path):
        from pipeline import migrate

        (tmp_path / "0001_initial_schema.sql").write_text("SELECT 1;")
        (tmp_path / "0099_probe.sql").write_text("CREATE TABLE mig_probe (n int);")
        try:
            assert migrate.apply_migrations(postgres_url, tmp_path) == ["0099"]
            assert migrate.apply_migrations(postgres_url, tmp_path) == []
        finally:
            with db_connection.get_connection() as conn:
                conn.execute("DROP TABLE IF EXISTS mig_probe")
                conn.execute("DELETE FROM schema_migrations WHERE version = '0099'")

    @pytest.mark.db
    @pytest.mark.unit
    def test_a_failing_migration_applies_nothing_of_itself(self, db_connection, postgres_url, tmp_path):
        from pipeline import migrate

        (tmp_path / "0001_initial_schema.sql").write_text("SELECT 1;")
        (tmp_path / "0099_bad.sql").write_text("CREATE TABLE mig_half (n int); SELECT 1/0;")
        with pytest.raises(psycopg.errors.DivisionByZero):
            migrate.apply_migrations(postgres_url, tmp_path)
        with db_connection.get_connection() as conn:
            assert conn.execute("SELECT to_regclass('mig_half') AS t").fetchone()["t"] is None
            assert conn.execute(
                "SELECT count(*) AS n FROM schema_migrations WHERE version = '0099'"
            ).fetchone()["n"] == 0

    @pytest.mark.db
    @pytest.mark.unit
    def test_migration_file_names_are_validated(self, tmp_path):
        from pipeline import migrate

        (tmp_path / "init.sql").write_text("SELECT 1;")
        with pytest.raises(ValueError):
            migrate.list_migrations(tmp_path)
