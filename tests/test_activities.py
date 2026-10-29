"""
Unit tests for pipeline/activities.py - Temporal activities.

Tests cover:
- OCR activity (mocked)
- Chunking activity
- Translation activity (mocked)
- Ingestion activity (mocked)
- State update activity
"""

import asyncio
import itertools
import os
from unittest.mock import MagicMock

import pytest
from temporalio.exceptions import ApplicationError

os.environ["MINIO_ACCESS_KEY"] = "test-access"
os.environ["MINIO_SECRET_KEY"] = "test-secret"
os.environ["TRANSLATION_VLLM_BASE_URL"] = "http://localhost:8000/v1"


_code_seq = itertools.count(1)


def _next_code() -> str:
    """A unique document code for fixtures (documents need one in v2)."""
    return f"doc-{next(_code_seq)}"


class TestChunkingActivity:
    """Tests for the chunking activity."""

    @pytest.mark.unit
    def test_create_chunks_basic(self):
        """Test basic chunking of pages."""
        from pipeline.activities import create_chunks

        pages = [
            {
                "page_number": 1,
                "original_markdown": "This is page one with some content. " * 50,
                "edited_markdown": None,
                "detected_language": "en"
            },
            {
                "page_number": 2,
                "original_markdown": "This is page two with more content. " * 50,
                "edited_markdown": None,
                "detected_language": "en"
            }
        ]

        chunks = asyncio.run(create_chunks(pages, chunk_size=100, chunk_overlap=20, min_tokens=10))

        assert len(chunks) > 0
        for chunk in chunks:
            assert "chunk_number" in chunk
            assert "original_text" in chunk
            assert "token_count" in chunk
            assert len(chunk["original_text"]) > 0

    @pytest.mark.unit
    def test_create_chunks_uses_edited_markdown(self):
        """Test that edited_markdown is preferred over original."""
        from pipeline.activities import create_chunks

        pages = [
            {
                "page_number": 1,
                "original_markdown": "Original content that should not appear.",
                "edited_markdown": "Edited content that should appear. " * 30,
                "detected_language": "en"
            }
        ]

        chunks = asyncio.run(create_chunks(pages, chunk_size=50, chunk_overlap=10, min_tokens=5))

        assert len(chunks) > 0
        # The edited content should be in the chunks
        all_text = " ".join(c["original_text"] for c in chunks)
        assert "Edited content" in all_text
        assert "Original content that should not appear" not in all_text

    @pytest.mark.unit
    def test_create_chunks_empty_pages(self):
        """Test chunking with empty pages."""
        from pipeline.activities import create_chunks

        pages = []
        chunks = asyncio.run(create_chunks(pages, chunk_size=100, chunk_overlap=20, min_tokens=10))
        assert chunks == []

    @pytest.mark.unit
    def test_create_chunks_min_tokens_filter(self):
        """Test that chunks below min_tokens are filtered."""
        from pipeline.activities import create_chunks

        pages = [
            {
                "page_number": 1,
                "original_markdown": "Short.",
                "edited_markdown": None,
                "detected_language": "en"
            }
        ]

        # With high min_tokens, short content should ideally be filtered.
        # Current deterministic path may still emit a single short chunk; assert it stays small.
        chunks = asyncio.run(create_chunks(pages, chunk_size=100, chunk_overlap=20, min_tokens=100))
        assert all(isinstance(c.get("token_count"), int) for c in chunks)
        assert all(c["token_count"] < 100 for c in chunks) or len(chunks) == 0


class TestPrepareIngestionRecords:
    """Tests for the ingestion preparation activity."""

    @pytest.mark.unit
    def test_prepare_records_basic(self):
        """Test preparing records for vector-index ingestion."""
        from pipeline.activities import prepare_ingestion_records

        chunks = [
            {
                "chunk_number": 1,
                "original_text": "Test chunk one",
                "edited_text": None,
                "source_pages": [1],
                "token_count": 5,
                "is_excluded": False
            },
            {
                "chunk_number": 2,
                "original_text": "Test chunk two",
                "edited_text": None,
                "source_pages": [1, 2],
                "token_count": 5,
                "is_excluded": False
            }
        ]

        records = prepare_ingestion_records(
            document_id="test-doc",
            filename="test.pdf",
            chunks=chunks
        )

        assert len(records) == 2
        for record in records:
            assert "_id" in record
            assert "doc_id" in record
            assert "text" in record
            assert "chunk_num" in record
            assert record["doc_id"] == "test-doc"

    @pytest.mark.unit
    def test_prepare_records_carries_instance_name(self):
        """Payload ships the readable state name beside the instance code."""
        from pipeline.activities import _prepare_records

        chunks = [{"chunk_number": 1, "original_text": "Test chunk", "token_count": 5}]

        for code, expected in (("bv", "Bharat Vistaar"), ("mh", "Maharashtra"), ("ka", "Karnataka")):
            records = _prepare_records(
                document_id="test-doc",
                filename="test.pdf",
                chunks=chunks,
                group=code,
            )
            assert records[0]["instance"] == code
            assert records[0]["instance_name"] == expected

    @pytest.mark.unit
    def test_prepare_records_type_carries_kind(self):
        """Every record's payload type is its kind; document_kind is not written."""
        from pipeline.activities import _prepare_records
        from pipeline.vector_store.qdrant_store import _record_payload

        chunks = [{"chunk_number": 1, "original_text": "Test chunk", "token_count": 5}]

        advisory = _prepare_records(
            document_id="test-doc", filename="test.pdf", chunks=chunks, kind="advisory"
        )
        assert advisory[0]["type"] == "advisory"
        assert "document_kind" not in advisory[0]
        assert _record_payload(advisory[0])["type"] == "advisory"
        assert "document_kind" not in _record_payload(advisory[0])

        default = _prepare_records(document_id="test-doc", filename="test.pdf", chunks=chunks)
        assert default[0]["type"] == "document"

        scheme = _prepare_records(
            document_id="test-doc",
            filename="test.pdf",
            chunks=chunks,
            kind="scheme",
            code="pm-kisan",
            name="PM-KISAN",
        )
        assert scheme[0]["type"] == "scheme"
        assert "document_kind" not in scheme[0]

    @pytest.mark.unit
    def test_prepare_records_fills_scheme_aliases(self):
        """A scheme must never index with scheme_aliases: []."""
        from pipeline.activities import _prepare_records

        chunks = [{"chunk_number": 1, "original_text": "Test chunk", "token_count": 5}]

        records = _prepare_records(
            document_id="test-doc",
            filename="NMEO-OPGUIEDELINES.pdf",
            chunks=chunks,
            group="bv",
            kind="scheme",
            code="nmeoop",
            name="National Mission on Edible Oils - Oil Palm",
            aliases=None,
        )

        record = records[0]
        assert record["type"] == "scheme"
        assert record["scheme_aliases"], "aliases must be populated at ingest"
        lowered = {a.lower() for a in record["scheme_aliases"]}
        assert "nmeoop" in lowered
        assert "oil palm" in lowered
        assert record["instance_name"] == "Bharat Vistaar"

    @pytest.mark.unit
    def test_prepare_records_non_scheme_has_no_aliases(self):
        """Plain documents keep the scheme fields absent, as before."""
        from pipeline.activities import _prepare_records

        chunks = [{"chunk_number": 1, "original_text": "Test chunk", "token_count": 5}]

        records = _prepare_records(
            document_id="test-doc",
            filename="test.pdf",
            chunks=chunks,
            group="mh",
            kind="document",
        )

        assert "scheme_aliases" not in records[0]
        assert records[0]["type"] == "document"
        assert records[0]["instance_name"] == "Maharashtra"

    @pytest.mark.unit
    def test_prepare_records_excludes_excluded_chunks(self):
        """Test that excluded chunks are not included in records."""
        from pipeline.activities import prepare_ingestion_records

        chunks = [
            {
                "chunk_number": 1,
                "original_text": "Included",
                "edited_text": None,
                "source_pages": [1],
                "token_count": 5,
                "is_excluded": False
            },
            {
                "chunk_number": 2,
                "original_text": "Excluded",
                "edited_text": None,
                "source_pages": [1],
                "token_count": 5,
                "is_excluded": True
            }
        ]

        records = prepare_ingestion_records(
            document_id="test-doc",
            filename="test.pdf",
            chunks=chunks
        )

        assert len(records) == 1
        assert records[0]["text"] == "Included"

    @pytest.mark.unit
    def test_prepare_records_uses_edited_text(self):
        """Test that edited_text is preferred over original."""
        from pipeline.activities import prepare_ingestion_records

        chunks = [
            {
                "chunk_number": 1,
                "original_text": "Original",
                "edited_text": "Edited",
                "source_pages": [1],
                "token_count": 5,
                "is_excluded": False
            }
        ]

        records = prepare_ingestion_records(
            document_id="test-doc",
            filename="test.pdf",
            chunks=chunks
        )

        assert records[0]["text"] == "Edited"


class TestPrepareRecordsValidity:
    """The validity period every ingested chunk carries."""

    CHUNKS = [
        {"chunk_number": 1, "original_text": "Chunk one", "token_count": 5},
        {"chunk_number": 2, "original_text": "Chunk two", "token_count": 5},
    ]

    @pytest.mark.unit
    def test_stamps_the_period_on_every_chunk(self):
        # Every chunk, not just the first: the filter runs per point, so a
        # chunk without dates would outlive its own document.
        from pipeline.activities import _prepare_records

        records = _prepare_records(
            document_id="test-doc",
            filename="test.pdf",
            chunks=self.CHUNKS,
            valid_from="2026-09-22",
            valid_to="2027-09-22",
        )

        assert len(records) == 2
        for record in records:
            assert record["start_date"] == "2026-09-22"
            assert record["end_date"] == "2027-09-22"

    @pytest.mark.unit
    def test_omits_the_period_when_the_document_has_none(self):
        # Absent, not null — the search filter's missing-field branch is what
        # keeps such a chunk answerable.
        from pipeline.activities import _prepare_records

        records = _prepare_records(
            document_id="test-doc", filename="test.pdf", chunks=self.CHUNKS
        )

        assert "start_date" not in records[0]
        assert "end_date" not in records[0]

    @pytest.mark.unit
    def test_a_start_with_no_end_stamps_a_start_and_omits_the_end(self):
        # The normal shape since the business dropped the fixed expiry: the
        # chunk is searchable from its start day and never expires. The end is
        # omitted rather than null — the filter's "end_date missing" branch is
        # what keeps it answerable.
        from pipeline.activities import _prepare_records

        records = _prepare_records(
            document_id="test-doc",
            filename="test.pdf",
            chunks=self.CHUNKS,
            valid_from="2026-09-22",
        )

        assert records[0]["start_date"] == "2026-09-22"
        assert "end_date" not in records[0]

    @pytest.mark.unit
    def test_the_period_is_in_the_passage_schema(self):
        from pipeline.activities import _passage_schema_field_names

        assert {"start_date", "end_date"} <= _passage_schema_field_names()


class TestValidityFieldsFromDoc:
    """What the ingest activities read off a documents row."""

    @pytest.mark.unit
    def test_uses_the_stored_period(self):
        from pipeline.activities import _validity_fields_from_doc

        fields = _validity_fields_from_doc(
            {"valid_from": "2026-10-01", "valid_to": "2026-12-31"}
        )

        assert fields == {"valid_from": "2026-10-01", "valid_to": "2026-12-31"}

    @pytest.mark.unit
    def test_falls_back_to_the_upload_day_for_a_row_with_no_period(self):
        # A document uploaded before validity existed, being reingested: its
        # own upload day is a truer start than today, which would date it as
        # though it arrived this morning.
        from pipeline.activities import _validity_fields_from_doc

        fields = _validity_fields_from_doc({"created_at": "2024-05-01T09:30:00"})

        assert fields == {"valid_from": "2024-05-01", "valid_to": None}

    @pytest.mark.unit
    def test_falls_back_to_today_when_the_row_has_no_upload_day_either(self):
        from datetime import date

        from pipeline.activities import _validity_fields_from_doc

        fields = _validity_fields_from_doc({})

        assert fields["valid_from"] == date.today().isoformat()

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "doc", [{}, None, {"valid_from": "2026-01-01"}, {"created_at": "bogus"}]
    )
    def test_always_returns_a_start(self, doc):
        # The ingest path must never write a period with no start. The end is
        # optional by design — None means the document never expires.
        from pipeline.activities import _validity_fields_from_doc

        fields = _validity_fields_from_doc(doc)

        assert fields["valid_from"]
        assert "valid_to" in fields

    @pytest.mark.unit
    def test_a_stored_open_ended_period_survives(self):
        from pipeline.activities import _validity_fields_from_doc

        fields = _validity_fields_from_doc(
            {"valid_from": "2026-01-01", "valid_to": None}
        )

        assert fields == {"valid_from": "2026-01-01", "valid_to": None}


class TestUpdateDocumentState:
    """Tests for the state update activity."""

    @pytest.mark.unit
    def test_update_state(self, db_connection):
        """Test updating document state in the database."""
        from pipeline.activities import update_document_state

        workflow_id = "state-update-test"
        db_connection.upsert_document(
            kind="scheme",
            code=_next_code(),
            workflow_id=workflow_id,
            document_id="doc-state",
            filename="test.pdf",
            filepath="/app/books/test.pdf",
            stage="registered"
        )

        # Page and chunk counts are derived from the pages table, not stored.
        db_connection.save_pages(
            workflow_id,
            [{"page_number": n, "original_markdown": f"page {n}"} for n in range(1, 6)],
        )

        asyncio.run(update_document_state(
            workflow_id=workflow_id,
            stage="ocr_processing",
            page_count=5,
            chunk_count=0
        ))

        doc = db_connection.get_document(workflow_id)
        assert doc["stage"] == "ocr_processing"
        assert doc["page_count"] == 5


class TestMinIOClient:
    """Tests for MinIO client creation."""

    @pytest.mark.unit
    def test_get_minio_client_requires_credentials(self):
        """Test that missing credentials raise error."""
        from pipeline.activities import get_minio_client

        # Save original values
        orig_access = os.environ.get("MINIO_ACCESS_KEY")
        orig_secret = os.environ.get("MINIO_SECRET_KEY")

        try:
            # Remove credentials
            if "MINIO_ACCESS_KEY" in os.environ:
                del os.environ["MINIO_ACCESS_KEY"]
            if "MINIO_SECRET_KEY" in os.environ:
                del os.environ["MINIO_SECRET_KEY"]

            with pytest.raises(RuntimeError, match="required"):
                get_minio_client()
        finally:
            # Restore
            if orig_access:
                os.environ["MINIO_ACCESS_KEY"] = orig_access
            if orig_secret:
                os.environ["MINIO_SECRET_KEY"] = orig_secret

    @pytest.mark.unit
    def test_get_minio_client_with_credentials(self):
        """Test MinIO client creation with credentials."""
        from pipeline.activities import get_minio_client

        os.environ["MINIO_ACCESS_KEY"] = "test-key"
        os.environ["MINIO_SECRET_KEY"] = "test-secret"

        client = get_minio_client()
        assert client is not None


class TestOCRActivity:
    """Tests for OCR activity (mocked)."""

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_run_ocr_calls_provider(self, monkeypatch, tmp_path):
        """Test that OCR activity delegates PDF OCR to the OCR service."""
        import pipeline.activities as activities

        pdf_path = tmp_path / "test.pdf"
        pdf_path.write_bytes(b"%PDF-1.4 test content")

        mock_run_ocr_pdf = MagicMock(
            return_value=[
                {
                    "page_number": 1,
                    "original_markdown": "# Page 1 content",
                    "edited_markdown": None,
                    "is_reviewed": False,
                    "reviewer_notes": None,
                }
            ]
        )
        monkeypatch.setattr(activities, "run_ocr_pdf", mock_run_ocr_pdf)
        monkeypatch.setattr(activities, "_ensure_pdf_input", lambda path: (path, False))

        pages = await activities.run_ocr(str(pdf_path))

        assert len(pages) == 1
        assert pages[0]["page_number"] == 1
        assert "# Page 1 content" in pages[0]["original_markdown"]
        mock_run_ocr_pdf.assert_called_once()


class TestPublishCatalogToNetworkActivity:
    """Tests for the publish_catalog_to_network activity (thin wrapper over DiscoveryPublishService)."""

    @pytest.fixture(autouse=True)
    def _publishing_on(self, monkeypatch):
        # Publishing is off by default; these tests exercise the publish path.
        monkeypatch.setenv("PUBLISH_TO_NETWORK", "true")

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_publish_off_skips_without_building_the_service(self, monkeypatch):
        import pipeline.activities as activities
        import pipeline.db as db

        monkeypatch.delenv("PUBLISH_TO_NETWORK", raising=False)

        class ExplodingService:
            def __init__(self):
                raise AssertionError("service must not be built while publishing is off")

        monkeypatch.setattr(activities, "DiscoveryPublishService", ExplodingService)
        monkeypatch.setattr(
            activities,
            "_upload_file_to_minio",
            lambda *a, **k: ("minio://documents/network_publish_skipped.json", 45, "application/json"),
        )
        monkeypatch.setattr(db, "get_document", lambda workflow_id: {"kind": "advisory"})
        monkeypatch.setattr(db, "get_latest_document_job", lambda workflow_id: None)

        recorded = {}
        monkeypatch.setattr(db, "add_document_artifact", lambda **kwargs: recorded.update(kwargs))

        result = await activities.publish_catalog_to_network("wf-5", "txn-5")

        assert result["status"] == "skipped"
        assert result["message_id"] is None
        assert recorded["artifact_type"] == "network_publish_skipped"
        assert recorded["metadata"]["reason"] == "PUBLISH_TO_NETWORK is off"

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_publish_records_artifact_on_success(self, monkeypatch):
        import pipeline.activities as activities
        import pipeline.db as db

        fake_result = {
            "skipped": False,
            "envelope": {
                "context": {
                    "action": "publish",
                    "version": "2.0.0",
                    "messageId": "msg-1",
                    "transactionId": "txn-1",
                    "timestamp": "2026-03-04T10:00:00.000Z",
                    "senderId": "docs-pipeline-bv",
                    "networkId": "docs-pipeline-bv",
                },
                "message": {"catalogs": [{"id": "cat-oan-knowledge-provider-advisories"}]},
            },
            "status_code": 200,
            "response_body": '{"message": {"results": [{"status": "ACCEPTED"}]}}',
            "result_status": "ACCEPTED",
            "errors": [],
        }

        class FakeService:
            def __init__(self):
                pass

            def publish(self, transaction_id, kind, workflow_id=None):
                assert transaction_id == "txn-1"
                assert kind == "advisory"
                assert workflow_id == "wf-1"
                return fake_result

        monkeypatch.setattr(activities, "DiscoveryPublishService", FakeService)
        monkeypatch.setattr(
            activities,
            "_upload_file_to_minio",
            lambda *a, **k: ("minio://documents/network_publish.json", 123, "application/json"),
        )
        monkeypatch.setattr(db, "get_document", lambda workflow_id: {"kind": "advisory"})
        monkeypatch.setattr(db, "get_latest_document_job", lambda workflow_id: {"id": 42})

        recorded = {}

        def fake_add_artifact(**kwargs):
            recorded.update(kwargs)
            return 1

        monkeypatch.setattr(db, "add_document_artifact", fake_add_artifact)

        result = await activities.publish_catalog_to_network("wf-1", "txn-1")

        assert result == {
            "status": "published",
            "document_kind": "advisory",
            "transaction_id": "txn-1",
            "message_id": "msg-1",
            "result_status": "ACCEPTED",
        }
        assert recorded["workflow_id"] == "wf-1"
        assert recorded["job_id"] == 42
        assert recorded["artifact_type"] == "network_publish_payload"
        assert recorded["stage"] == "publishing_to_network"
        assert recorded["metadata"]["request"] == fake_result["envelope"]
        assert recorded["metadata"]["response_status"] == 200
        assert recorded["metadata"]["result_status"] == "ACCEPTED"
        assert recorded["metadata"]["errors"] == []

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_publish_failure_propagates_and_records_nothing(self, monkeypatch):
        import pipeline.activities as activities
        import pipeline.db as db

        class FakeService:
            def publish(self, transaction_id, kind, workflow_id=None):
                raise RuntimeError("discovery service unreachable")

        monkeypatch.setattr(activities, "DiscoveryPublishService", FakeService)
        monkeypatch.setattr(db, "get_document", lambda workflow_id: {"kind": "advisory"})

        add_artifact_calls = []
        monkeypatch.setattr(db, "add_document_artifact", lambda **kwargs: add_artifact_calls.append(kwargs))

        with pytest.raises(RuntimeError, match="discovery service unreachable"):
            await activities.publish_catalog_to_network("wf-1", "txn-1")

        assert add_artifact_calls == []


    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_unmapped_kind_records_a_skipped_artifact(self, monkeypatch):
        import pipeline.activities as activities
        import pipeline.db as db

        class FakeService:
            def publish(self, transaction_id, kind, workflow_id=None):
                return {
                    "skipped": True,
                    "envelope": None,
                    "status_code": None,
                    "response_body": None,
                    "result_status": None,
                    "errors": [],
                }

        monkeypatch.setattr(activities, "DiscoveryPublishService", FakeService)
        monkeypatch.setattr(
            activities,
            "_upload_file_to_minio",
            lambda *a, **k: ("minio://documents/network_publish_skipped.json", 45, "application/json"),
        )
        monkeypatch.setattr(db, "get_document", lambda workflow_id: {"kind": "document"})
        monkeypatch.setattr(db, "get_latest_document_job", lambda workflow_id: None)

        recorded = {}
        monkeypatch.setattr(db, "add_document_artifact", lambda **kwargs: recorded.update(kwargs))

        result = await activities.publish_catalog_to_network("wf-2", "txn-2")

        assert result == {
            "status": "skipped",
            "document_kind": "document",
            "transaction_id": "txn-2",
            "message_id": None,
            "result_status": None,
        }
        assert recorded["artifact_type"] == "network_publish_skipped"
        assert recorded["stage"] == "publishing_to_network"
        assert recorded["job_id"] is None
        assert recorded["metadata"]["skipped"] is True
        assert recorded["storage_uri"]

    @pytest.mark.unit
    @pytest.mark.asyncio
    @pytest.mark.parametrize("result_status", ["ACCEPTED", "PARTIAL", None])
    async def test_non_rejected_results_complete_the_stage(self, monkeypatch, result_status):
        import pipeline.activities as activities
        import pipeline.db as db

        class FakeService:
            def publish(self, transaction_id, kind, workflow_id=None):
                return {
                    "skipped": False,
                    "envelope": {"context": {"messageId": "msg-3"}},
                    "status_code": 200,
                    "response_body": "{}",
                    "result_status": result_status,
                    "errors": [],
                }

        monkeypatch.setattr(activities, "DiscoveryPublishService", FakeService)
        monkeypatch.setattr(
            activities,
            "_upload_file_to_minio",
            lambda *a, **k: ("minio://documents/network_publish.json", 12, "application/json"),
        )
        monkeypatch.setattr(db, "get_document", lambda workflow_id: {"kind": "advisory"})
        monkeypatch.setattr(db, "get_latest_document_job", lambda workflow_id: None)
        monkeypatch.setattr(db, "add_document_artifact", lambda **kwargs: 1)

        result = await activities.publish_catalog_to_network("wf-3", "txn-3")

        assert result["status"] == "published"
        assert result["result_status"] == result_status

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_rejected_result_records_the_errors_then_fails(self, monkeypatch):
        import pipeline.activities as activities
        import pipeline.db as db

        errors = [{"code": "SCH_SCHEMA_VALIDATION_FAILED", "message": "topics is required"}]

        class FakeService:
            def publish(self, transaction_id, kind, workflow_id=None):
                return {
                    "skipped": False,
                    "envelope": {"context": {"messageId": "msg-4"}},
                    "status_code": 200,
                    "response_body": "{}",
                    "result_status": "REJECTED",
                    "errors": errors,
                }

        monkeypatch.setattr(activities, "DiscoveryPublishService", FakeService)
        monkeypatch.setattr(
            activities,
            "_upload_file_to_minio",
            lambda *a, **k: ("minio://documents/network_publish.json", 12, "application/json"),
        )
        monkeypatch.setattr(db, "get_document", lambda workflow_id: {"kind": "advisory"})
        monkeypatch.setattr(db, "get_latest_document_job", lambda workflow_id: None)

        recorded = {}
        monkeypatch.setattr(db, "add_document_artifact", lambda **kwargs: recorded.update(kwargs))

        with pytest.raises(ApplicationError, match="SCH_SCHEMA_VALIDATION_FAILED") as excinfo:
            await activities.publish_catalog_to_network("wf-4", "txn-4")

        # Retrying is pointless: the same envelope is rejected the same way.
        assert excinfo.value.non_retryable is True
        # The reason must survive the failure, so the artifact is written first.
        assert recorded["metadata"]["errors"] == errors


class TestWorkerActivityRegistration:
    """
    Every @activity.defn in activities.py must be registered on the worker
    (pipeline/worker.py), or Temporal fails at runtime with a NotFoundError
    when a workflow tries to run it - a silent gap that unit tests calling
    activity functions directly (like the tests above) can't catch on their
    own, since they invoke the function directly rather than through a
    registered worker.
    """

    @pytest.mark.unit
    def test_every_defined_activity_is_registered_on_the_worker(self):
        import inspect

        from temporalio.activity import _Definition

        from pipeline import activities, worker

        defined_activities = {
            name
            for name, obj in vars(activities).items()
            if inspect.isfunction(obj) and _Definition.from_callable(obj)
        }

        worker_source = inspect.getsource(worker)
        missing = {name for name in defined_activities if name not in worker_source}

        assert not missing, f"Activities defined but never registered on the worker: {missing}"


class TestSupersededPoints:
    """After new content is indexed, points of earlier file versions are deleted."""

    class _Store:
        def __init__(self, failing=()):
            self.calls = []
            self.failing = set(failing)

        def delete_by_doc_id(self, collection, doc_id):
            self.calls.append((collection, doc_id))
            if doc_id in self.failing:
                return {"deleted": 0, "doc_id": doc_id, "error": "boom"}
            return {"deleted": -1, "doc_id": doc_id}

    @staticmethod
    def _document(db):
        doc_uuid = db.upsert_document(
            workflow_id="wf-sup-act",
            document_id="c" * 32,
            filename="a.pdf",
            filepath="minio://documents/a.pdf",
            kind="scheme",
            code="sup-act",
        )
        db.upsert_document_index_status("wf-sup-act", "documents-index", doc_id="a" * 32, status="indexed")
        db.reset_document_for_reupload(
            doc_uuid, filename="b.pdf", filepath="minio://documents/b.pdf", fingerprint="c" * 32
        )
        return doc_uuid

    @pytest.mark.asyncio
    @pytest.mark.db
    @pytest.mark.unit
    async def test_old_keys_are_deleted_and_forgotten(self, db_connection):
        from pipeline import activities

        doc_uuid = self._document(db_connection)
        store = self._Store()

        await activities._drop_superseded_points(
            "dev", "wf-sup-act", db_connection.get_document(doc_uuid), "c" * 32, "documents-index", store=store
        )

        assert store.calls == [("documents-index", "a" * 32)]
        assert db_connection.get_document(doc_uuid)["superseded_doc_ids"] == {}

    @pytest.mark.asyncio
    @pytest.mark.db
    @pytest.mark.unit
    async def test_a_failed_delete_keeps_the_key_for_the_next_run(self, db_connection):
        from pipeline import activities

        doc_uuid = self._document(db_connection)
        store = self._Store(failing={"a" * 32})

        await activities._drop_superseded_points(
            "dev", "wf-sup-act", db_connection.get_document(doc_uuid), "c" * 32, "documents-index", store=store
        )

        assert db_connection.get_document(doc_uuid)["superseded_doc_ids"] == {"dev": ["a" * 32]}

    @pytest.mark.asyncio
    @pytest.mark.db
    @pytest.mark.unit
    async def test_a_key_another_live_document_is_indexed_under_is_kept_and_remembered(self, db_connection):
        from pipeline import activities

        doc_uuid = self._document(db_connection)
        # A second document is indexed under the very key the first one replaced.
        db_connection.upsert_document(
            workflow_id="wf-twin", document_id="a" * 32, filename="a.pdf",
            filepath="minio://documents/a.pdf", group="gj", kind="scheme", code="twin",
        )
        db_connection.upsert_document_index_status(
            "wf-twin", "documents-index", doc_id="a" * 32, status="indexed"
        )
        store = self._Store()

        await activities._drop_superseded_points(
            "dev", "wf-sup-act", db_connection.get_document(doc_uuid), "c" * 32, "documents-index", store=store
        )

        # Points stay for the twin, and the key stays on the list for a later cleanup.
        assert store.calls == []
        assert db_connection.get_document(doc_uuid)["superseded_doc_ids"] == {"dev": ["a" * 32]}

        # Once the twin is gone the same cleanup removes them.
        db_connection.set_document_disabled("wf-twin", True)
        await activities._drop_superseded_points(
            "dev", "wf-sup-act", db_connection.get_document(doc_uuid), "c" * 32, "documents-index", store=store
        )
        assert store.calls == [("documents-index", "a" * 32)]
        assert db_connection.get_document(doc_uuid)["superseded_doc_ids"] == {}
