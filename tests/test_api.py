"""
Unit tests for pipeline/api.py - FastAPI endpoints.

Tests cover:
- Health endpoints
- Document listing and retrieval
- Page/chunk operations
- Approval workflows
- Settings endpoints
- Error handling
"""

import os

import pytest

# Set test environment
os.environ["DOCUMENT_DB_PATH"] = ":memory:"
os.environ["MINIO_ACCESS_KEY"] = "test-key"
os.environ["MINIO_SECRET_KEY"] = "test-secret"
os.environ["ALLOWED_FILE_PATHS"] = "/app/books,/tmp"


class TestHealthEndpoints:
    """Tests for health check endpoints."""

    @pytest.mark.api
    @pytest.mark.unit
    def test_health_endpoint(self, test_client):
        """Test health check returns ok status."""
        response = test_client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "ok"

    @pytest.mark.api
    @pytest.mark.unit
    def test_pipeline_stages_endpoint(self, test_client):
        """Test pipeline stages endpoint returns stage definitions."""
        response = test_client.get("/pipeline/stages")
        assert response.status_code == 200
        stages = response.json()
        assert len(stages) > 0
        assert all("id" in s and "label" in s for s in stages)

    @pytest.mark.api
    @pytest.mark.unit
    def test_pipeline_stages_network_publish_follows_toggle(self, test_client, monkeypatch):
        """publishing_to_network is listed only when PUBLISH_TO_NETWORK is on."""
        monkeypatch.delenv("DISABLE_PROD_SETTING", raising=False)

        monkeypatch.delenv("PUBLISH_TO_NETWORK", raising=False)
        off = {s["id"] for s in test_client.get("/pipeline/stages").json()}
        assert "publishing_to_network" not in off
        assert "ingesting_prod" in off

        monkeypatch.setenv("PUBLISH_TO_NETWORK", "true")
        on = {s["id"] for s in test_client.get("/pipeline/stages").json()}
        assert "publishing_to_network" in on


class TestDocumentEndpoints:
    """Tests for document CRUD endpoints."""

    @pytest.mark.api
    @pytest.mark.unit
    def test_list_documents(self, test_client, db_connection):
        """Test listing documents."""
        # Create a test document
        db_connection.upsert_document(
            workflow_id="api-test-001",
            document_id="doc-001",
            filename="test.pdf",
            filepath="/app/books/test.pdf",
            stage="completed"
        )

        response = test_client.get("/documents")
        assert response.status_code == 200
        docs = response.json()
        assert isinstance(docs, list)

    @pytest.mark.api
    @pytest.mark.unit
    def test_list_documents_with_stage_filter(self, test_client, db_connection):
        """Test filtering documents by stage."""
        db_connection.upsert_document(
            workflow_id="filter-api-001",
            document_id="doc-filter-001",
            filename="completed.pdf",
            filepath="/app/books/completed.pdf",
            stage="completed"
        )

        response = test_client.get("/documents?stage=completed")
        assert response.status_code == 200
        docs = response.json()
        for doc in docs:
            assert doc["stage"] == "completed"

    @pytest.mark.api
    @pytest.mark.unit
    def test_get_document_from_db(self, test_client, db_connection):
        """Test getting document details from SQLite fallback."""
        workflow_id = "get-test-001"
        db_connection.upsert_document(
            workflow_id=workflow_id,
            document_id="doc-get-001",
            filename="test.pdf",
            filepath="/app/books/test.pdf",
            stage="completed"
        )

        response = test_client.get(f"/documents/{workflow_id}")
        assert response.status_code == 200
        doc = response.json()
        assert doc["workflow_id"] == workflow_id

    @pytest.mark.api
    @pytest.mark.unit
    def test_get_document_not_found(self, test_client):
        """Test 404 for non-existent document."""
        response = test_client.get("/documents/nonexistent-workflow")
        assert response.status_code == 404


class TestProdApproval:
    """Promoting to PROD takes no body.

    It used to carry an announcement window; these pin that the gate still
    works without one and that no validity leaks back into the contract.
    """

    def _document_at_the_gate(self, db_connection, workflow_id):
        db_connection.upsert_document(
            workflow_id=workflow_id,
            document_id=f"doc-{workflow_id}",
            filename="test.pdf",
            filepath="/app/books/test.pdf",
            stage="approval_for_prod",
        )

    @pytest.mark.api
    @pytest.mark.unit
    def test_a_body_less_approval_is_accepted(self, test_client, db_connection):
        workflow_id = "prod-approval-001"
        self._document_at_the_gate(db_connection, workflow_id)

        response = test_client.post(f"/documents/{workflow_id}/approve-prod")

        assert response.status_code == 200
        assert response.json()["approved"] == "prod"
        assert response.json()["next_stage"] == "ingesting_prod"

    @pytest.mark.api
    @pytest.mark.unit
    def test_the_response_carries_no_validity(self, test_client, db_connection):
        workflow_id = "prod-approval-002"
        self._document_at_the_gate(db_connection, workflow_id)

        body = test_client.post(f"/documents/{workflow_id}/approve-prod").json()

        assert not [key for key in body if "valid" in key]

    @pytest.mark.api
    @pytest.mark.unit
    def test_an_unexpected_body_does_not_break_the_gate(self, test_client, db_connection):
        # A caller still sending the old window must not get a 422 — the
        # endpoint simply has no body to bind any more.
        workflow_id = "prod-approval-003"
        self._document_at_the_gate(db_connection, workflow_id)

        response = test_client.post(
            f"/documents/{workflow_id}/approve-prod",
            json={"network_valid_from": "2026-09-22", "network_valid_to": "2099-12-31"},
        )

        assert response.status_code == 200

    @pytest.mark.api
    @pytest.mark.unit
    def test_the_document_carries_no_validity_window(self, test_client, db_connection):
        workflow_id = "prod-approval-004"
        self._document_at_the_gate(db_connection, workflow_id)
        test_client.post(f"/documents/{workflow_id}/approve-prod")

        doc = test_client.get(f"/documents/{workflow_id}").json()

        assert "network_valid_from" not in doc
        assert "network_valid_to" not in doc
        # Document Validity is a different thing and is still reported.
        assert "valid_from" in doc

    @pytest.mark.api
    @pytest.mark.unit
    def test_the_wrong_stage_is_still_refused(self, test_client, db_connection):
        workflow_id = "prod-approval-005"
        db_connection.upsert_document(
            workflow_id=workflow_id,
            document_id=f"doc-{workflow_id}",
            filename="test.pdf",
            filepath="/app/books/test.pdf",
            stage="chunk_review",
        )

        response = test_client.post(f"/documents/{workflow_id}/approve-prod")

        assert response.status_code == 400
        assert "chunk_review" in response.json()["detail"]


class TestDocumentValidityEndpoints:
    """The validity period a reviewer sets alongside the document type.

    The period is stamped on upload so the form is never blank, and the PATCH
    that carries the document type carries the reviewer's edit to it.
    """

    def _uploaded_document(self, db_connection, workflow_id):
        db_connection.upsert_document(
            workflow_id=workflow_id,
            document_id=f"doc-{workflow_id}",
            filename="test.pdf",
            filepath="/app/books/test.pdf",
            stage="chunk_review",
        )

    @pytest.mark.api
    @pytest.mark.unit
    def test_upload_starts_today_and_never_expires(self, test_client, sample_pdf_content):
        # The upload endpoint owns this default — db.py stores what it is given
        # and has no opinion on what a period should be — so the guarantee is
        # only real if it is asserted through the endpoint that grants it.
        # Spelled out rather than compared against default_period(), so a
        # change to that default fails here instead of agreeing with itself.
        from datetime import date

        response = test_client.post(
            "/upload",
            files={"file": ("validity-upload.pdf", sample_pdf_content, "application/pdf")},
        )

        assert response.status_code == 200
        doc = test_client.get(f"/documents/{response.json()['workflow_id']}").json()
        assert doc["valid_from"] == date.today().isoformat()
        assert doc["valid_to"] is None

    @pytest.mark.api
    @pytest.mark.unit
    def test_an_end_date_can_be_set_and_then_cleared(self, test_client, db_connection):
        # Clearing is the way back to "never expires". Blank has to be a real
        # answer, not a missing one, or an end date could never be undone.
        workflow_id = "validity-009"
        self._uploaded_document(db_connection, workflow_id)

        set_response = test_client.patch(
            f"/documents/{workflow_id}/scheme-metadata",
            json={"document_kind": "advisory", "valid_from": "2026-01-01", "valid_to": "2026-06-30"},
        )
        assert set_response.json()["valid_to"] == "2026-06-30"

        cleared = test_client.patch(
            f"/documents/{workflow_id}/scheme-metadata",
            json={"document_kind": "advisory", "valid_to": ""},
        )

        assert cleared.status_code == 200
        assert cleared.json()["valid_to"] is None
        assert cleared.json()["valid_from"] == "2026-01-01"
        assert db_connection.get_document(workflow_id)["valid_to"] is None

    @pytest.mark.api
    @pytest.mark.unit
    def test_a_document_stored_without_a_period_reports_none(self, test_client, db_connection):
        # Rows written outside the upload path (scripts, backfills) carry no
        # period, and the API surfaces that honestly rather than inventing one.
        workflow_id = "validity-000"
        self._uploaded_document(db_connection, workflow_id)

        doc = test_client.get(f"/documents/{workflow_id}").json()

        assert doc["valid_from"] is None
        assert doc["valid_to"] is None

    @pytest.mark.api
    @pytest.mark.unit
    def test_stores_the_reviewers_edit(self, test_client, db_connection):
        workflow_id = "validity-002"
        self._uploaded_document(db_connection, workflow_id)

        response = test_client.patch(
            f"/documents/{workflow_id}/scheme-metadata",
            json={
                "document_kind": "advisory",
                "valid_from": "2026-10-01",
                "valid_to": "2026-12-31",
            },
        )

        assert response.status_code == 200
        assert response.json()["valid_from"] == "2026-10-01"
        assert response.json()["valid_to"] == "2026-12-31"
        stored = db_connection.get_document(workflow_id)
        assert stored["valid_from"] == "2026-10-01"
        assert stored["valid_to"] == "2026-12-31"

    @pytest.mark.api
    @pytest.mark.unit
    def test_moving_only_the_end_keeps_the_stored_start(self, test_client, db_connection):
        workflow_id = "validity-003"
        self._uploaded_document(db_connection, workflow_id)
        db_connection.set_document_validity(workflow_id, "2026-01-01", "2026-06-30")

        response = test_client.patch(
            f"/documents/{workflow_id}/scheme-metadata",
            json={"document_kind": "advisory", "valid_to": "2027-06-30"},
        )

        assert response.status_code == 200
        assert response.json()["valid_from"] == "2026-01-01"
        assert response.json()["valid_to"] == "2027-06-30"

    @pytest.mark.api
    @pytest.mark.unit
    def test_classifying_without_dates_leaves_the_period_alone(self, test_client, db_connection):
        workflow_id = "validity-004"
        self._uploaded_document(db_connection, workflow_id)
        db_connection.set_document_validity(workflow_id, "2026-01-01", "2026-06-30")

        test_client.patch(
            f"/documents/{workflow_id}/scheme-metadata",
            json={"document_kind": "advisory"},
        )

        stored = db_connection.get_document(workflow_id)
        assert stored["valid_from"] == "2026-01-01"
        assert stored["valid_to"] == "2026-06-30"

    @pytest.mark.api
    @pytest.mark.unit
    def test_rejects_an_end_before_the_start(self, test_client, db_connection):
        workflow_id = "validity-005"
        self._uploaded_document(db_connection, workflow_id)

        response = test_client.patch(
            f"/documents/{workflow_id}/scheme-metadata",
            json={
                "document_kind": "advisory",
                "valid_from": "2026-12-31",
                "valid_to": "2026-01-01",
            },
        )

        assert response.status_code == 400
        assert "cannot be before" in response.json()["detail"]

    @pytest.mark.api
    @pytest.mark.unit
    def test_rejects_a_malformed_date(self, test_client, db_connection):
        workflow_id = "validity-006"
        self._uploaded_document(db_connection, workflow_id)

        response = test_client.patch(
            f"/documents/{workflow_id}/scheme-metadata",
            json={"document_kind": "advisory", "valid_to": "31-12-2026"},
        )

        assert response.status_code == 400
        assert "YYYY-MM-DD" in response.json()["detail"]

    @pytest.mark.api
    @pytest.mark.unit
    def test_a_rejected_period_stores_nothing(self, test_client, db_connection):
        # A 400 leaves the document as it was, kind included, rather than
        # half-applying the PATCH.
        workflow_id = "validity-007"
        self._uploaded_document(db_connection, workflow_id)
        db_connection.set_document_validity(workflow_id, "2026-01-01", "2026-06-30")

        test_client.patch(
            f"/documents/{workflow_id}/scheme-metadata",
            json={"document_kind": "advisory", "valid_to": "not-a-date"},
        )

        stored = db_connection.get_document(workflow_id)
        assert stored["valid_from"] == "2026-01-01"
        assert stored["valid_to"] == "2026-06-30"
        assert (stored["document_kind"] or "document") == "document"

    @pytest.mark.api
    @pytest.mark.unit
    def test_the_period_is_listed_with_the_document(self, test_client, db_connection):
        workflow_id = "validity-008"
        self._uploaded_document(db_connection, workflow_id)
        db_connection.set_document_validity(workflow_id, "2026-01-01", "2026-06-30")

        listed = test_client.get("/documents").json()
        row = next(d for d in listed if d["workflow_id"] == workflow_id)

        assert row["valid_from"] == "2026-01-01"
        assert row["valid_to"] == "2026-06-30"


class TestSearchValidity:
    """Search must not answer from a document outside its validity period."""

    @pytest.mark.api
    @pytest.mark.unit
    def test_applies_validity_by_default(self, test_client, monkeypatch):
        from datetime import date

        captured = {}

        class FakeStore:
            backend = "qdrant"

            def search(self, **kwargs):
                captured.update(kwargs)
                return {"hits": [], "valid_on": kwargs.get("valid_on") or date.today().isoformat()}

        monkeypatch.setattr(
            "pipeline.vector_store.get_vector_store", lambda: FakeStore()
        )

        response = test_client.post("/search", json={"query": "kisan"})

        assert response.status_code == 200
        assert captured["apply_validity"] is True
        config = response.json()["effective_config"]
        assert config["apply_validity"] is True
        assert config["valid_on"] == date.today().isoformat()

    @pytest.mark.api
    @pytest.mark.unit
    def test_an_operator_can_ask_about_another_day(self, test_client, monkeypatch):
        captured = {}

        class FakeStore:
            backend = "qdrant"

            def search(self, **kwargs):
                captured.update(kwargs)
                return {"hits": [], "valid_on": kwargs.get("valid_on")}

        monkeypatch.setattr(
            "pipeline.vector_store.get_vector_store", lambda: FakeStore()
        )

        response = test_client.post(
            "/search", json={"query": "kisan", "valid_on": "2027-01-01"}
        )

        assert response.status_code == 200
        assert captured["valid_on"] == "2027-01-01"
        assert response.json()["effective_config"]["valid_on"] == "2027-01-01"

    @pytest.mark.api
    @pytest.mark.unit
    def test_an_operator_can_include_expired_documents(self, test_client, monkeypatch):
        captured = {}

        class FakeStore:
            backend = "qdrant"

            def search(self, **kwargs):
                captured.update(kwargs)
                return {"hits": [], "valid_on": None}

        monkeypatch.setattr(
            "pipeline.vector_store.get_vector_store", lambda: FakeStore()
        )

        response = test_client.post(
            "/search", json={"query": "kisan", "include_expired": True}
        )

        assert response.status_code == 200
        assert captured["apply_validity"] is False
        assert response.json()["effective_config"]["apply_validity"] is False

    @pytest.mark.api
    @pytest.mark.unit
    def test_normalises_valid_on_before_it_reaches_the_store(self, test_client, monkeypatch):
        # strptime accepts "2026-9-3" but the vector store's date range does
        # not, so validating without using the parsed value turned a bad
        # request into a "Vector search failed" 400 blaming the store.
        captured = {}

        class FakeStore:
            backend = "qdrant"

            def search(self, **kwargs):
                captured.update(kwargs)
                return {"hits": [], "valid_on": kwargs.get("valid_on")}

        monkeypatch.setattr(
            "pipeline.vector_store.get_vector_store", lambda: FakeStore()
        )

        response = test_client.post(
            "/search", json={"query": "kisan", "valid_on": "2026-9-3"}
        )

        assert response.status_code == 200
        assert captured["valid_on"] == "2026-09-03"
        assert response.json()["effective_config"]["valid_on"] == "2026-09-03"

    @pytest.mark.api
    @pytest.mark.unit
    def test_rejects_a_malformed_valid_on(self, test_client):
        response = test_client.post(
            "/search", json={"query": "kisan", "valid_on": "01-01-2027"}
        )

        assert response.status_code == 400
        assert "YYYY-MM-DD" in response.json()["detail"]


class TestPageEndpoints:
    """Tests for page operations."""

    @pytest.mark.api
    @pytest.mark.unit
    def test_list_pages(self, test_client, db_connection):
        """Test listing pages for a document."""
        workflow_id = "pages-test-001"
        db_connection.upsert_document(
            workflow_id=workflow_id,
            document_id="doc-pages",
            filename="test.pdf",
            filepath="/app/books/test.pdf",
            stage="completed"
        )
        db_connection.persist_document_content(
            workflow_id=workflow_id,
            pages=[
                {"page_number": 1, "original_markdown": "Page 1", "detected_language": "en"},
                {"page_number": 2, "original_markdown": "Page 2", "detected_language": "en"}
            ],
            chunks=[]
        )

        response = test_client.get(f"/documents/{workflow_id}/pages")
        assert response.status_code == 200
        pages = response.json()
        assert len(pages) == 2

    @pytest.mark.api
    @pytest.mark.unit
    def test_get_page(self, test_client, db_connection):
        """Test getting a specific page."""
        workflow_id = "page-get-001"
        db_connection.upsert_document(
            workflow_id=workflow_id,
            document_id="doc-page",
            filename="test.pdf",
            filepath="/app/books/test.pdf",
            stage="completed"
        )
        db_connection.persist_document_content(
            workflow_id=workflow_id,
            pages=[{"page_number": 1, "original_markdown": "Content", "detected_language": "en"}],
            chunks=[]
        )

        response = test_client.get(f"/documents/{workflow_id}/pages/1")
        assert response.status_code == 200
        page = response.json()
        assert page["page_number"] == 1

    @pytest.mark.api
    @pytest.mark.unit
    def test_get_page_invalid_number(self, test_client, db_connection):
        """Test validation rejects invalid page numbers."""
        workflow_id = "page-invalid-001"
        db_connection.upsert_document(
            workflow_id=workflow_id,
            document_id="doc-invalid",
            filename="test.pdf",
            filepath="/app/books/test.pdf",
            stage="completed"
        )

        # Page 0 should be invalid (pages are 1-indexed)
        response = test_client.get(f"/documents/{workflow_id}/pages/0")
        assert response.status_code == 422  # Validation error

        # Negative page number
        response = test_client.get(f"/documents/{workflow_id}/pages/-1")
        assert response.status_code == 422

    @pytest.mark.api
    @pytest.mark.unit
    def test_update_page(self, test_client, db_connection):
        """Test updating a page."""
        workflow_id = "page-update-001"
        db_connection.upsert_document(
            workflow_id=workflow_id,
            document_id="doc-update",
            filename="test.pdf",
            filepath="/app/books/test.pdf",
            stage="completed"
        )
        db_connection.persist_document_content(
            workflow_id=workflow_id,
            pages=[{"page_number": 1, "original_markdown": "Original", "detected_language": "en"}],
            chunks=[]
        )

        response = test_client.patch(
            f"/documents/{workflow_id}/pages/1",
            json={"edited_markdown": "Updated content"}
        )
        assert response.status_code == 200
        page = response.json()
        assert page["edited_markdown"] == "Updated content"


class TestChunkEndpoints:
    """Tests for chunk operations."""

    @pytest.mark.api
    @pytest.mark.unit
    def test_list_chunks(self, test_client, db_connection):
        """Test listing chunks for a document."""
        workflow_id = "chunks-test-001"
        db_connection.upsert_document(
            workflow_id=workflow_id,
            document_id="doc-chunks",
            filename="test.pdf",
            filepath="/app/books/test.pdf",
            stage="completed"
        )
        db_connection.persist_document_content(
            workflow_id=workflow_id,
            pages=[],
            chunks=[
                {"chunk_number": 1, "original_text": "Chunk 1", "source_pages": [1], "token_count": 5},
                {"chunk_number": 2, "original_text": "Chunk 2", "source_pages": [1], "token_count": 5}
            ]
        )

        response = test_client.get(f"/documents/{workflow_id}/chunks")
        assert response.status_code == 200
        chunks = response.json()
        assert len(chunks) == 2

    @pytest.mark.api
    @pytest.mark.unit
    def test_list_chunks_include_excluded(self, test_client, db_connection):
        """Test including excluded chunks in listing."""
        workflow_id = "chunks-excluded-001"
        db_connection.upsert_document(
            workflow_id=workflow_id,
            document_id="doc-excluded",
            filename="test.pdf",
            filepath="/app/books/test.pdf",
            stage="completed"
        )
        db_connection.persist_document_content(
            workflow_id=workflow_id,
            pages=[],
            chunks=[
                {"chunk_number": 1, "original_text": "Included", "source_pages": [1], "token_count": 5},
                {"chunk_number": 2, "original_text": "Excluded", "source_pages": [1], "token_count": 5}
            ]
        )
        db_connection.update_chunk(workflow_id, 2, is_excluded=True)

        # Without flag - should exclude
        response = test_client.get(f"/documents/{workflow_id}/chunks")
        assert len(response.json()) == 1

        # With flag - should include
        response = test_client.get(f"/documents/{workflow_id}/chunks?include_excluded=true")
        assert len(response.json()) == 2

    @pytest.mark.api
    @pytest.mark.unit
    def test_get_chunk_invalid_number(self, test_client, db_connection):
        """Test validation rejects invalid chunk numbers."""
        workflow_id = "chunk-invalid-001"
        db_connection.upsert_document(
            workflow_id=workflow_id,
            document_id="doc-chunk-invalid",
            filename="test.pdf",
            filepath="/app/books/test.pdf",
            stage="completed"
        )

        response = test_client.get(f"/documents/{workflow_id}/chunks/0")
        assert response.status_code == 422


class TestAuditEndpoints:
    """Tests for audit log endpoints."""

    @pytest.mark.api
    @pytest.mark.unit
    def test_get_document_audit(self, test_client, db_connection):
        """Test getting audit logs for a document."""
        workflow_id = "audit-test-001"
        db_connection.upsert_document(
            workflow_id=workflow_id,
            document_id="doc-audit",
            filename="test.pdf",
            filepath="/app/books/test.pdf",
            stage="registered"
        )
        db_connection.log_audit(
            workflow_id=workflow_id,
            document_id="doc-audit",
            action_type="test_action"
        )

        response = test_client.get(f"/documents/{workflow_id}/audit")
        assert response.status_code == 200
        data = response.json()
        assert "logs" in data
        assert "total" in data

    @pytest.mark.api
    @pytest.mark.unit
    def test_get_global_audit(self, test_client, db_connection):
        """Test getting global audit logs."""
        response = test_client.get("/audit")
        assert response.status_code == 200
        data = response.json()
        assert "logs" in data
        assert "total" in data

    @pytest.mark.api
    @pytest.mark.unit
    def test_audit_pagination(self, test_client, db_connection):
        """Test audit log pagination."""
        response = test_client.get("/audit?limit=5&offset=0")
        assert response.status_code == 200
        data = response.json()
        assert data["limit"] == 5
        assert data["offset"] == 0


class TestSettingsEndpoints:
    """Tests for settings endpoints."""

    @pytest.mark.api
    @pytest.mark.unit
    def test_get_search_settings(self, test_client, db_connection):
        """Test getting search settings."""
        response = test_client.get("/settings/search")
        assert response.status_code == 200
        settings = response.json()
        assert "searchMethod" in settings
        assert "limit" in settings

    @pytest.mark.api
    @pytest.mark.unit
    def test_update_search_settings(self, test_client, db_connection):
        """Test updating search settings."""
        response = test_client.put(
            "/settings/search",
            json={"limit": 25}
        )
        assert response.status_code == 200
        settings = response.json()
        assert settings["limit"] == 25

    @pytest.mark.api
    @pytest.mark.unit
    def test_get_settings_audit(self, test_client, db_connection):
        """Test getting settings audit trail."""
        # Make a change first
        test_client.put("/settings/search", json={"limit": 30})

        response = test_client.get("/settings/search/audit")
        assert response.status_code == 200
        data = response.json()
        assert "logs" in data


class TestErrorHandling:
    """Tests for error handling."""

    @pytest.mark.api
    @pytest.mark.unit
    def test_document_not_found(self, test_client):
        """Test 404 response for missing document."""
        response = test_client.get("/documents/nonexistent")
        assert response.status_code == 404
        assert "not found" in response.json()["detail"].lower()

    @pytest.mark.api
    @pytest.mark.unit
    def test_page_not_found(self, test_client, db_connection):
        """Test 404 for missing page."""
        workflow_id = "page-404-test"
        db_connection.upsert_document(
            workflow_id=workflow_id,
            document_id="doc-404",
            filename="test.pdf",
            filepath="/app/books/test.pdf",
            stage="completed"
        )
        # No pages persisted

        response = test_client.get(f"/documents/{workflow_id}/pages/999")
        assert response.status_code == 404


class TestPdfHeaders:
    @pytest.mark.unit
    def test_inline_content_disposition_unicode_filename(self):
        from pipeline.api import _inline_content_disposition

        header = _inline_content_disposition("રબર મેટ.pdf")
        header.encode("latin-1")
        assert "filename*=" in header
        assert header.startswith('inline; filename="')

    @pytest.mark.unit
    def test_inline_content_disposition_strips_crlf(self):
        from pipeline.api import _inline_content_disposition

        header = _inline_content_disposition("evil.pdf\r\nX-Injected: yes")
        assert "\r" not in header
        assert "\n" not in header
        # Remains a single Content-Disposition value (no injected header line).
        assert header.count(":") >= 1
        assert "\r\nX-Injected" not in header
        assert header.startswith('inline; filename="')
