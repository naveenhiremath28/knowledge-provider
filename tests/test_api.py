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

import hashlib
import itertools
import os

import pytest

# Set test environment
os.environ["MINIO_ACCESS_KEY"] = "test-key"
os.environ["MINIO_SECRET_KEY"] = "test-secret"
os.environ["ALLOWED_FILE_PATHS"] = "/app/books,/tmp"


_code_seq = itertools.count(1)


def _next_code() -> str:
    """A unique document code for fixtures (documents need one in v2)."""
    return f"doc-{next(_code_seq)}"


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
            kind="scheme",
            code=_next_code(),
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
            kind="scheme",
            code=_next_code(),
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
        """Test getting document details from the database."""
        workflow_id = "get-test-001"
        db_connection.upsert_document(
            kind="scheme",
            code=_next_code(),
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


class TestDocumentIdentifiers:
    """`/documents/{id}` takes the UUID (canonical) or any legacy identifier."""

    FINGERPRINT = hashlib.md5(b"identifiers").hexdigest()
    WORKFLOW_ID = f"doc-{FINGERPRINT}"

    def _document(self, db_connection):
        db_connection.upsert_document(
            kind="scheme",
            code=_next_code(),
            workflow_id=self.WORKFLOW_ID,
            document_id=self.FINGERPRINT,
            filename="identifiers.pdf",
            filepath="minio://documents/identifiers.pdf",
            stage="ocr_review",
        )
        db_connection.save_pages(
            self.WORKFLOW_ID, [{"page_number": 1, "original_markdown": "page one"}]
        )
        return db_connection.get_document(self.WORKFLOW_ID)

    @pytest.mark.api
    @pytest.mark.unit
    def test_the_response_carries_the_uuid_and_the_temporal_id(self, test_client, db_connection):
        doc = self._document(db_connection)
        body = test_client.get(f"/documents/{doc['id']}").json()
        assert body["id"] == doc["id"]
        assert body["workflow_id"] == self.WORKFLOW_ID
        assert body["document_id"] == self.FINGERPRINT

    @pytest.mark.api
    @pytest.mark.unit
    @pytest.mark.parametrize("form", ["uuid", "workflow", "fingerprint", "filename"])
    def test_every_identifier_form_reaches_the_same_document(self, test_client, db_connection, form):
        doc = self._document(db_connection)
        identifier = {
            "uuid": doc["id"],
            "workflow": self.WORKFLOW_ID,
            "fingerprint": self.FINGERPRINT,
            "filename": "identifiers.pdf",
        }[form]
        detail = test_client.get(f"/documents/{identifier}")
        pages = test_client.get(f"/documents/{identifier}/pages")
        assert detail.status_code == 200 and pages.status_code == 200
        assert detail.json()["id"] == doc["id"]
        assert [p["page_number"] for p in pages.json()] == [1]

    @pytest.mark.api
    @pytest.mark.unit
    def test_the_uuid_is_rewritten_to_the_temporal_id_before_handlers_run(
        self, test_client, db_connection, mock_temporal_client
    ):
        doc = self._document(db_connection)
        db_connection.update_document_stage(doc["id"], "approval_for_prod")
        response = test_client.post(f"/documents/{doc['id']}/approve-prod")
        assert response.status_code == 200
        queried = [call.args[0] for call in mock_temporal_client.get_workflow_handle.call_args_list]
        assert queried and set(queried) == {self.WORKFLOW_ID}

    @pytest.mark.api
    @pytest.mark.unit
    def test_static_document_routes_are_not_mistaken_for_identifiers(self, test_client, db_connection):
        db_connection.upsert_document(
            kind="scheme",
            code=_next_code(),
            workflow_id="summary", document_id=hashlib.md5(b"summary").hexdigest(),
            filename="summary", filepath="/tmp/s.pdf",
        )
        response = test_client.get("/documents/summary")
        assert response.status_code == 200
        assert "total_documents" in response.json()

    @pytest.mark.api
    @pytest.mark.unit
    def test_an_unknown_identifier_is_404(self, test_client, db_connection):
        assert test_client.get("/documents/00000000-0000-0000-0000-000000000000").status_code == 404


class TestArtifactAndJobIdentifiers:
    """Artifact and job ids are UUIDs."""

    def _document(self, db_connection):
        db_connection.upsert_document(
            kind="scheme",
            code=_next_code(),
            workflow_id="wf-art-api", document_id=hashlib.md5(b"art").hexdigest(),
            filename="a.pdf", filepath="minio://documents/a.pdf", stage="ocr_review",
        )
        artifact_id = db_connection.add_document_artifact(
            "wf-art-api", "original_upload", "minio://documents/a.pdf", stage="registered",
            filename="a.pdf",
        )
        return artifact_id

    @pytest.mark.api
    @pytest.mark.unit
    def test_artifacts_are_addressed_by_uuid(self, test_client, db_connection):
        artifact_id = self._document(db_connection)
        listed = test_client.get("/documents/wf-art-api/artifacts").json()
        assert [a["id"] for a in listed] == [artifact_id]
        assert test_client.get(f"/documents/wf-art-api/artifacts/{artifact_id}").json()["id"] == artifact_id
        assert test_client.get("/documents/wf-art-api/artifacts/7").status_code == 404

    @pytest.mark.api
    @pytest.mark.unit
    def test_document_detail_derives_artifact_and_job_pointers(self, test_client, db_connection):
        artifact_id = self._document(db_connection)
        body = test_client.get("/documents/wf-art-api").json()
        job = db_connection.get_latest_document_job("wf-art-api")
        assert body["original_artifact_id"] == artifact_id
        assert body["normalized_artifact_id"] is None
        assert body["latest_job_id"] == job["id"]
        assert body["artifacts"][0]["job_id"] == job["id"]

    @pytest.mark.api
    @pytest.mark.unit
    def test_runs_are_addressed_by_uuid(self, test_client, db_connection):
        self._document(db_connection)
        job = db_connection.get_latest_document_job("wf-art-api")
        assert test_client.get(f"/runs/{job['id']}").json()["id"] == job["id"]
        assert test_client.get("/runs/3").status_code == 404
        assert [r["id"] for r in test_client.get("/runs").json()] == [job["id"]]


class TestHealthAndSettings:
    @pytest.mark.api
    @pytest.mark.unit
    def test_health_reports_the_database(self, test_client):
        body = test_client.get("/health").json()
        assert body["database"] == {"reachable": True, "migrations_current": True}

    @pytest.mark.api
    @pytest.mark.unit
    def test_settings_default_alpha_and_validation(self, test_client):
        assert test_client.get("/settings/search").json()["alpha"] == 0.6
        assert test_client.put("/settings/search", json={"alpha": 2}).status_code == 400
        updated = test_client.put("/settings/search", json={"limit": 7}).json()
        assert updated["limit"] == 7
        reset = test_client.post("/settings/search/reset").json()
        assert reset["limit"] == 12 and reset["alpha"] == 0.6
        audit = test_client.get("/settings/search/audit").json()
        assert audit["total"] == 2 and audit["logs"][0]["document_id"] == "__settings__"


class TestProdApproval:
    """Promoting to PROD takes no body.

    It used to carry an announcement window; these pin that the gate still
    works without one and that no validity leaks back into the contract.
    """

    def _document_at_the_gate(self, db_connection, workflow_id):
        db_connection.upsert_document(
            kind="scheme",
            code=_next_code(),
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
            kind="scheme",
            code=_next_code(),
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
            kind="scheme",
            code=_next_code(),
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
            params={"kind": "scheme"},
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
            json={"kind": "advisory", "valid_from": "2026-01-01", "valid_to": "2026-06-30"},
        )
        assert set_response.json()["valid_to"] == "2026-06-30"

        cleared = test_client.patch(
            f"/documents/{workflow_id}/scheme-metadata",
            json={"kind": "advisory", "valid_to": ""},
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
                "kind": "advisory",
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
            json={"kind": "advisory", "valid_to": "2027-06-30"},
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
            json={"kind": "advisory"},
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
                "kind": "advisory",
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
            json={"kind": "advisory", "valid_to": "31-12-2026"},
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
            json={"kind": "advisory", "valid_to": "not-a-date"},
        )

        stored = db_connection.get_document(workflow_id)
        assert stored["valid_from"] == "2026-01-01"
        assert stored["valid_to"] == "2026-06-30"
        assert stored["kind"] == "scheme"

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
            kind="scheme",
            code=_next_code(),
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
            kind="scheme",
            code=_next_code(),
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
            kind="scheme",
            code=_next_code(),
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
            kind="scheme",
            code=_next_code(),
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
            kind="scheme",
            code=_next_code(),
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
            kind="scheme",
            code=_next_code(),
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
            kind="scheme",
            code=_next_code(),
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
            kind="scheme",
            code=_next_code(),
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
            kind="scheme",
            code=_next_code(),
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


class TestUploadCatalogIdentity:
    """/upload picks the tenant's catalog from ``kind`` and names the document by ``code``."""

    @pytest.fixture(autouse=True)
    def _fresh_rate_limit(self):
        # /upload is limited per IP; these tests post more often than one minute allows.
        import pipeline.api as api

        api.limiter.reset()
        yield
        api.limiter.reset()

    @staticmethod
    def _pdf(sample_pdf_content: bytes, tag: str) -> bytes:
        # A different trailer gives a different fingerprint, hence a new workflow id.
        return sample_pdf_content + f"\n% {tag}".encode()

    @pytest.mark.api
    @pytest.mark.unit
    def test_kind_is_required(self, test_client, sample_pdf_content):
        response = test_client.post(
            "/upload", files={"file": ("a.pdf", sample_pdf_content, "application/pdf")}
        )

        assert response.status_code == 422

    @pytest.mark.api
    @pytest.mark.unit
    @pytest.mark.parametrize("kind", ["document", "video", "", "Scheme-X"])
    def test_an_unknown_kind_is_rejected(self, test_client, sample_pdf_content, kind):
        response = test_client.post(
            "/upload",
            params={"kind": kind},
            files={"file": ("a.pdf", sample_pdf_content, "application/pdf")},
        )

        assert response.status_code == 400
        assert "scheme" in response.json()["detail"]

    @pytest.mark.api
    @pytest.mark.unit
    def test_an_invalid_code_is_rejected(self, test_client, sample_pdf_content):
        response = test_client.post(
            "/upload",
            params={"kind": "scheme", "code": "Not A Slug"},
            files={"file": ("a.pdf", sample_pdf_content, "application/pdf")},
        )

        assert response.status_code == 400

    @pytest.mark.api
    @pytest.mark.unit
    def test_upload_creates_the_catalog_and_derives_the_code_from_the_filename(
        self, test_client, db_connection, sample_pdf_content
    ):
        response = test_client.post(
            "/upload",
            params={"kind": "scheme", "group": "mh"},
            files={"file": ("National Mission on Natural Farming.pdf", sample_pdf_content, "application/pdf")},
        )

        assert response.status_code == 200
        doc = test_client.get(f"/documents/{response.json()['workflow_id']}").json()
        assert doc["kind"] == "scheme"
        assert doc["group"] == "mh"
        assert doc["code"] == "nmnf"
        assert doc["name"] == "National Mission on Natural Farming"
        with db_connection.get_connection() as conn:
            catalog = conn.execute(
                "SELECT c.type, g.name, g.path FROM catalogs c JOIN groups g ON g.id = c.group_id"
            ).fetchall()
        assert [(r["type"], r["name"], r["path"]) for r in catalog] == [
            ("openagrinet:AgricultureResource", "mh", "/states/MH")
        ]

    @pytest.mark.api
    @pytest.mark.unit
    def test_a_second_upload_reuses_the_catalog_and_gets_a_fresh_code(
        self, test_client, db_connection, sample_pdf_content
    ):
        first = test_client.post(
            "/upload",
            params={"kind": "advisory", "group": "mh"},
            files={"file": ("Crop Advisory.pdf", self._pdf(sample_pdf_content, "one"), "application/pdf")},
        )
        second = test_client.post(
            "/upload",
            params={"kind": "advisory", "group": "mh"},
            files={"file": ("Crop Advisory.pdf", self._pdf(sample_pdf_content, "two"), "application/pdf")},
        )

        assert first.status_code == second.status_code == 200
        codes = {
            test_client.get(f"/documents/{r.json()['workflow_id']}").json()["code"]
            for r in (first, second)
        }
        assert codes == {"ca", "ca-2"}
        with db_connection.get_connection() as conn:
            assert conn.execute("SELECT count(*) AS n FROM catalogs").fetchone()["n"] == 1

    @pytest.mark.api
    @pytest.mark.unit
    def test_an_existing_code_updates_the_same_document(
        self, test_client, db_connection, sample_pdf_content
    ):
        first = test_client.post(
            "/upload",
            params={"kind": "scheme", "group": "mh", "code": "pm-kisan"},
            files={"file": ("pmkisan.pdf", self._pdf(sample_pdf_content, "v1"), "application/pdf")},
        )
        assert first.status_code == 200
        before = test_client.get(f"/documents/{first.json()['workflow_id']}").json()
        db_connection.save_pages(
            before["id"], [{"page_number": 1, "original_markdown": "old text"}]
        )
        db_connection.save_chunks(before["id"], [{"chunk_number": 1, "original_text": "old"}])
        # The first upload's run is still "running"; a replace waits for it to settle.
        db_connection.update_document_job(
            db_connection.get_latest_document_job(before["id"])["id"], status="waiting_review"
        )

        second = test_client.post(
            "/upload",
            params={"kind": "scheme", "group": "mh", "code": "pm-kisan"},
            files={"file": ("pmkisan-2026.pdf", self._pdf(sample_pdf_content, "v2"), "application/pdf")},
        )

        assert second.status_code == 200
        assert second.json()["workflow_id"] != first.json()["workflow_id"]
        after = test_client.get(f"/documents/{before['id']}").json()
        assert after["id"] == before["id"]
        assert after["filename"] == "pmkisan-2026.pdf"
        assert after["source_file_fingerprint"] != before["source_file_fingerprint"]
        assert after["reindex_required"] is True
        assert after["reindex_reason"] == "source_replaced"
        assert (after["page_count"], after["chunk_count"]) == (0, 0)
        with db_connection.get_connection() as conn:
            assert conn.execute("SELECT count(*) AS n FROM documents").fetchone()["n"] == 1
            assert conn.execute("SELECT count(*) AS n FROM workflows").fetchone()["n"] == 2

    @pytest.mark.api
    @pytest.mark.unit
    def test_the_same_code_in_another_tenant_is_a_different_document(
        self, test_client, db_connection, sample_pdf_content
    ):
        for group, tag in (("mh", "a"), ("gj", "b")):
            response = test_client.post(
                "/upload",
                params={"kind": "scheme", "group": group, "code": "pm-kisan"},
                files={"file": ("p.pdf", self._pdf(sample_pdf_content, tag), "application/pdf")},
            )
            assert response.status_code == 200

        with db_connection.get_connection() as conn:
            assert conn.execute("SELECT count(*) AS n FROM documents").fetchone()["n"] == 2
            assert conn.execute("SELECT count(*) AS n FROM catalogs").fetchone()["n"] == 2

    @staticmethod
    def _user(*group_paths):
        from pipeline.auth.jwt import claims_to_user

        return claims_to_user({"sub": "u-" + "-".join(group_paths), "groups": list(group_paths)})

    @staticmethod
    def _mh_doc(db, **extra):
        doc_uuid = db.upsert_document(
            workflow_id="wf-mh-doc",
            document_id="d-mh",
            filename="a.pdf",
            filepath="/tmp/a.pdf",
            group="mh",
            kind="scheme",
            code="pm-kisan",
            **extra,
        )
        # A new document starts with a "running" workflow; settle it so the
        # document is at rest, as one waiting in review is.
        db.update_document_job(db.get_latest_document_job(doc_uuid)["id"], status="waiting_review")
        return doc_uuid

    @pytest.mark.api
    @pytest.mark.unit
    def test_replacing_a_document_needs_rights_in_its_group(self, db_connection):
        from fastapi import HTTPException

        import pipeline.api as api

        self._mh_doc(db_connection)
        identity = lambda user: api._resolve_document_identity(  # noqa: E731
            user, "mh", "scheme", "pm-kisan", "a.pdf"
        )

        # Another state, and a view-only member of this one, are both turned away
        # before anything about the document is revealed.
        for outsider in (self._user("/states/GJ/contributor"), self._user("/states/MH/view")):
            with pytest.raises(HTTPException) as exc:
                identity(outsider)
            assert exc.value.status_code == 403

        # The same state with a role that can upload and review may replace.
        kind, code, existing = identity(self._user("/states/MH/contributor"))
        assert (kind, code, existing["workflow_id"]) == ("scheme", "pm-kisan", "wf-mh-doc")

    @pytest.mark.api
    @pytest.mark.unit
    def test_upload_into_a_group_needs_upload_rights_there(self, db_connection):
        from fastapi import HTTPException

        import pipeline.api as api

        mixed = self._user("/states/MH/contributor", "/states/GJ/view")
        # Allowed where the user can upload, refused where they only view.
        api._resolve_document_identity(mixed, "mh", "scheme", "", "new scheme.pdf")
        with pytest.raises(HTTPException) as exc:
            api._resolve_document_identity(mixed, "gj", "scheme", "", "new scheme.pdf")
        assert exc.value.status_code == 403

    @pytest.mark.api
    @pytest.mark.unit
    def test_a_disabled_or_running_document_cannot_be_replaced(self, db_connection):
        from fastapi import HTTPException

        import pipeline.api as api

        db = db_connection
        contributor = self._user("/states/MH/contributor")
        doc_uuid = self._mh_doc(db)

        db.create_document_job(doc_uuid, "pipeline", "wf-mh-doc", status="running")
        with pytest.raises(HTTPException) as exc:
            api._resolve_document_identity(contributor, "mh", "scheme", "pm-kisan", "a.pdf")
        assert (exc.value.status_code, "in progress" in exc.value.detail) == (409, True)

        db.update_document_job(db.get_latest_document_job(doc_uuid)["id"], status="waiting_review")
        db.set_document_disabled("wf-mh-doc", True)
        with pytest.raises(HTTPException) as exc:
            api._resolve_document_identity(contributor, "mh", "scheme", "pm-kisan", "a.pdf")
        assert (exc.value.status_code, "disabled" in exc.value.detail) == (409, True)

    @pytest.mark.api
    @pytest.mark.unit
    def test_a_document_live_in_prod_needs_approval_rights_to_replace(self, db_connection):
        from fastapi import HTTPException

        import pipeline.api as api

        db = db_connection
        self._mh_doc(db)
        db.upsert_document_index_status(
            "wf-mh-doc", "documents-index", doc_id="d-mh", status="indexed"
        )
        db.update_document_fields("wf-mh-doc")  # no-op, keeps the dev entry only
        with db.get_connection() as conn:
            conn.execute(
                "UPDATE documents SET metadata = jsonb_set(metadata, '{index_status,prod}', "
                "'{\"status\": \"indexed\", \"doc_id\": \"d-mh\"}')"
            )

        with pytest.raises(HTTPException) as exc:
            api._resolve_document_identity(
                self._user("/states/MH/contributor"), "mh", "scheme", "pm-kisan", "a.pdf"
            )
        assert exc.value.status_code == 403

        _, _, existing = api._resolve_document_identity(
            self._user("/states/MH/approver"), "mh", "scheme", "pm-kisan", "a.pdf"
        )
        assert existing["workflow_id"] == "wf-mh-doc"

    @pytest.mark.api
    @pytest.mark.unit
    def test_the_demo_and_bootstrap_routes_are_gone(self, test_client):
        assert test_client.post("/documents/anything/demo").status_code in (404, 405)
        assert test_client.post("/catalog/v1/bootstrap").status_code in (404, 405)


class TestVectorKeysToDelete:
    @pytest.mark.api
    @pytest.mark.unit
    def test_disable_never_deletes_a_key_another_document_uses(self, db_connection):
        import pipeline.api as api

        db = db_connection
        uuid_a = db.upsert_document(
            workflow_id="wf-k-a", document_id="a" * 32, filename="a.pdf",
            filepath="minio://documents/a.pdf", group="mh", kind="scheme", code="k-a",
        )
        db.upsert_document(
            workflow_id="wf-k-b", document_id="a" * 32, filename="a.pdf",
            filepath="minio://documents/a.pdf", group="gj", kind="scheme", code="k-b",
        )
        db.upsert_document(
            workflow_id="wf-k-c", document_id="b" * 32, filename="c.pdf",
            filepath="minio://documents/c.pdf", group="mh", kind="scheme", code="k-c",
        )

        assert api._vector_doc_ids(db.get_document(uuid_a), "dev") == []
        assert api._vector_doc_ids(db.get_document("wf-k-c"), "dev") == ["b" * 32]
