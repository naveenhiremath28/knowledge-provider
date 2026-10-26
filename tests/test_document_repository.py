"""Unit tests for DocumentRepository.

Two styles on purpose: the `db_connection` fixture exercises the repository
against real the database (so a drifted db.py signature fails here), and the fake-db
tests pin the None guards that are the repository's own behaviour.
"""

import pytest

from pipeline.document_repository import DocumentRepository


class FakeDb:
    """Records calls and returns canned rows, standing in for pipeline.db."""

    def __init__(self, document=None):
        self._document = document

    def get_document(self, workflow_id):
        self.get_document_called_with = workflow_id
        return self._document


class TestGetDocumentKind:
    @pytest.mark.unit
    def test_returns_the_stored_kind(self):
        repo = DocumentRepository(FakeDb(document={"document_kind": "advisory"}))

        assert repo.get_document_kind("wf-1") == "advisory"

    @pytest.mark.unit
    def test_returns_none_for_a_missing_document(self):
        repo = DocumentRepository(FakeDb(document=None))

        assert repo.get_document_kind("wf-nope") is None

    @pytest.mark.unit
    def test_returns_none_when_the_column_is_null(self):
        # Old rows predate the column default, so it can be NULL.
        repo = DocumentRepository(FakeDb(document={"document_kind": None}))

        assert repo.get_document_kind("wf-1") is None

    @pytest.mark.unit
    def test_does_not_normalise_the_value(self):
        repo = DocumentRepository(FakeDb(document={"document_kind": "  Advisory "}))

        assert repo.get_document_kind("wf-1") == "  Advisory "

    @pytest.mark.unit
    def test_passes_the_workflow_id_through(self):
        fake = FakeDb(document={"document_kind": "scheme"})

        DocumentRepository(fake).get_document_kind("wf-42")

        assert fake.get_document_called_with == "wf-42"


class TestAgainstRealSqlite:
    """Guards against db.py drifting out from under the repository."""

    @pytest.mark.unit
    @pytest.mark.db
    def test_reads_a_kind_written_through_the_db_layer(self, db_connection, sample_document):
        workflow_id = sample_document["workflow_id"]
        db_connection.update_document_fields(workflow_id, document_kind="advisory")

        assert DocumentRepository().get_document_kind(workflow_id) == "advisory"

    @pytest.mark.unit
    @pytest.mark.db
    def test_unclassified_document_reads_as_the_column_default(self, db_connection, sample_document):
        assert DocumentRepository().get_document_kind(sample_document["workflow_id"]) == "document"

    @pytest.mark.unit
    @pytest.mark.db
    def test_missing_document_reads_as_none(self, db_connection):
        assert DocumentRepository().get_document_kind("no-such-workflow") is None
