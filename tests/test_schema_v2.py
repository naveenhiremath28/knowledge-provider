"""
Schema v2 rules: groups, catalogs, documents keyed by (catalog, code), the
group snapshot on audit_logs and the catalog_status view.

Everything runs against the real Postgres from conftest.py.
"""

import hashlib
import logging

import psycopg
import pytest
from psycopg.types.json import Jsonb


def _md5(text):
    return hashlib.md5(text.encode()).hexdigest()


def _doc(db, workflow_id, **kwargs):
    kwargs.setdefault("document_id", _md5(workflow_id))
    kwargs.setdefault("filename", f"{workflow_id}.pdf")
    kwargs.setdefault("filepath", f"minio://documents/{workflow_id}.pdf")
    kwargs.setdefault("kind", "scheme")
    kwargs.setdefault("code", workflow_id)
    return db.upsert_document(workflow_id=workflow_id, **kwargs)


def _group(db, group="mh"):
    return db.get_or_create_group(group)


class TestGroups:
    @pytest.mark.db
    @pytest.mark.unit
    def test_get_or_create_group_is_idempotent_and_maps_tenant_codes_to_paths(self, db_connection):
        db = db_connection
        mh = _group(db, "mh")
        assert _group(db, "MH") == mh
        assert _group(db, "up") != mh
        bv = _group(db, "bv")
        with db.get_connection() as conn:
            rows = {r["name"]: r["path"] for r in conn.execute("SELECT name, path FROM groups").fetchall()}
        assert rows == {"mh": "/states/MH", "up": "/states/UP", "bv": "/global"}
        assert _group(db, "bv") == bv

    @pytest.mark.db
    @pytest.mark.unit
    def test_upsert_group_fills_the_keycloak_id_of_an_existing_path(self, db_connection):
        db = db_connection
        first = db.upsert_group("/states/MH")  # as created by the SQLite move: no Keycloak id yet
        again = db.upsert_group("/states/MH", keycloak_group_id="kc-group-1")
        assert again == first
        with db.get_connection() as conn:
            row = conn.execute("SELECT keycloak_group_id, name FROM groups WHERE id = %s::uuid", (first,)).fetchone()
        assert (row["keycloak_group_id"], row["name"]) == ("kc-group-1", "mh")
        # A later sync that does not know the id must not erase it.
        db.upsert_group("/states/MH")
        with db.get_connection() as conn:
            row = conn.execute("SELECT keycloak_group_id FROM groups WHERE id = %s::uuid", (first,)).fetchone()
        assert row["keycloak_group_id"] == "kc-group-1"

    @pytest.mark.db
    @pytest.mark.unit
    def test_keycloak_group_id_is_unique_but_nullable(self, db_connection):
        with db_connection.get_connection() as conn:
            conn.execute("INSERT INTO groups (name, path) VALUES ('a', '/states/A')")
            conn.execute("INSERT INTO groups (name, path) VALUES ('b', '/states/B')")  # two NULLs are fine
            conn.execute("INSERT INTO groups (keycloak_group_id, name, path) VALUES ('k', 'c', '/states/C')")
            with pytest.raises(psycopg.errors.UniqueViolation):
                conn.execute("INSERT INTO groups (keycloak_group_id, name, path) VALUES ('k', 'd', '/states/D')")

    @pytest.mark.db
    @pytest.mark.unit
    def test_a_group_with_catalogs_cannot_be_deleted(self, db_connection):
        db = db_connection
        group_id = _group(db)
        db.get_or_create_catalog(group_id, "scheme")
        with db.get_connection() as conn:
            with pytest.raises(psycopg.errors.ForeignKeyViolation):
                conn.execute("DELETE FROM groups WHERE id = %s::uuid", (group_id,))

    @pytest.mark.db
    @pytest.mark.unit
    def test_deleting_a_group_clears_users_and_audit_snapshots(self, db_connection):
        db = db_connection
        group_id = _group(db, "zz")
        db.upsert_user("kc-zz", "zed", None, [], tenants=["zz"])
        with db.get_connection() as conn:
            conn.execute(
                "INSERT INTO audit_logs (action_type, entity_type, group_id) VALUES ('x', 'setting', %s::uuid)",
                (group_id,),
            )
            conn.execute("DELETE FROM groups WHERE id = %s::uuid", (group_id,))
            assert conn.execute("SELECT group_id FROM audit_logs WHERE action_type = 'x'").fetchone()["group_id"] is None
        assert db.get_user_by_keycloak_id("kc-zz")["group_id"] is None


class TestCatalogs:
    @pytest.mark.db
    @pytest.mark.unit
    def test_get_or_create_catalog_is_idempotent_per_group_and_kind(self, db_connection):
        db = db_connection
        mh = _group(db, "mh")
        scheme = db.get_or_create_catalog(mh, "scheme")
        assert db.get_or_create_catalog(mh, "scheme")["id"] == scheme["id"]
        assert scheme["type"] == "openagrinet:AgricultureResource"
        assert scheme["name"] == "mh scheme"
        advisory = db.get_or_create_catalog(mh, "advisory")
        assert advisory["id"] != scheme["id"]
        assert advisory["type"] == "openagrinet:KnowledgeAdvisory"
        assert db.get_or_create_catalog(_group(db, "up"), "scheme")["id"] != scheme["id"]

    @pytest.mark.db
    @pytest.mark.unit
    def test_a_kind_without_a_catalog_type_is_rejected(self, db_connection):
        with pytest.raises(ValueError):
            db_connection.get_or_create_catalog(_group(db_connection), "video")

    @pytest.mark.db
    @pytest.mark.unit
    def test_one_catalog_per_group_and_type(self, db_connection):
        db = db_connection
        group_id = _group(db)
        with db.get_connection() as conn:
            conn.execute(
                "INSERT INTO catalogs (group_id, type, name) VALUES (%s::uuid, 'openagrinet:MandiPrice', 'A')",
                (group_id,),
            )
            with pytest.raises(psycopg.errors.UniqueViolation):
                conn.execute(
                    "INSERT INTO catalogs (group_id, type, name) VALUES (%s::uuid, 'openagrinet:MandiPrice', 'B')",
                    (group_id,),
                )

    @pytest.mark.db
    @pytest.mark.unit
    @pytest.mark.parametrize("bad_type", ["scheme", "openagrinet:", ":Thing", "open agrinet:X", "a:b:c", ""])
    def test_type_must_look_like_a_schema_name(self, db_connection, bad_type):
        group_id = _group(db_connection)
        with db_connection.get_connection() as conn:
            with pytest.raises(psycopg.errors.CheckViolation):
                conn.execute(
                    "INSERT INTO catalogs (group_id, type, name) VALUES (%s::uuid, %s, 'N')",
                    (group_id, bad_type),
                )

    @pytest.mark.db
    @pytest.mark.unit
    def test_catalog_metadata_must_be_an_object(self, db_connection):
        group_id = _group(db_connection)
        with db_connection.get_connection() as conn:
            with pytest.raises(psycopg.errors.CheckViolation):
                conn.execute(
                    "INSERT INTO catalogs (group_id, type, name, metadata) "
                    "VALUES (%s::uuid, 'openagrinet:MandiPrice', 'N', '[]'::jsonb)",
                    (group_id,),
                )

    @pytest.mark.db
    @pytest.mark.unit
    def test_a_catalog_with_documents_cannot_be_deleted(self, db_connection):
        db = db_connection
        _doc(db, "wf-held")
        with db.get_connection() as conn:
            with pytest.raises(psycopg.errors.ForeignKeyViolation):
                conn.execute("DELETE FROM catalogs")


class TestDocumentIdentity:
    @pytest.mark.db
    @pytest.mark.unit
    def test_a_new_document_needs_a_kind_and_a_code(self, db_connection):
        db = db_connection
        common = dict(workflow_id="wf-x", document_id=_md5("x"), filename="x.pdf", filepath="/x.pdf")
        with pytest.raises(ValueError):
            db.upsert_document(**common, code="x")
        with pytest.raises(ValueError):
            db.upsert_document(**common, kind="scheme")
        assert db.get_document("wf-x") is None

    @pytest.mark.db
    @pytest.mark.unit
    def test_a_new_document_lands_in_its_tenants_catalog_of_that_kind(self, db_connection):
        db = db_connection
        _doc(db, "wf-land", group="mh", kind="advisory", code="kharif", name="Kharif advice")
        doc = db.get_document("wf-land")
        assert (doc["group"], doc["kind"], doc["code"], doc["name"]) == (
            "mh", "advisory", "kharif", "Kharif advice",
        )
        expected = db.get_or_create_catalog(_group(db, "mh"), "advisory")["id"]
        assert doc["catalog_id"] == expected

    @pytest.mark.db
    @pytest.mark.unit
    def test_the_name_defaults_to_the_code(self, db_connection):
        _doc(db_connection, "wf-name", code="pm-kisan")
        assert db_connection.get_document("wf-name")["name"] == "pm-kisan"

    @pytest.mark.db
    @pytest.mark.unit
    def test_a_code_is_unique_within_its_catalog(self, db_connection):
        db = db_connection
        _doc(db, "wf-one", group="mh", code="pm-kisan")
        with pytest.raises(db.DuplicateDocumentCode):
            _doc(db, "wf-two", group="mh", code="pm-kisan")
        assert db.get_document("wf-two") is None

    @pytest.mark.db
    @pytest.mark.unit
    def test_the_same_code_is_fine_in_another_group_or_kind(self, db_connection):
        db = db_connection
        _doc(db, "wf-mh", group="mh", code="pm-kisan")
        _doc(db, "wf-up", group="up", code="pm-kisan")
        _doc(db, "wf-adv", group="mh", code="pm-kisan", kind="advisory")
        assert {d["workflow_id"] for d in db.list_documents()} == {"wf-mh", "wf-up", "wf-adv"}

    @pytest.mark.db
    @pytest.mark.unit
    def test_a_duplicate_code_leaves_no_half_written_rows(self, db_connection):
        db = db_connection
        _doc(db, "wf-first", code="dup")
        with pytest.raises(db.DuplicateDocumentCode):
            _doc(db, "wf-second", code="dup")
        with db.get_connection() as conn:
            assert conn.execute("SELECT count(*) AS n FROM documents").fetchone()["n"] == 1
            assert conn.execute("SELECT count(*) AS n FROM workflows").fetchone()["n"] == 1

    @pytest.mark.db
    @pytest.mark.unit
    def test_find_document_by_code(self, db_connection):
        db = db_connection
        _doc(db, "wf-find", group="mh", code="pm-kisan")
        found = db.find_document_by_code("mh", "scheme", "pm-kisan")
        assert found and found["workflow_id"] == "wf-find"
        assert db.find_document_by_code("MH", "scheme", "pm-kisan")["id"] == found["id"]
        assert db.find_document_by_code("up", "scheme", "pm-kisan") is None
        assert db.find_document_by_code("mh", "advisory", "pm-kisan") is None
        assert db.find_document_by_code("mh", "scheme", "other") is None

    @pytest.mark.db
    @pytest.mark.unit
    def test_changing_the_kind_moves_the_document_within_its_group(self, db_connection):
        db = db_connection
        _doc(db, "wf-kind", group="mh", code="movable")
        db.update_document_fields("wf-kind", kind="advisory")
        doc = db.get_document("wf-kind")
        assert doc["kind"] == "advisory"
        with db.get_connection() as conn:
            row = conn.execute(
                "SELECT c.group_id, c.type FROM catalogs c WHERE c.id = %s::uuid", (doc["catalog_id"],)
            ).fetchone()
        assert str(row["group_id"]) == _group(db, "mh")
        assert row["type"] == "openagrinet:KnowledgeAdvisory"

    @pytest.mark.db
    @pytest.mark.unit
    def test_moving_to_a_kind_that_already_holds_the_code_is_a_duplicate(self, db_connection):
        db = db_connection
        _doc(db, "wf-s", group="mh", code="same")
        _doc(db, "wf-a", group="mh", code="same", kind="advisory")
        with pytest.raises(db.DuplicateDocumentCode):
            db.update_document_fields("wf-s", kind="advisory")
        assert db.get_document("wf-s")["kind"] == "scheme"


class TestReupload:
    @pytest.mark.db
    @pytest.mark.unit
    def test_reset_for_reupload_replaces_the_source_and_clears_the_content(self, db_connection):
        db = db_connection
        doc_uuid = _doc(db, "wf-re", filename="old.pdf", valid_from="2026-01-01", valid_to="2026-12-31")
        db.save_pages("wf-re", [{"page_number": 1, "original_markdown": "old"}])
        db.save_chunks("wf-re", [{"chunk_number": 1, "original_text": "old", "chunking_provider": "p"}])
        db.upsert_document_index_status("wf-re", "documents-index", status="indexed")
        artifact_id = db.add_document_artifact("wf-re", "original_upload", "minio://documents/old.pdf")
        new_fingerprint = _md5("new content")

        db.reset_document_for_reupload(
            doc_uuid, filename="new.pdf", filepath="minio://documents/new.pdf", fingerprint=new_fingerprint,
        )

        doc = db.get_document("wf-re")
        assert doc["id"] == doc_uuid
        assert doc["source_file_fingerprint"] == new_fingerprint
        assert (doc["filename"], doc["filepath"]) == ("new.pdf", "minio://documents/new.pdf")
        assert db.get_pages("wf-re") == []
        assert db.get_chunks("wf-re") == []
        assert (doc["page_count"], doc["chunk_count"]) == (0, 0)
        assert doc["reindex_required"] is True
        assert doc["reindex_reason"] == "source_replaced"

        # What is not content survives: workflows, artifacts, period, index state, code.
        assert [j["job_type"] for j in db.list_document_jobs("wf-re")] == ["pipeline"]
        assert db.get_document_artifact("wf-re", artifact_id) is not None
        assert (doc["valid_from"], doc["valid_to"]) == ("2026-01-01", "2026-12-31")
        assert "dev" in doc["index_status"]
        assert doc["code"] == "wf-re"
        with db.get_connection() as conn:
            assert "chunking" not in conn.execute(
                "SELECT metadata FROM documents WHERE id = %s::uuid", (doc_uuid,)
            ).fetchone()["metadata"]

    @pytest.mark.db
    @pytest.mark.unit
    def test_a_rechunk_after_reupload_starts_a_fresh_run_number(self, db_connection):
        db = db_connection
        doc_uuid = _doc(db, "wf-run")
        db.save_chunks("wf-run", [{"chunk_number": 1, "original_text": "a", "chunking_provider": "p"}])
        db.reset_document_for_reupload(
            doc_uuid, filename="n.pdf", filepath="/n.pdf", fingerprint=_md5("n"),
        )
        db.save_chunks("wf-run", [{"chunk_number": 1, "original_text": "b", "chunking_provider": "p"}])
        assert db.get_chunk("wf-run", 1)["chunk_version"] == 1

    @pytest.mark.db
    @pytest.mark.unit
    def test_a_fingerprint_must_be_hex(self, db_connection):
        doc_uuid = _doc(db_connection, "wf-bad")
        with pytest.raises(ValueError):
            db_connection.reset_document_for_reupload(
                doc_uuid, filename="n.pdf", filepath="/n.pdf", fingerprint="not-hex",
            )


class TestUserGroup:
    @pytest.mark.db
    @pytest.mark.unit
    def test_a_single_tenant_sets_the_group_without_a_warning(self, db_connection, caplog):
        db = db_connection
        with caplog.at_level(logging.WARNING, logger="pipeline.db"):
            db.upsert_user("kc-one", "one", None, [], tenants=["MH"])
        assert db.get_user_by_keycloak_id("kc-one")["group_id"] == _group(db, "mh")
        assert not [r for r in caplog.records if "tenants" in r.getMessage()]

    @pytest.mark.db
    @pytest.mark.unit
    def test_several_tenants_use_the_first_sorted_one_and_warn(self, db_connection, caplog):
        db = db_connection
        with caplog.at_level(logging.WARNING, logger="pipeline.db"):
            db.upsert_user("kc-many", "many", None, [], tenants=["up", "mh", "up"])
        assert db.get_user_by_keycloak_id("kc-many")["group_id"] == _group(db, "mh")
        warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
        assert any("kc-many" in w and "mh" in w for w in warnings)

    @pytest.mark.db
    @pytest.mark.unit
    def test_a_login_without_tenants_keeps_the_stored_group(self, db_connection):
        db = db_connection
        db.upsert_user("kc-keep", "keep", None, [], tenants=["up"])
        db.upsert_user("kc-keep", "keep", None, [])
        db.upsert_user("kc-keep", "keep", None, [], tenants=[])
        assert db.get_user_by_keycloak_id("kc-keep")["group_id"] == _group(db, "up")

    @pytest.mark.db
    @pytest.mark.unit
    def test_a_new_login_moves_the_user_to_the_new_first_tenant(self, db_connection):
        db = db_connection
        db.upsert_user("kc-move", "move", None, [], tenants=["up"])
        db.upsert_user("kc-move", "move", None, [], tenants=["mh"])
        assert db.get_user_by_keycloak_id("kc-move")["group_id"] == _group(db, "mh")

    @pytest.mark.db
    @pytest.mark.unit
    def test_users_without_tenants_have_no_group(self, db_connection):
        db_connection.upsert_user("kc-none", "none", None, [])
        assert db_connection.get_user_by_keycloak_id("kc-none")["group_id"] is None

    @pytest.mark.db
    @pytest.mark.unit
    def test_the_uploader_lands_in_the_documents_metadata_and_gets_the_tenant_group(self, db_connection):
        db = db_connection
        _doc(db, "wf-up", group="mh", uploaded_by_user_id="kc-up", uploaded_by_username="uma")
        doc = db.get_document("wf-up")
        user = db.get_user_by_keycloak_id("kc-up")
        assert doc["uploaded_by_user_id"] == "kc-up"
        assert user["group_id"] == _group(db, "mh")
        with db.get_connection() as conn:
            meta = conn.execute("SELECT metadata FROM documents").fetchone()["metadata"]
        assert meta["uploaded_by_user_id"] == user["id"]


class TestAuditGroupSnapshot:
    @pytest.mark.db
    @pytest.mark.unit
    def test_an_entry_snapshots_the_documents_group(self, db_connection):
        db = db_connection
        _doc(db, "wf-aud", group="mh")
        db.log_audit(workflow_id="wf-aud", document_id="x", action_type="page_edit", entity_type="page")
        with db.get_connection() as conn:
            row = conn.execute("SELECT group_id FROM audit_logs WHERE action_type = 'page_edit'").fetchone()
        assert str(row["group_id"]) == _group(db, "mh")

    @pytest.mark.db
    @pytest.mark.unit
    def test_the_snapshot_survives_a_purge_and_keeps_tenant_scope(self, db_connection):
        db = db_connection
        _doc(db, "wf-gone", group="mh")
        _doc(db, "wf-other", group="up", code="other")
        for wf in ("wf-gone", "wf-other"):
            db.log_audit(workflow_id=wf, document_id="x", action_type="purge_document", entity_type="document")
        db.purge_document("wf-gone")
        db.purge_document("wf-other")

        with db.get_connection() as conn:
            rows = conn.execute(
                "SELECT document_id, group_id, workflow_id FROM audit_logs WHERE action_type = 'purge_document'"
            ).fetchall()
        assert {r["workflow_id"] for r in rows} == {"wf-gone", "wf-other"}
        assert all(r["document_id"] is None for r in rows)
        assert {str(r["group_id"]) for r in rows} == {_group(db, "mh"), _group(db, "up")}
        assert db.get_all_audit_log_count(groups=["mh"]) == 1
        assert db.get_all_audit_log_count(groups=["up"]) == 1
        assert db.get_all_audit_log_count(groups=["gj"]) == 0
        assert [e["workflow_id"] for e in db.get_all_audit_logs(groups=["mh"])] == ["wf-gone"]
        assert db.get_all_audit_logs(groups=["mh"])[0]["group"] == "mh"

    @pytest.mark.db
    @pytest.mark.unit
    def test_system_entries_have_no_group(self, db_connection):
        db = db_connection
        db.log_audit(workflow_id="__catalog__", document_id="__catalog__", action_type="catalog_rebuild",
                     entity_type="catalog")
        with db.get_connection() as conn:
            row = conn.execute("SELECT group_id, document_id FROM audit_logs WHERE action_type = 'catalog_rebuild'").fetchone()
        assert row["group_id"] is None and row["document_id"] is None


class TestCatalogStatusView:
    @staticmethod
    def _rows(db):
        with db.get_connection() as conn:
            return {r["code"]: r for r in conn.execute("SELECT * FROM catalog_status").fetchall()}

    @pytest.mark.db
    @pytest.mark.unit
    def test_one_row_per_document_with_catalog_group_and_type(self, db_connection):
        db = db_connection
        _doc(db, "wf-1", group="mh", code="alpha", stage="completed")
        _doc(db, "wf-2", group="mh", code="beta", stage="completed")
        _doc(db, "wf-3", group="up", code="gamma", stage="completed", kind="advisory")
        rows = self._rows(db)
        assert set(rows) == {"alpha", "beta", "gamma"}
        assert rows["alpha"]["catalog_id"] == rows["beta"]["catalog_id"] != rows["gamma"]["catalog_id"]
        assert str(rows["alpha"]["group_id"]) == _group(db, "mh")
        assert (rows["alpha"]["group_name"], rows["gamma"]["group_name"]) == ("mh", "up")
        assert rows["alpha"]["type"] == "openagrinet:AgricultureResource"
        assert rows["gamma"]["type"] == "openagrinet:KnowledgeAdvisory"
        assert rows["alpha"]["workflow_id"] == "wf-1"
        assert rows["alpha"]["name"] == "alpha"

    @pytest.mark.db
    @pytest.mark.unit
    def test_status_follows_stage_flags_and_disabling(self, db_connection):
        db = db_connection
        _doc(db, "wf-live", code="live", stage="completed")
        _doc(db, "wf-stale", code="stale", stage="completed")
        db.mark_document_reindex_required("wf-stale", True, "edit")
        _doc(db, "wf-gate", code="gate", stage="approval_for_prod")
        _doc(db, "wf-wip", code="wip", stage="chunk_review")
        _doc(db, "wf-off", code="off", stage="completed")
        db.set_document_disabled("wf-off", True)
        _doc(db, "wf-hidden", code="hidden", stage="completed")
        db.update_document_fields("wf-hidden", catalog_visible=False)
        _doc(db, "wf-gate-off", code="gate-off", stage="approval_for_prod")
        db.set_document_disabled("wf-gate-off", True)

        status = {code: row["status"] for code, row in self._rows(db).items()}
        assert status == {
            "live": "live",
            "stale": "pending_reindex",
            "gate": "pending_prod",
            "wip": "disabled",
            "off": "disabled",
            "hidden": "disabled",
            "gate-off": "disabled",
        }

    @pytest.mark.db
    @pytest.mark.unit
    def test_chunk_count_is_counted_from_chunks(self, db_connection):
        db = db_connection
        _doc(db, "wf-c", code="counted", stage="completed")
        assert self._rows(db)["counted"]["chunk_count"] == 0
        db.save_chunks("wf-c", [{"chunk_number": n, "original_text": "t"} for n in (1, 2, 3)])
        assert self._rows(db)["counted"]["chunk_count"] == 3
        db.save_chunks("wf-c", [{"chunk_number": 1, "original_text": "t"}])
        assert self._rows(db)["counted"]["chunk_count"] == 1

    @pytest.mark.db
    @pytest.mark.unit
    def test_network_visible_defaults_to_true_and_follows_the_flag(self, db_connection):
        db = db_connection
        _doc(db, "wf-v1", code="visible", stage="completed")
        _doc(db, "wf-v2", code="private", stage="completed")
        db.update_document_fields("wf-v2", network_visible=False)
        rows = self._rows(db)
        assert (rows["visible"]["network_visible"], rows["private"]["network_visible"]) == (True, False)

    @pytest.mark.db
    @pytest.mark.unit
    def test_promoted_at_comes_from_the_prod_index_of_live_documents_only(self, db_connection, monkeypatch):
        monkeypatch.setenv("PROD_VECTOR_DB_COLLECTION_NAME", "prod-docs")
        db = db_connection
        _doc(db, "wf-p1", code="promoted", stage="completed")
        _doc(db, "wf-p2", code="not-live", stage="chunk_review")
        _doc(db, "wf-p3", code="dev-only", stage="completed")
        for wf in ("wf-p1", "wf-p2"):
            db.upsert_document_index_status(wf, "prod-docs", last_indexed_at="2026-05-01T10:00:00+00:00")
        db.upsert_document_index_status("wf-p3", "documents-index", last_indexed_at="2026-05-02T10:00:00+00:00")
        rows = self._rows(db)
        assert rows["promoted"]["promoted_at"] is not None
        assert rows["promoted"]["promoted_at"].isoformat().startswith("2026-05-01T10:00:00")
        assert rows["not-live"]["promoted_at"] is None
        assert rows["dev-only"]["promoted_at"] is None

    @pytest.mark.db
    @pytest.mark.unit
    def test_a_catalog_without_documents_has_no_rows(self, db_connection):
        db = db_connection
        db.get_or_create_catalog(_group(db), "scheme")
        assert self._rows(db) == {}


class TestDocumentJsonbShape:
    @pytest.mark.db
    @pytest.mark.unit
    def test_a_new_document_carries_the_default_metadata_keys(self, db_connection):
        db = db_connection
        _doc(db, "wf-meta")
        with db.get_connection() as conn:
            meta = conn.execute("SELECT metadata FROM documents").fetchone()["metadata"]
        assert meta["index_status"] == {}
        assert (meta["catalog_visible"], meta["network_visible"], meta["reindex_required"]) == (True, True, False)
        for dropped in ("is_demo", "chunk_count", "display_name"):
            assert dropped not in meta

    @pytest.mark.db
    @pytest.mark.unit
    def test_the_source_has_no_page_count(self, db_connection):
        db = db_connection
        _doc(db, "wf-src")
        db.save_pages("wf-src", [{"page_number": 1, "original_markdown": "a"}])
        with db.get_connection() as conn:
            source = conn.execute("SELECT source FROM documents").fetchone()["source"]
        assert "page_count" not in source
        assert db.get_document("wf-src")["page_count"] == 1

    @pytest.mark.db
    @pytest.mark.unit
    def test_there_are_no_instance_kind_or_demo_columns(self, db_connection):
        with db_connection.get_connection() as conn:
            cols = {
                r["column_name"]
                for r in conn.execute(
                    "SELECT column_name FROM information_schema.columns WHERE table_name = 'documents'"
                ).fetchall()
            }
        assert {"instance", "document_kind", "is_demo", "catalog_entry_id", "valid_from", "valid_to",
                "index_status", "uploaded_by_user_id"}.isdisjoint(cols)
        assert {"catalog_id", "code", "name", "aliases", "source", "is_disabled", "metadata"} <= cols

    @pytest.mark.db
    @pytest.mark.unit
    def test_aliases_cannot_hold_an_empty_string(self, db_connection):
        db = db_connection
        _doc(db, "wf-al", aliases=["PM Kisan", "", "pmk"])
        with db.get_connection() as conn:
            aliases = conn.execute("SELECT aliases FROM documents").fetchone()["aliases"]
        assert aliases == ["PM Kisan", "pmk"]
        with db.get_connection() as conn:
            with pytest.raises(psycopg.errors.CheckViolation):
                conn.execute("UPDATE documents SET aliases = ARRAY['a', '']")

    @pytest.mark.db
    @pytest.mark.unit
    def test_document_metadata_must_be_an_object(self, db_connection):
        db = db_connection
        _doc(db, "wf-obj")
        with db.get_connection() as conn:
            with pytest.raises(psycopg.errors.CheckViolation):
                conn.execute("UPDATE documents SET metadata = %s", (Jsonb([1]),))

    @pytest.mark.db
    @pytest.mark.unit
    def test_workflow_and_audit_columns_are_plain_text(self, db_connection):
        with db_connection.get_connection() as conn:
            types = {
                (r["table_name"], r["column_name"]): r["data_type"]
                for r in conn.execute(
                    "SELECT table_name, column_name, data_type FROM information_schema.columns "
                    "WHERE (table_name, column_name) IN (('workflows','workflow_type'), ('workflows','status'), "
                    "('workflows','current_stage'), ('workflow_artifacts','stage'), ('audit_logs','entity_type'))"
                ).fetchall()
            }
            enums = conn.execute("SELECT count(*) AS n FROM pg_type WHERE typtype = 'e'").fetchone()["n"]
        assert len(types) == 5 and set(types.values()) == {"text"}
        assert enums == 0


class TestSupersededIndexKeys:
    """A replaced file changes the index key; the old key must be remembered for cleanup."""

    @staticmethod
    def _indexed(db, workflow_id, envs):
        doc_uuid = _doc(db, workflow_id)
        old_key = db.get_document(workflow_id)["document_id"]
        for env in envs:
            index = "prod-index" if env == "prod" else "documents-index"
            db.upsert_document_index_status(
                workflow_id, index, doc_id=old_key, chunk_count_indexed=1, status="indexed"
            )
        return doc_uuid, old_key

    @pytest.mark.db
    @pytest.mark.unit
    def test_reupload_remembers_the_old_key_for_every_index_that_holds_it(
        self, db_connection, monkeypatch
    ):
        db = db_connection
        monkeypatch.setenv("PROD_VECTOR_DB_COLLECTION_NAME", "prod-index")
        doc_uuid, old_key = self._indexed(db, "wf-sup", ["dev", "prod"])

        db.reset_document_for_reupload(
            doc_uuid, filename="n.pdf", filepath="minio://documents/n.pdf", fingerprint="b" * 32
        )

        assert db.get_document(doc_uuid)["superseded_doc_ids"] == {"dev": [old_key], "prod": [old_key]}

    @pytest.mark.db
    @pytest.mark.unit
    def test_only_indexes_that_hold_the_old_key_are_tracked(self, db_connection):
        db = db_connection
        doc_uuid, old_key = self._indexed(db, "wf-sup-dev", ["dev"])

        db.reset_document_for_reupload(
            doc_uuid, filename="n.pdf", filepath="minio://documents/n.pdf", fingerprint="c" * 32
        )

        assert db.get_document(doc_uuid)["superseded_doc_ids"] == {"dev": [old_key]}

    @pytest.mark.db
    @pytest.mark.unit
    def test_the_same_file_again_supersedes_nothing(self, db_connection):
        db = db_connection
        doc_uuid, old_key = self._indexed(db, "wf-sup-same", ["dev"])

        db.reset_document_for_reupload(
            doc_uuid, filename="n.pdf", filepath="minio://documents/n.pdf", fingerprint=old_key
        )

        assert db.get_document(doc_uuid)["superseded_doc_ids"] == {}

    @pytest.mark.db
    @pytest.mark.unit
    def test_two_replacements_keep_both_old_keys_until_cleared(self, db_connection):
        db = db_connection
        doc_uuid, first_key = self._indexed(db, "wf-sup-two", ["dev"])
        db.reset_document_for_reupload(
            doc_uuid, filename="n.pdf", filepath="minio://documents/n.pdf", fingerprint="d" * 32
        )
        # The second version never reached the index, so its key is not tracked.
        db.reset_document_for_reupload(
            doc_uuid, filename="m.pdf", filepath="minio://documents/m.pdf", fingerprint="e" * 32
        )
        assert db.get_document(doc_uuid)["superseded_doc_ids"] == {"dev": [first_key]}

        db.clear_superseded_doc_ids(doc_uuid, "dev", [first_key])

        assert db.get_document(doc_uuid)["superseded_doc_ids"] == {}
