"""Unit tests for multi-group (tenant) scoping."""

from __future__ import annotations

import logging
import re

import pytest
from fastapi import HTTPException

from pipeline.auth.jwt import claims_to_user
from pipeline.auth.models import local_bypass_user
from pipeline.auth.tenancy import (
    PORTAL_GROUP,
    allowed_groups,
    assert_document_group_access,
    assert_group_access,
    resolve_create_group,
    user_can_access_group,
)


def test_bypass_user_is_unrestricted():
    user = local_bypass_user()
    assert allowed_groups(user) is None
    assert user_can_access_group(user, "tenant-a")
    assert user_can_access_group(user, "tenant-b")


def test_resolve_create_group_state_user_defaults_to_only_state():
    user = claims_to_user(
        {
            "sub": "u1",
            "groups": ["/states/MH/contributor"],
        }
    )
    assert resolve_create_group(user, None) == "mh"
    assert resolve_create_group(user, "MH") == "mh"
    with pytest.raises(HTTPException) as exc:
        resolve_create_group(user, "up")
    assert exc.value.status_code == 403


def test_resolve_create_group_superadmin_defaults_to_portal():
    user = claims_to_user(
        {
            "sub": "sa",
            "groups": ["/global/super-admin"],
        }
    )
    assert resolve_create_group(user, None) == PORTAL_GROUP
    assert resolve_create_group(user, "mh") == "mh"


def test_resolve_create_group_multi_state_requires_choice():
    user = claims_to_user(
        {
            "sub": "u2",
            "groups": ["/states/MH/contributor", "/states/UP/view"],
        }
    )
    with pytest.raises(HTTPException) as exc:
        resolve_create_group(user, None)
    assert exc.value.status_code == 400
    assert resolve_create_group(user, "up") == "up"


def test_user_instances_are_enforced():
    user = claims_to_user(
        {
            "sub": "u1",
            "realm_access": {"roles": ["content_curator"]},
            "instances": ["Tenant-A", "tenant-b"],
        }
    )
    assert allowed_groups(user) == {"tenant-a", "tenant-b"}
    assert user_can_access_group(user, "tenant-a")
    assert not user_can_access_group(user, "mh")
    with pytest.raises(HTTPException) as exc:
        assert_group_access(user, "mh")
    assert exc.value.status_code == 403


def test_document_access_hides_other_instances():
    user = claims_to_user(
        {
            "sub": "u1",
            "realm_access": {"roles": ["viewer"]},
            "instances": ["tenant-a"],
        }
    )
    with pytest.raises(HTTPException) as exc:
        assert_document_group_access(user, {"workflow_id": "wf", "group": "tenant-b"})
    assert exc.value.status_code == 404


def test_superadmin_token_is_group_unrestricted_despite_scoped_claim():
    """Platform superadmin stays unrestricted even with a narrow instances claim."""
    for role in ("superadmin", "super_admin", "super-admin"):
        user = claims_to_user(
            {
                "sub": "admin-1",
                "realm_access": {"roles": [role]},
                "instances": ["tenant-a"],  # scoped claim must NOT limit superadmin
            }
        )
        assert user.is_superadmin is True
        assert user.is_admin is True  # alias
        assert user.is_group_unrestricted() is True
        # Unrestricted -> allowed_groups is None (every group visible).
        assert allowed_groups(user) is None
        assert user_can_access_group(user, "tenant-b")
        doc = assert_document_group_access(
            user, {"workflow_id": "wf", "group": "tenant-b"}
        )
        assert doc["group"] == "tenant-b"


def test_state_admin_is_scoped_to_instances_claim():
    """State-level admin is limited to claimed instances (not platform-wide)."""
    from pipeline.auth.permissions import Permission

    user = claims_to_user(
        {
            "sub": "state-1",
            "realm_access": {"roles": ["admin"]},
            "instances": ["tenant-a"],
        }
    )
    assert user.is_superadmin is False
    assert user.is_group_unrestricted() is False
    assert allowed_groups(user) == {"tenant-a"}
    assert user_can_access_group(user, "tenant-a")
    assert not user_can_access_group(user, "tenant-b")
    assert Permission.SEARCH in user.permissions
    assert Permission.UPLOAD in user.permissions
    assert Permission.ADMIN not in user.permissions
    with pytest.raises(HTTPException) as exc:
        assert_document_group_access(user, {"workflow_id": "wf", "group": "tenant-b"})
    assert exc.value.status_code == 404


def test_content_curator_with_scoped_claim_cannot_cross_tenants():
    """Non-superadmin roles remain limited to their claimed instances."""
    user = claims_to_user(
        {
            "sub": "curator-1",
            "realm_access": {"roles": ["content_curator"]},
            "instances": ["tenant-a"],
        }
    )
    assert user.is_admin is False
    assert user.is_group_unrestricted() is False
    assert allowed_groups(user) == {"tenant-a"}
    assert user_can_access_group(user, "tenant-a")
    assert not user_can_access_group(user, "tenant-b")
    with pytest.raises(HTTPException) as exc:
        assert_document_group_access(user, {"workflow_id": "wf", "group": "tenant-b"})
    assert exc.value.status_code == 404


def _register(db, **kwargs):
    """upsert_document the v2 way: a new document needs a kind and a code.

    The tenant (``group``) picks the group row; the kind plus that group pick the
    catalog, which is created on first use.
    """
    kwargs.setdefault("kind", "scheme")
    kwargs.setdefault("code", re.sub(r"[^a-z0-9]+", "-", kwargs["workflow_id"].lower()).strip("-"))
    return db.upsert_document(**kwargs)


def test_list_documents_filters_by_group(db_connection):
    db = db_connection
    _register(db,
        workflow_id="wf-tenant-a",
        document_id="d1",
        filename="a.pdf",
        filepath="/tmp/a.pdf",
        stage="completed",
        group="tenant-a",
    )
    _register(db,
        workflow_id="wf-tenant-b",
        document_id="d2",
        filename="b.pdf",
        filepath="/tmp/b.pdf",
        stage="completed",
        group="tenant-b",
    )

    tenant_a_only = db.list_documents(groups=["tenant-a"])
    assert {d["workflow_id"] for d in tenant_a_only} == {"wf-tenant-a"}

    both = db.list_documents(groups=["tenant-a", "tenant-b"])
    assert {d["workflow_id"] for d in both} == {"wf-tenant-a", "wf-tenant-b"}

    none = db.list_documents(groups=[])
    assert none == []


def test_summary_counts_honor_group_filter(db_connection):
    db = db_connection
    _register(db,
        workflow_id="wf-tenant-a-2",
        document_id="d3",
        filename="c.pdf",
        filepath="/tmp/c.pdf",
        stage="completed",
        group="tenant-a",
    )
    _register(db,
        workflow_id="wf-tenant-b-2",
        document_id="d4",
        filename="d.pdf",
        filepath="/tmp/d.pdf",
        stage="failed",
        group="tenant-b",
    )
    summary = db.get_document_summary_counts(groups=["tenant-a"])
    assert summary["total_documents"] == 1
    assert summary["completed_documents"] == 1
    assert summary["failed_documents"] == 0


def test_list_runs_filters_by_group(db_connection):
    """Runs must honor the same tenant scope as documents (role-based states)."""
    db = db_connection
    doc_a = _register(db,
        with_workflow=False,
        workflow_id="wf-run-a",
        document_id="d-run-a",
        filename="a.pdf",
        filepath="/tmp/a.pdf",
        stage="completed",
        group="tenant-a",
    )
    doc_b = _register(db,
        with_workflow=False,
        workflow_id="wf-run-b",
        document_id="d-run-b",
        filename="b.pdf",
        filepath="/tmp/b.pdf",
        stage="completed",
        group="tenant-b",
    )
    job_a = db.create_document_job(doc_a, "ocr_only", status="completed")
    job_b = db.create_document_job(doc_b, "ocr_only", status="running")

    tenant_a_only = db.list_runs(groups=["tenant-a"])
    assert {r["id"] for r in tenant_a_only} == {job_a}
    assert all(r.get("group") == "tenant-a" for r in tenant_a_only)

    both = db.list_runs(groups=["tenant-a", "tenant-b"])
    assert {r["id"] for r in both} == {job_a, job_b}

    none = db.list_runs(groups=[])
    assert none == []

    unrestricted_rows = db.list_runs(groups=None)
    assert {r["id"] for r in unrestricted_rows} >= {job_a, job_b}

    # Status filter still works under group scope.
    running_a = db.list_runs(status="running", groups=["tenant-a"])
    assert running_a == []
    running_b = db.list_runs(status="running", groups=["tenant-b"])
    assert {r["id"] for r in running_b} == {job_b}


def test_state_user_cannot_see_bv_portal_documents(db_connection):
    """State-scoped users must not see portal (BV) docs — only their state."""
    db = db_connection
    _register(db,
        workflow_id="wf-bv",
        document_id="d-bv",
        filename="portal.pdf",
        filepath="/tmp/portal.pdf",
        stage="ocr_review",
        group=PORTAL_GROUP,
    )
    _register(db,
        workflow_id="wf-mh",
        document_id="d-mh",
        filename="mh.pdf",
        filepath="/tmp/mh.pdf",
        stage="ocr_review",
        group="mh",
    )
    db.create_document_job("wf-bv", "ocr_only", status="running")
    db.create_document_job("wf-mh", "ocr_only", status="running")

    # MH contributor from Keycloak group path
    mh_user = claims_to_user(
        {
            "sub": "mh-user",
            "groups": ["/states/MH/contributor"],
            "realm_access": {"roles": ["contributor"]},
        }
    )
    assert mh_user.is_superadmin is False
    assert allowed_groups(mh_user) == {"mh"}
    assert not user_can_access_group(mh_user, PORTAL_GROUP)
    assert not user_can_access_group(mh_user, "bv")

    mh_docs = db.list_documents(groups=sorted(allowed_groups(mh_user) or []))
    assert {d["workflow_id"] for d in mh_docs} == {"wf-mh"}

    mh_runs = db.list_runs(groups=sorted(allowed_groups(mh_user) or []))
    assert all(r.get("group") == "mh" for r in mh_runs)
    assert {r["workflow_id"] for r in mh_runs} == {"wf-mh"}

    mh_queue, mh_total = db.list_operations_queue(
        groups=sorted(allowed_groups(mh_user) or [])
    )
    assert mh_total == 1
    assert {r["workflow_id"] for r in mh_queue} == {"wf-mh"}

    with pytest.raises(HTTPException) as exc:
        assert_document_group_access(
            mh_user, {"workflow_id": "wf-bv", "group": PORTAL_GROUP}
        )
    assert exc.value.status_code == 404

    # Super-admin sees both
    sa = claims_to_user({"sub": "sa", "groups": ["/global/super-admin"]})
    assert allowed_groups(sa) is None
    assert user_can_access_group(sa, PORTAL_GROUP)
    all_docs = db.list_documents(groups=None)
    assert {d["workflow_id"] for d in all_docs} >= {"wf-bv", "wf-mh"}


def test_upsert_does_not_reassign_group(db_connection):
    db = db_connection
    _register(db,
        workflow_id="wf-owned",
        document_id="d1",
        filename="a.pdf",
        filepath="/tmp/a.pdf",
        stage="registered",
        group="tenant-a",
    )
    _register(db,
        workflow_id="wf-owned",
        document_id="d1",
        filename="a.pdf",
        filepath="/tmp/a.pdf",
        stage="ocr_review",
        group="tenant-b",  # must be ignored on update
    )
    doc = db.get_document("wf-owned")
    assert doc["group"] == "tenant-a"
    assert doc["stage"] == "ocr_review"


def test_api_helpers_hide_cross_tenant_mutations(db_connection, monkeypatch):
    """Mutation helpers must 404 (not 403) for other tenants."""
    import pipeline.api as api
    import pipeline.db as db_mod

    monkeypatch.setattr(api, "db", db_mod)
    _register(db_mod,
        workflow_id="wf-tenant-b-doc",
        document_id="d-tenant-b",
        filename="b.pdf",
        filepath="/tmp/b.pdf",
        stage="ocr_review",
        group="tenant-b",
    )
    user = claims_to_user(
        {
            "sub": "u1",
            "realm_access": {"roles": ["content_curator"]},
            "instances": ["tenant-a"],
        }
    )
    with pytest.raises(HTTPException) as exc:
        api._require_document_for_user("wf-tenant-b-doc", user)
    assert exc.value.status_code == 404
    assert api._document_for_user_or_none("wf-tenant-b-doc", user) is None


def test_group_is_reached_through_the_documents_catalog(db_connection):
    """No documents.instance column: a document's group is a groups row reached through its catalog."""
    db = db_connection
    _register(db, workflow_id="wf-g-a", document_id="g1", filename="a.pdf",
              filepath="/tmp/a.pdf", group="tenant-a")
    _register(db, workflow_id="wf-g-bv", document_id="g2", filename="b.pdf",
              filepath="/tmp/b.pdf", group=PORTAL_GROUP)
    _register(db, workflow_id="wf-g-adv", document_id="g3", filename="c.pdf",
              filepath="/tmp/c.pdf", group="tenant-a", kind="advisory")

    with db.get_connection() as conn:
        groups = {r["name"]: r["path"] for r in conn.execute("SELECT name, path FROM groups")}
        catalogs = conn.execute(
            "SELECT g.name AS group_name, c.type FROM catalogs c JOIN groups g ON g.id = c.group_id"
        ).fetchall()
    assert groups == {
        "tenant-a": "/states/TENANT-A",
        PORTAL_GROUP: "/global" if PORTAL_GROUP == "bv" else f"/states/{PORTAL_GROUP.upper()}",
    }
    # One catalog per (group, type): tenant-a has scheme + advisory, the portal only scheme.
    assert sorted((r["group_name"], r["type"]) for r in catalogs) == sorted([
        ("tenant-a", "openagrinet:AgricultureResource"),
        ("tenant-a", "openagrinet:KnowledgeAdvisory"),
        (PORTAL_GROUP, "openagrinet:AgricultureResource"),
    ])
    assert db.get_document("wf-g-a")["group"] == "tenant-a"
    assert db.get_document("wf-g-adv")["kind"] == "advisory"
    assert {d["workflow_id"] for d in db.list_documents(groups=["tenant-a"])} == {"wf-g-a", "wf-g-adv"}


def test_same_code_is_allowed_in_another_tenants_catalog(db_connection):
    db = db_connection
    _register(db, workflow_id="wf-c-a", document_id="c1", filename="a.pdf",
              filepath="/tmp/a.pdf", group="tenant-a", code="pm-kisan")
    _register(db, workflow_id="wf-c-b", document_id="c2", filename="b.pdf",
              filepath="/tmp/b.pdf", group="tenant-b", code="pm-kisan")
    assert db.get_document("wf-c-a")["code"] == db.get_document("wf-c-b")["code"] == "pm-kisan"
    # ...but not twice inside one tenant's catalog of that kind.
    with pytest.raises(db.DuplicateDocumentCode):
        _register(db, workflow_id="wf-c-a2", document_id="c3", filename="a2.pdf",
                  filepath="/tmp/a2.pdf", group="tenant-a", code="pm-kisan")


def test_audit_keeps_the_tenant_scope_after_the_document_is_purged(db_connection):
    """audit_logs snapshots the group, so a purged document's history stays tenant-scoped."""
    db = db_connection
    _register(db, workflow_id="wf-aud-a", document_id="a1", filename="a.pdf",
              filepath="/tmp/a.pdf", group="tenant-a")
    _register(db, workflow_id="wf-aud-b", document_id="a2", filename="b.pdf",
              filepath="/tmp/b.pdf", group="tenant-b")
    db.log_audit(workflow_id="wf-aud-a", document_id="", action_type="page_edit", entity_type="document")
    db.log_audit(workflow_id="wf-aud-b", document_id="", action_type="page_edit", entity_type="document")
    db.purge_document("wf-aud-a", keep_audit=True)

    a_rows = db.get_all_audit_logs(groups=["tenant-a"])
    assert [r["workflow_id"] for r in a_rows if r["action_type"] == "page_edit"] == ["wf-aud-a"]
    b_rows = db.get_all_audit_logs(groups=["tenant-b"])
    assert [r["workflow_id"] for r in b_rows if r["action_type"] == "page_edit"] == ["wf-aud-b"]
    assert db.get_all_audit_log_count(groups=["tenant-a"]) == len(a_rows)
    assert db.get_all_audit_logs(groups=[]) == []


def test_user_group_follows_the_first_tenant_in_the_token(db_connection, caplog):
    db = db_connection

    def group_of(keycloak_id):
        with db.get_connection() as conn:
            row = conn.execute(
                "SELECT g.name FROM users u LEFT JOIN groups g ON g.id = u.group_id "
                "WHERE u.keycloak_user_id = %s", (keycloak_id,)).fetchone()
        return row["name"]

    db.upsert_user("kc-single", "single", "single@example.com", ["contributor"], tenants=["MH"])
    assert group_of("kc-single") == "mh"

    with caplog.at_level(logging.WARNING, logger="pipeline.db"):
        db.upsert_user("kc-multi", "multi", "multi@example.com", ["contributor"], tenants=["up", "mh"])
    assert group_of("kc-multi") == "mh"  # sorted, first wins
    assert any("several tenants" in rec.message for rec in caplog.records)

    # No tenants in the token leaves an existing group alone.
    db.upsert_user("kc-single", "single", "single@example.com", ["contributor"])
    assert group_of("kc-single") == "mh"
    db.upsert_user("kc-none", "none", "none@example.com", ["contributor"])
    assert group_of("kc-none") is None


def test_group_path_fills_the_keycloak_id_on_sync(db_connection):
    """The SQLite move creates groups without a Keycloak id; a later sync matches by path."""
    db = db_connection
    first = db.upsert_group("/states/MH")
    with db.get_connection() as conn:
        assert conn.execute("SELECT keycloak_group_id FROM groups WHERE id = %s::uuid",
                            (first,)).fetchone()["keycloak_group_id"] is None
    again = db.upsert_group("/states/MH", keycloak_group_id="kc-group-123")
    assert again == first
    with db.get_connection() as conn:
        row = conn.execute("SELECT name, keycloak_group_id FROM groups WHERE id = %s::uuid", (first,)).fetchone()
    assert (row["name"], row["keycloak_group_id"]) == ("mh", "kc-group-123")
    # A sync without an id never erases one already stored.
    assert db.upsert_group("/states/MH") == first
    with db.get_connection() as conn:
        assert conn.execute("SELECT keycloak_group_id FROM groups WHERE id = %s::uuid",
                            (first,)).fetchone()["keycloak_group_id"] == "kc-group-123"
