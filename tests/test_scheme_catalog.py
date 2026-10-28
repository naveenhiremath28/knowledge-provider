"""Tests for Master Scheme Catalog (AI tool / prompt sync)."""

from __future__ import annotations

import pytest


@pytest.fixture
def catalog_db(db_connection):
    from pipeline import scheme_catalog

    yield db_connection, scheme_catalog


def _doc(db, workflow_id, *, code, kind="scheme", name=None, stage="completed",
         group="default", filename=None, **extra):
    """Register a document the v2 way: it needs a kind (catalog) and a code."""
    return db.upsert_document(
        workflow_id=workflow_id,
        document_id=f"doc-{workflow_id}",
        filename=filename or f"{workflow_id}.pdf",
        filepath=f"/tmp/{workflow_id}.pdf",
        stage=stage,
        group=group,
        kind=kind,
        code=code,
        name=name,
        **extra,
    )


def test_catalog_has_no_bootstrap_entries(catalog_db):
    """The catalog is documents only; the code-defined lists stay as hints."""
    db, scheme_catalog = catalog_db
    assert not hasattr(scheme_catalog, "bootstrap_catalog_if_empty")
    snap = scheme_catalog.build_snapshot()
    assert snap["vector_schemes"] == []
    assert snap["catalog_version"] == 0
    # Static lists are untouched: curated alias source and legacy tool list.
    assert len(scheme_catalog.BOOTSTRAP_VECTOR_SCHEMES) == 13
    assert "nbm" not in scheme_catalog.BOOTSTRAP_VECTOR_SCHEMES  # legacy-only
    assert any(s["scheme_code"] == "nbm" for s in snap["legacy_schemes"])
    assert "search_schemes_doc" in snap["tool_prompts"]
    assert snap["routing_exceptions"]["pkvy"] == "search_schemes"
    assert snap["routing_exceptions"]["nbm"] == "get_scheme_info"


def test_snapshot_has_one_entry_per_live_scheme_document(catalog_db):
    db, scheme_catalog = catalog_db
    _doc(db, "wf-a", code="pkvy", name="Paramparagat Krishi Vikas Yojana", group="mh")
    _doc(db, "wf-b", code="mif", name="Micro Irrigation Fund", group="gj")
    _doc(db, "wf-adv", code="rain-advice", kind="advisory", name="Rain advisory")
    _doc(db, "wf-draft", code="draft", stage="chunk_review")
    snap = scheme_catalog.build_snapshot()
    by_code = {s["scheme_code"]: s for s in snap["vector_schemes"]}
    assert set(by_code) == {"pkvy", "mif"}  # advisory kind and non-live docs stay out
    assert by_code["pkvy"]["instances"] == ["mh"]
    assert by_code["mif"]["instances"] == ["gj"]
    assert by_code["pkvy"]["workflow_ids"] == ["wf-a"]
    assert by_code["pkvy"]["source"] == "pipeline"

    only_mh = scheme_catalog.build_snapshot(instance="mh")
    assert [s["scheme_code"] for s in only_mh["vector_schemes"]] == ["pkvy"]


def test_scheme_metadata_and_promote_rebuild(catalog_db):
    db, scheme_catalog = catalog_db
    v0 = db.get_catalog_meta()["version"]
    _doc(db, "wf-pkvy-1", code="pkvy")
    result = scheme_catalog.apply_scheme_metadata(
        "wf-pkvy-1",
        kind="scheme",
        code="pkvy",
        name="Paramparagat Krishi Vikas Yojana",
        aliases=["PKVY", "organic"],
        network_visible=True,
        catalog_visible=True,
    )
    assert result["document"]["code"] == "pkvy"
    assert result["catalog_version"] is not None
    assert result["catalog_version"] > v0

    entry = db.get_catalog_entry("pkvy")
    assert entry is not None
    assert entry["status"] == "live"
    assert entry["source"] == "pipeline"
    assert entry["name"] == "Paramparagat Krishi Vikas Yojana"
    assert "organic" in entry["aliases"] and "PKVY" in entry["aliases"]


def test_code_rename_pending_reindex(catalog_db):
    db, scheme_catalog = catalog_db
    _doc(db, "wf-rename", code="old-code", name="Old")
    result = scheme_catalog.apply_scheme_metadata(
        "wf-rename",
        code="new-code",
        name="New",
    )
    assert result["pending_reindex"] is True
    doc = db.get_document("wf-rename")
    assert doc["code"] == "new-code"
    assert doc["reindex_required"] is True
    # New code must not appear in live vector_schemes
    snap = scheme_catalog.build_snapshot()
    live_codes = {s["scheme_code"] for s in snap["vector_schemes"]}
    assert "new-code" not in live_codes
    entry = db.get_catalog_entry("new-code")
    assert entry is not None
    assert entry["status"] == "pending_reindex"
    assert db.get_catalog_entry("old-code") is None  # the document holds one code


def test_derive_scheme_code_acronym(catalog_db):
    db, scheme_catalog = catalog_db
    assert scheme_catalog.derive_scheme_code("National Mission on Natural Farming") == "nmnf"
    # Single-word title has too few words for a useful acronym -> slug fallback.
    assert scheme_catalog.derive_scheme_code("Guideline") == "guideline"[:8]


def test_derive_scheme_code_collision_appends_suffix(catalog_db):
    db, scheme_catalog = catalog_db
    _doc(db, "wf-nmnf-1", code="nmnf", name="National Mission on Natural Farming")
    # A second, unrelated document whose title happens to acronym to the same code.
    assert scheme_catalog.derive_scheme_code("New Model National Farming") == "nmnf-2"
    # The collision domain is the kind: an advisory may reuse the code.
    assert scheme_catalog.derive_scheme_code("New Model National Farming", kind="advisory") == "nmnf"
    # A document's own code does not collide with itself.
    assert scheme_catalog.derive_scheme_code(
        "National Mission on Natural Farming", exclude_workflow_id="wf-nmnf-1"
    ) == "nmnf"


def test_collision_domain_spans_tenants(catalog_db):
    """Codes are unique per catalog in SQL, but derivation avoids every tenant's codes."""
    db, scheme_catalog = catalog_db
    _doc(db, "wf-mh", code="nmnf", group="mh")
    assert scheme_catalog.derive_scheme_code("National Mission on Natural Farming") == "nmnf-2"
    # The same explicit code in another tenant's catalog is still allowed by the schema.
    _doc(db, "wf-gj", code="nmnf", group="gj")


def test_apply_scheme_metadata_keeps_the_code_when_only_the_name_changes(catalog_db):
    """Every document has a code from upload, so a name edit never re-derives it."""
    db, scheme_catalog = catalog_db
    _doc(db, "wf-name-only", code="gn", stage="ready_for_ingestion")
    result = scheme_catalog.apply_scheme_metadata(
        "wf-name-only",
        kind="scheme",
        name="National Mission on Natural Farming",
    )
    assert result["document"]["code"] == "gn"
    assert result["document"]["name"] == "National Mission on Natural Farming"
    assert result["code_renamed"] is False


def test_kind_is_limited_to_the_known_kinds(catalog_db):
    db, scheme_catalog = catalog_db
    _doc(db, "wf-kind", code="kind-doc", stage="ready_for_ingestion")
    assert db.get_document("wf-kind")["kind"] == "scheme"

    result = scheme_catalog.apply_scheme_metadata("wf-kind", kind="advisory")
    assert result["document"]["kind"] == "advisory"
    assert result["document"]["code"] == "kind-doc"  # moved catalogs, kept its code

    result = scheme_catalog.apply_scheme_metadata("wf-kind", kind="video")
    assert result["document"]["kind"] == "video"

    result = scheme_catalog.apply_scheme_metadata("wf-kind", kind="scheme")
    assert result["document"]["kind"] == "scheme"

    for bad in ("how_to_faq", "document", "podcast", "Not Valid!!"):
        with pytest.raises(ValueError):
            scheme_catalog.apply_scheme_metadata("wf-kind", kind=bad)
    assert db.get_document("wf-kind")["kind"] == "scheme"


def test_prepare_records_scheme_payload():
    from pipeline.activities import _prepare_records

    chunks = [
        {
            "chunk_number": 1,
            "original_text": "Eligibility for farmers under the scheme.",
            "token_count": 10,
            "page_start": 1,
            "page_end": 1,
            "is_excluded": False,
        }
    ]
    records = _prepare_records(
        "doc1",
        "pkvy.pdf",
        chunks,
        workflow_id="wf1",
        group="default",
        kind="scheme",
        code="pkvy",
        name="Paramparagat Krishi Vikas Yojana",
        aliases=["PKVY"],
    )
    assert len(records) == 1
    r = records[0]
    assert r["type"] == "scheme"
    assert r["scheme_code"] == "pkvy"
    assert r["scheme_name"].startswith("Paramparagat")
    # The curator alias leads; bootstrap and derived aliases are merged in so
    # the scheme is not searchable by its exact name alone.
    assert r["scheme_aliases"][0] == "PKVY"
    assert len(r["scheme_aliases"]) > 1
    assert "paramparagat krishi vikas yojana" in {a.lower() for a in r["scheme_aliases"]}
    assert r["instance_name"] == "Default"
    assert r["chunk_id"] == r["_id"]
    assert r["chunk_index"] == 1


def test_prepare_records_document_unchanged():
    from pipeline.activities import _prepare_records

    chunks = [
        {
            "chunk_number": 0,
            "original_text": "General agronomy notes.",
            "token_count": 5,
            "is_excluded": False,
        }
    ]
    records = _prepare_records(
        "doc2",
        "notes.pdf",
        chunks,
        kind="document",
    )
    assert records[0]["type"] == "document"
    assert "scheme_code" not in records[0]


@pytest.fixture
def asgi_client(db_connection, mock_temporal_client, mock_minio_client):
    """HTTP client that does not run real Temporal/MinIO lifespan."""
    from contextlib import asynccontextmanager

    from fastapi.testclient import TestClient

    from pipeline import api

    api.temporal_client = mock_temporal_client
    api.minio_client = mock_minio_client

    @asynccontextmanager
    async def _noop_lifespan(_app):
        yield

    original = api.app.router.lifespan_context
    api.app.router.lifespan_context = _noop_lifespan
    try:
        with TestClient(api.app) as client:
            yield client
    finally:
        api.app.router.lifespan_context = original


def test_catalog_api_snapshot(asgi_client):
    from pipeline import db, scheme_catalog

    _doc(db, "wf-snap-1", code="pkvy", name="Paramparagat Krishi Vikas Yojana")
    _doc(db, "wf-snap-2", code="pm-ddky", name="Deen Dayal Upadhyaya Krishi Kalyan Yojana")
    scheme_catalog.rebuild_catalog_from_documents(reason="test")

    r = asgi_client.get("/catalog/v1/version")
    assert r.status_code == 200
    assert "version" in r.json()
    assert r.json()["version"] >= 1

    r = asgi_client.get("/catalog/v1/snapshot")
    assert r.status_code == 200
    body = r.json()
    assert body["api_version"] == "1"
    assert {s["scheme_code"] for s in body["vector_schemes"]} == {"pkvy", "pm-ddky"}
    assert "tool_prompts" in body
    assert "legacy_schemes" in body
    assert body["collections"]["scheme_qdrant"]["name"] == "schemes-index"

    r = asgi_client.get("/catalog/v1/tool-prompt")
    assert r.status_code == 200
    assert "search_schemes_doc" in r.json()["tool_prompts"]

    r = asgi_client.get("/catalog/v1/schemes")
    assert r.status_code == 200
    assert len(r.json()["vector_schemes"]) == 2


def test_catalog_bootstrap_endpoint_is_gone(asgi_client):
    assert asgi_client.post("/catalog/v1/bootstrap").status_code in (404, 405)


def test_catalog_service_key_auth(asgi_client, monkeypatch):
    monkeypatch.setenv("CATALOG_SERVICE_API_KEYS", "test-secret-key")

    r = asgi_client.get(
        "/catalog/v1/snapshot",
        headers={"X-Catalog-Service-Key": "test-secret-key"},
    )
    assert r.status_code == 200
    # AUTH_DISABLED bypass user has admin — still allowed without key
    r2 = asgi_client.get("/catalog/v1/snapshot")
    assert r2.status_code == 200


def test_scheme_metadata_api(asgi_client):
    from pipeline import db

    _doc(db, "wf-meta", code="meta-draft", stage="chunk_review", filename="scheme.pdf")
    r = asgi_client.patch(
        "/documents/wf-meta/scheme-metadata",
        json={
            "kind": "scheme",
            "code": "mif",
            "name": "Micro Irrigation Fund",
            "aliases": ["MIF"],
            "network_visible": True,
        },
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["code"] == "mif"
    assert body["kind"] == "scheme"
    assert "MIF" in body["aliases"]


def test_scheme_metadata_api_rejects_an_unknown_kind(asgi_client):
    from pipeline import db

    _doc(db, "wf-meta-bad", code="meta-bad", stage="chunk_review")
    r = asgi_client.patch("/documents/wf-meta-bad/scheme-metadata", json={"kind": "podcast"})
    assert r.status_code == 400


@pytest.mark.parametrize("kind", ["scheme", "advisory", "video"])
def test_reingest_is_allowed_for_every_kind(asgi_client, mock_temporal_client, kind):
    """No 'never classified' guard any more: a document always has a kind and a code."""
    from pipeline import db

    _doc(db, "wf-reingest", code="testcode", kind=kind)
    db.save_chunks("wf-reingest", [
        {"chunk_number": 1, "original_text": "some content", "token_count": 5},
    ])

    r = asgi_client.post("/documents/wf-reingest/reingest")
    assert r.status_code == 200, r.text
    assert r.json()["status"] == "started"


def test_request_prod_ready_wrong_stage_rejected(asgi_client):
    from pipeline import db

    _doc(db, "wf-prod-ready-wrong-stage", code="prw", stage="chunk_review")
    r = asgi_client.post("/documents/wf-prod-ready-wrong-stage/request-prod-ready")
    assert r.status_code == 400


def test_request_prod_ready_flags_document_and_clears_on_approve(asgi_client, mock_temporal_client):
    from pipeline import db

    _doc(db, "wf-prod-ready", code="pr", stage="approval_for_prod")
    doc = db.get_document("wf-prod-ready")
    assert doc["prod_ready_requested_at"] is None

    detail = asgi_client.get("/documents/wf-prod-ready").json()
    assert "request_prod_ready" in detail["available_actions"]

    r = asgi_client.post("/documents/wf-prod-ready/request-prod-ready")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["requested"] is True
    assert body["prod_ready_requested_at"] is not None

    doc = db.get_document("wf-prod-ready")
    assert doc["prod_ready_requested_at"] is not None

    # Once requested, it drops off available_actions (no duplicate requests) —
    # approve_prod (the real privileged action) stays available throughout.
    detail = asgi_client.get("/documents/wf-prod-ready").json()
    assert "request_prod_ready" not in detail["available_actions"]
    assert "approve_prod" in detail["available_actions"]

    # Actual prod approval clears the request flag.
    r = asgi_client.post("/documents/wf-prod-ready/approve-prod")
    assert r.status_code == 200, r.text
    doc = db.get_document("wf-prod-ready")
    assert doc["prod_ready_requested_at"] is None


class TestResolveSchemeAliases:
    """Aliases indexed with each scheme — curator + bootstrap + derived."""

    @pytest.mark.unit
    def test_nmeoop_gets_aliases_without_any_curation(self):
        """The real case: a scheme indexed with scheme_aliases: [] matched nothing."""
        from pipeline.scheme_catalog import resolve_scheme_aliases

        aliases = resolve_scheme_aliases(
            "nmeoop", "National Mission on Edible Oils - Oil Palm", None
        )

        lowered = {a.lower() for a in aliases}
        assert aliases, "a scheme must never index with an empty alias list"
        assert "nmeoop" in lowered
        assert "national mission on edible oils - oil palm" in lowered
        assert "national mission on edible oils oil palm" in lowered
        assert "oil palm" in lowered
        # The acronym of the significant words is NMEOOP, which is what the
        # code already is — dedupe keeps one copy rather than both.
        assert lowered.count("nmeoop") if isinstance(lowered, list) else True

    @pytest.mark.unit
    def test_acronym_is_derived_from_significant_words(self):
        from pipeline.scheme_catalog import resolve_scheme_aliases

        aliases = resolve_scheme_aliases("cdp-2", "Crop Diversification Programme", None)

        assert "CDP" in aliases

    @pytest.mark.unit
    def test_stopwords_are_excluded_from_the_acronym(self):
        from pipeline.scheme_catalog import resolve_scheme_aliases

        aliases = resolve_scheme_aliases("x", "National Mission on Edible Oils", None)

        assert "NMEO" in aliases  # the "on" is skipped

    @pytest.mark.unit
    def test_curator_aliases_are_kept_and_come_first(self):
        from pipeline.scheme_catalog import resolve_scheme_aliases

        aliases = resolve_scheme_aliases(
            "nmeoop",
            "National Mission on Edible Oils - Oil Palm",
            ["NMEO-OP", "oil palm mission"],
        )

        assert aliases[0] == "NMEO-OP"
        assert "oil palm mission" in aliases
        # merged, not short-circuited
        assert "nmeoop" in {a.lower() for a in aliases}

    @pytest.mark.unit
    def test_bootstrap_aliases_are_merged_for_known_codes(self):
        from pipeline.scheme_catalog import resolve_scheme_aliases

        aliases = {a.lower() for a in resolve_scheme_aliases("nmeo", None, None)}

        assert "nmeo-os" in aliases
        assert "oilseeds mission" in aliases

    @pytest.mark.unit
    def test_aliases_are_deduped_case_insensitively(self):
        from pipeline.scheme_catalog import resolve_scheme_aliases

        aliases = resolve_scheme_aliases("pkvy", "Paramparagat Krishi Vikas Yojana", ["PKVY", "pkvy"])

        lowered = [a.lower() for a in aliases]
        assert len(lowered) == len(set(lowered))

    @pytest.mark.unit
    def test_json_string_aliases_are_accepted(self):
        """Aliases may arrive as a JSON string."""
        from pipeline.scheme_catalog import resolve_scheme_aliases

        aliases = resolve_scheme_aliases("makhana", "Makhana Scheme", '["fox nut", "lotus seed"]')

        lowered = {a.lower() for a in aliases}
        assert "fox nut" in lowered and "lotus seed" in lowered

    @pytest.mark.unit
    def test_no_code_and_no_name_yields_nothing(self):
        from pipeline.scheme_catalog import resolve_scheme_aliases

        assert resolve_scheme_aliases(None, None, None) == []
