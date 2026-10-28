"""
End-to-end check of document validity against a live Qdrant.

Run against a local stack:  uv run python scripts/verify_document_validity_e2e.py
Needs Qdrant on :6333, the small embedding model cached, and a scratch Postgres
(E2E_DATABASE_URL, migrations are applied to it). It creates and deletes its own
scratch collection and four documents named wf-*, and touches neither the real
index nor the real db.

Real path, no stubs on the parts that matter: real Postgres rows, the real
_prepare_records + QdrantVectorStore.upsert the ingest activity calls, real
embeddings, a real Qdrant collection, and the real search filter. Only the
embedding *model* is swapped for a small cached one so this does not download
2GB to prove a date comparison.
"""

import os
import sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

COLLECTION = "e2e_validity_check"

if not os.environ.get("E2E_DATABASE_URL"):
    sys.exit("Set E2E_DATABASE_URL to a scratch Postgres database (it is never the real one).")
os.environ["DATABASE_URL"] = os.environ["E2E_DATABASE_URL"]
os.environ["AUTH_DISABLED"] = "true"
os.environ["VECTOR_DB_URL"] = "http://localhost:6333"
os.environ["VECTOR_DB_COLLECTION_NAME"] = COLLECTION
os.environ["EMBEDDING_PROVIDER"] = "sentence_transformers"
os.environ["EMBEDDING_MODEL"] = "sentence-transformers/all-MiniLM-L6-v2"
os.environ["EMBEDDING_VECTOR_SIZE"] = "384"
os.environ["EMBEDDING_DIM"] = "384"
os.environ["HF_HUB_OFFLINE"] = "1"

from datetime import datetime  # noqa: E402

from pipeline import db, document_validity  # noqa: E402
from pipeline.activities import _prepare_records, _validity_fields_from_doc  # noqa: E402
from pipeline.vector_store.qdrant_store import QdrantVectorStore  # noqa: E402

db.init_db()
SCRATCH_DOCUMENTS = ("wf-active", "wf-expired", "wf-future", "wf-legacy")
for scratch in SCRATCH_DOCUMENTS:
    db.purge_document(scratch, keep_audit=False)

PASS, FAIL = [], []


def check(label, actual, expected):
    if actual == expected:
        PASS.append(label)
        print(f"  PASS  {label}")
    else:
        FAIL.append(label)
        print(f"  FAIL  {label}\n          expected {expected}\n          actual   {actual}")


def clock_at(stamp):
    return lambda: datetime.strptime(stamp, "%Y-%m-%d")


def make_document(workflow_id, text, valid_from=None, valid_to=None, legacy=False):
    # Stands in for the upload endpoint, which is what decides the period a new
    # document starts with — db.py stores what it is given and defaults nothing.
    if not legacy and valid_from is None and valid_to is None:
        default = document_validity.default_period()
        valid_from, valid_to = default.start_date, default.end_date  # end is None
    db.upsert_document(
        workflow_id=workflow_id,
        document_id=workflow_id,
        filename=f"{workflow_id}.pdf",
        filepath=f"/books/{workflow_id}.pdf",
        stage="chunk_review",
        valid_from=valid_from,
        valid_to=valid_to,
        group="bv",
        kind="scheme",
        code=workflow_id,
    )
    if legacy:
        # A document that predates the columns: NULL, as the migration leaves it.
        db.set_document_validity(workflow_id, None, None)
    doc = db.get_document(workflow_id)
    chunks = [{"chunk_number": 1, "original_text": text, "token_count": 8, "is_excluded": False}]
    kwargs = {} if legacy else _validity_fields_from_doc(doc)
    return _prepare_records(
        document_id=workflow_id,
        filename=f"{workflow_id}.pdf",
        chunks=chunks,
        workflow_id=workflow_id,
        group="bv",
        **kwargs,
    )


print("\n=== 1. ingest: four documents, one per validity state ===")
records = []
records += make_document("wf-active", "Kisan credit card subsidy for active season")
records += make_document("wf-expired", "Kisan credit card subsidy expired scheme", "2024-01-01", "2025-01-01")
records += make_document("wf-future", "Kisan credit card subsidy future scheme", "2027-01-01", "2028-01-01")
records += make_document("wf-legacy", "Kisan credit card subsidy legacy document", legacy=True)

by_doc = {r["doc_id"]: r for r in records}
today = db.get_document("wf-active")["valid_from"]
check("upload stamps today as the default start", by_doc["wf-active"]["start_date"], today)
check(
    "upload leaves the default end unset (never expires)",
    "end_date" in by_doc["wf-active"],
    False,
)
check(
    "and stores NULL for it",
    db.get_document("wf-active")["valid_to"],
    None,
)
check("approver-set period reaches the payload", by_doc["wf-expired"]["start_date"], "2024-01-01")
check("legacy document carries no start_date", "start_date" in by_doc["wf-legacy"], False)
check("legacy document carries no end_date", "end_date" in by_doc["wf-legacy"], False)

store = QdrantVectorStore()
store.ensure_collection(COLLECTION, recreate=True)
result = store.upsert(COLLECTION, records, batch_size=8)
check("all four chunks upserted", result["records_ingested"], 4)

info = store.client.get_collection(COLLECTION)
indexed = set((info.payload_schema or {}).keys())
check("start_date is indexed in Qdrant", "start_date" in indexed, True)
check("end_date is indexed in Qdrant", "end_date" in indexed, True)


def search_docs(**kwargs):
    hits = store.search(COLLECTION, "kisan credit card subsidy", limit=10, **kwargs)["hits"]
    return sorted(h["doc_id"] for h in hits)


print("\n=== 2. search today (real clock) ===")
check(
    "today returns the active and the undated document only",
    search_docs(),
    ["wf-active", "wf-legacy"],
)

print("\n=== 3. search with an injected clock ===")
# Inside the future document's window. The active document is open-ended, so
# it is still live here too — only the expired one has dropped out.
future_store = QdrantVectorStore(client=store.client, clock=clock_at("2027-12-01"))
check(
    "in Dec 2027 the future document has started and the expired one is gone",
    sorted(h["doc_id"] for h in future_store.search(COLLECTION, "kisan credit card subsidy", limit=10)["hits"]),
    ["wf-active", "wf-future", "wf-legacy"],
)
past_store = QdrantVectorStore(client=store.client, clock=clock_at("2024-06-01"))
check(
    "in 2024 only the expired document was live",
    sorted(h["doc_id"] for h in past_store.search(COLLECTION, "kisan credit card subsidy", limit=10)["hits"]),
    ["wf-expired", "wf-legacy"],
)

print("\n=== 4. boundaries are inclusive ===")
check(
    "live on its own first day",
    sorted(h["doc_id"] for h in QdrantVectorStore(client=store.client, clock=clock_at("2024-01-01")).search(COLLECTION, "kisan credit card subsidy", limit=10)["hits"]),
    ["wf-expired", "wf-legacy"],
)
check(
    "live on its own last day",
    sorted(h["doc_id"] for h in QdrantVectorStore(client=store.client, clock=clock_at("2025-01-01")).search(COLLECTION, "kisan credit card subsidy", limit=10)["hits"]),
    ["wf-expired", "wf-legacy"],
)
check(
    "gone the day after it ends",
    sorted(h["doc_id"] for h in QdrantVectorStore(client=store.client, clock=clock_at("2025-01-02")).search(COLLECTION, "kisan credit card subsidy", limit=10)["hits"]),
    ["wf-legacy"],
)
check(
    "absent the day before it starts",
    sorted(h["doc_id"] for h in QdrantVectorStore(client=store.client, clock=clock_at("2023-12-31")).search(COLLECTION, "kisan credit card subsidy", limit=10)["hits"]),
    ["wf-legacy"],
)

print("\n=== 4b. an open-ended document never expires ===")
for far_future in ("2030-01-01", "2099-12-31", "9999-12-31"):
    check(
        f"still searchable on {far_future}",
        sorted(h["doc_id"] for h in QdrantVectorStore(
            client=store.client, clock=clock_at(far_future)
        ).search(COLLECTION, "kisan credit card subsidy", limit=10)["hits"]),
        ["wf-active", "wf-legacy"],
    )
check(
    "but not before its start day",
    sorted(h["doc_id"] for h in QdrantVectorStore(
        client=store.client, clock=clock_at("2020-01-01")
    ).search(COLLECTION, "kisan credit card subsidy", limit=10)["hits"]),
    ["wf-legacy"],
)

print("\n=== 5. operator overrides ===")
check(
    "valid_on answers for another day",
    search_docs(valid_on="2027-12-01"),
    ["wf-active", "wf-future", "wf-legacy"],
)
check(
    "include_expired returns everything",
    search_docs(apply_validity=False),
    ["wf-active", "wf-expired", "wf-future", "wf-legacy"],
)

print("\n=== 6. lexical mode filters too ===")
check(
    "LEXICAL mode applies the same period filter",
    sorted(h["doc_id"] for h in store.search(COLLECTION, "kisan", limit=10, search_mode="LEXICAL")["hits"]),
    ["wf-active", "wf-legacy"],
)

print("\n=== 7. document-scoped reads ignore validity ===")
check(
    "an expired document's chunks are still listable",
    len(store.list_by_doc_id(COLLECTION, "wf-expired")),
    1,
)

print("\n=== 8. reingest after the approver shortens the period ===")
db.set_document_validity("wf-active", "2026-01-01", "2026-01-31")
doc = db.get_document("wf-active")
again = _prepare_records(
    document_id="wf-active",
    filename="wf-active.pdf",
    chunks=[{"chunk_number": 1, "original_text": "Kisan credit card subsidy for active season", "token_count": 8}],
    workflow_id="wf-active",
    group="bv",
    **_validity_fields_from_doc(doc),
)
store.upsert(COLLECTION, again, batch_size=8)
check("the shortened period takes it out of today's results", search_docs(), ["wf-legacy"])

store.client.delete_collection(COLLECTION)
for scratch in SCRATCH_DOCUMENTS:
    db.purge_document(scratch, keep_audit=False)
print(f"\n{len(PASS)} passed, {len(FAIL)} failed")
sys.exit(1 if FAIL else 0)
