"""Tests for the webhook_records indexing state (indexed_hash / promoted_hash) helpers."""


import pytest


def _rec(content_id, title="Wheat Insect Management", published=1, status="Published"):
    return {
        "contentID": content_id,
        "title": title,
        "status": status,
        "isPublished": published,
        "domainName": "Agriculture",
        "slug": f"{content_id}/x",
        "popList": [{"popID": 1, "status": "Published"}],
    }


def _row_id(db, content_id):
    with db.get_connection() as conn:
        return str(conn.execute(
            "SELECT id FROM webhook_records WHERE external_id = %s", (str(content_id),)
        ).fetchone()["id"])


@pytest.mark.unit
@pytest.mark.db
class TestIndexState:
    def test_new_rows_are_unindexed_until_marked(self, db_connection):
        db = db_connection
        db.upsert_webhook_records("webhook", [_rec(1)])
        rid = _row_id(db, 1)
        assert db.get_unindexed_webhook_record_ids() == [rid]
        row = db.get_webhook_record(rid)
        db.mark_webhook_record_indexed(rid, row["payload_hash"])
        assert db.get_unindexed_webhook_record_ids() == []

    def test_changed_payload_becomes_unindexed_again(self, db_connection):
        db = db_connection
        db.upsert_webhook_records("webhook", [_rec(1)])
        rid = _row_id(db, 1)
        db.mark_webhook_record_indexed(rid, db.get_webhook_record(rid)["payload_hash"])

        db.upsert_webhook_records("webhook", [_rec(1)])  # identical
        assert db.get_unindexed_webhook_record_ids() == []

        db.upsert_webhook_records("webhook", [_rec(1, title="Changed")])
        assert db.get_unindexed_webhook_record_ids() == [rid]

    def test_marking_a_stale_hash_leaves_the_row_unindexed(self, db_connection):
        db = db_connection
        db.upsert_webhook_records("webhook", [_rec(1)])
        rid = _row_id(db, 1)
        stale = db.get_webhook_record(rid)["payload_hash"]
        db.upsert_webhook_records("webhook", [_rec(1, title="Changed")])
        db.mark_webhook_record_indexed(rid, stale)
        assert db.get_unindexed_webhook_record_ids() == [rid]

    def test_unindexed_ids_can_be_scoped_to_external_ids(self, db_connection):
        db = db_connection
        db.upsert_webhook_records("webhook", [_rec(1), _rec(2), _rec(3)])
        ids = db.get_unindexed_webhook_record_ids(external_ids=["1", "3"])
        assert sorted(ids) == sorted([_row_id(db, 1), _row_id(db, 3)])
        assert db.get_unindexed_webhook_record_ids(external_ids=[]) == []

    def test_get_webhook_record_parses_payload(self, db_connection):
        db = db_connection
        db.upsert_webhook_records("webhook", [_rec(101)])
        row = db.get_webhook_record(_row_id(db, 101))
        assert row["payload"]["contentID"] == 101
        assert row["external_id"] == "101"
        assert row["is_published"] == 1
        assert row["status"] == "Published"
        assert db.get_webhook_record("missing") is None

    def test_webhook_external_id_matches_upsert_rule(self, db_connection):
        db = db_connection
        assert db.webhook_external_id({"contentID": 7}) == "7"
        assert db.webhook_external_id({"id": "abc"}) == "abc"
        assert db.webhook_external_id({"slug": "s"}) == "s"
        assert len(db.webhook_external_id({"title": "x"})) == 64  # payload hash

    def test_record_id_lists(self, db_connection):
        db = db_connection
        db.upsert_webhook_records(
            "webhook", [_rec(1), _rec(2, published=0, status="Draft")]
        )
        assert len(db.get_webhook_record_ids()) == 2
        assert db.get_webhook_record_ids(published_only=True) == [_row_id(db, 1)]

    def test_status_counts(self, db_connection):
        db = db_connection
        db.upsert_webhook_records(
            "webhook", [_rec(1), _rec(2), _rec(3, published=0, status="Draft")]
        )
        a, b = _row_id(db, 1), _row_id(db, 2)
        db.mark_webhook_record_indexed(a, db.get_webhook_record(a)["payload_hash"])
        db.mark_webhook_record_promoted(a, db.get_webhook_record(a)["payload_hash"])
        counts = db.advisory_status_counts()
        assert counts == {
            "total": 3,
            "published": 2,
            "indexed": 1,
            "unindexed": 2,
            "promoted": 1,
        }
        assert b  # silence unused
