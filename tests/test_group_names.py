"""Tests for group (tenant) code → state name resolution."""

import pytest


class TestGroupDisplayName:
    @pytest.mark.unit
    @pytest.mark.parametrize(
        "code,expected",
        [
            ("bv", "Bharat Vistaar"),
            ("BV", "Bharat Vistaar"),
            ("mh", "Maharashtra"),
            ("ka", "Karnataka"),
            ("up", "Uttar Pradesh"),
            ("gj", "Gujarat"),
            ("tn", "Tamil Nadu"),
            ("wb", "West Bengal"),
            ("dl", "Delhi"),
            ("jk", "Jammu and Kashmir"),
            ("default", "Default"),
        ],
    )
    def test_known_codes(self, code, expected):
        from pipeline.group_names import group_display_name

        assert group_display_name(code) == expected

    @pytest.mark.unit
    @pytest.mark.parametrize("code,expected", [("or", "Odisha"), ("od", "Odisha"), ("ts", "Telangana"), ("tg", "Telangana")])
    def test_legacy_code_aliases(self, code, expected):
        from pipeline.group_names import group_display_name

        assert group_display_name(code) == expected

    @pytest.mark.unit
    def test_blank_falls_back_to_default(self):
        from pipeline.group_names import group_display_name

        assert group_display_name("") == "Default"
        assert group_display_name(None) == "Default"

    @pytest.mark.unit
    def test_unknown_code_is_made_presentable(self):
        """A new state must degrade to a readable label, never break ingestion."""
        from pipeline.group_names import group_display_name

        assert group_display_name("new-state") == "New State"


class TestProdStageDisabled:
    """DISABLE_PROD_SETTING — turns the PROD half of the pipeline off."""

    @pytest.mark.unit
    @pytest.mark.parametrize("value", ["true", "TRUE", "True", "1", "yes", "YES"])
    def test_truthy_values_disable_prod(self, monkeypatch, value):
        from pipeline.group_names import prod_stage_disabled

        monkeypatch.setenv("DISABLE_PROD_SETTING", value)
        assert prod_stage_disabled() is True

    @pytest.mark.unit
    @pytest.mark.parametrize("value", ["false", "FALSE", "0", "no", "", "anything-else"])
    def test_other_values_keep_prod_enabled(self, monkeypatch, value):
        from pipeline.group_names import prod_stage_disabled

        monkeypatch.setenv("DISABLE_PROD_SETTING", value)
        assert prod_stage_disabled() is False

    @pytest.mark.unit
    def test_defaults_to_enabled_when_unset(self, monkeypatch):
        """Absent flag must keep the full pipeline — never silently skip PROD."""
        from pipeline.group_names import prod_stage_disabled

        monkeypatch.delenv("DISABLE_PROD_SETTING", raising=False)
        assert prod_stage_disabled() is False

    @pytest.mark.unit
    def test_whitespace_is_tolerated(self, monkeypatch):
        from pipeline.group_names import prod_stage_disabled

        monkeypatch.setenv("DISABLE_PROD_SETTING", "  true  ")
        assert prod_stage_disabled() is True


class TestProdOnlyStages:
    @pytest.mark.unit
    def test_prod_only_stages_are_the_three_promotion_stages(self):
        from pipeline.models import PIPELINE_STAGES, PROD_ONLY_STAGES

        assert PROD_ONLY_STAGES == {"approval_for_prod", "ingesting_prod", "publishing_to_network"}
        # All three must still exist in the canonical order, or filtering is a no-op.
        stage_ids = {s[0] for s in PIPELINE_STAGES}
        assert PROD_ONLY_STAGES <= stage_ids

    @pytest.mark.unit
    def test_filtering_leaves_a_pipeline_ending_at_completed(self):
        from pipeline.models import PIPELINE_STAGES, PROD_ONLY_STAGES

        remaining = [s[0] for s in PIPELINE_STAGES if s[0] not in PROD_ONLY_STAGES]

        assert remaining[-1] == "completed"
        assert remaining[-2] == "ingesting"  # DEV ingest flows straight to completed when PROD is disabled

    @pytest.mark.unit
    def test_publishing_to_network_is_prod_only(self):
        """It must never run without a successful PROD promotion (see ADR 0004)."""
        from pipeline.models import PIPELINE_STAGES, PROD_ONLY_STAGES

        assert "publishing_to_network" in PROD_ONLY_STAGES

        stage_ids = [s[0] for s in PIPELINE_STAGES]
        assert stage_ids.index("publishing_to_network") == stage_ids.index("ingesting_prod") + 1
        assert stage_ids.index("publishing_to_network") == stage_ids.index("completed") - 1


@pytest.mark.unit
@pytest.mark.parametrize(
    "value,expected",
    [(None, False), ("", False), ("false", False), ("0", False),
     ("true", True), ("TRUE", True), (" 1 ", True), ("yes", True)],
)
def test_publish_to_network_enabled(monkeypatch, value, expected):
    from pipeline.group_names import publish_to_network_enabled

    if value is None:
        monkeypatch.delenv("PUBLISH_TO_NETWORK", raising=False)
    else:
        monkeypatch.setenv("PUBLISH_TO_NETWORK", value)
    assert publish_to_network_enabled() is expected
