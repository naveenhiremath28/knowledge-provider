import pytest

from pipeline.network_constants import (
    KIND_TO_CATALOG_TYPE,
    catalog_type_for_kind,
    kind_for_catalog_type,
)


@pytest.mark.parametrize("kind", ["scheme", "advisory", "video"])
def test_every_kind_round_trips_through_its_catalog_type(kind):
    assert kind_for_catalog_type(catalog_type_for_kind(kind)) == kind


def test_each_kind_has_its_own_catalog_type():
    assert len(set(KIND_TO_CATALOG_TYPE.values())) == len(KIND_TO_CATALOG_TYPE)


def test_video_is_a_known_kind():
    assert catalog_type_for_kind(" Video ") == "openagrinet:VideoResource"


def test_unknown_kind_is_rejected():
    with pytest.raises(ValueError):
        catalog_type_for_kind("custom")
