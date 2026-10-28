"""Fixed values sent on the network. v2 replaces the per-kind ones with AI-derived values."""

BECKN_VERSION = "2.0.0"

# Per-catalog result status in an on_publish body; a 200 is only an ACK.
RESULT_ACCEPTED = "ACCEPTED"
RESULT_REJECTED = "REJECTED"

SCHEMA_CONTEXT_BASE = "https://openagrinet.github.io/network-specs/schema"

# Permanent: updateMode MERGE upserts resources by id, so a changed id strands
# the old resource on the network instead of replacing it.
ADVISORY_CATALOG_ID = "cat-oan-knowledge-provider-advisories"
ADVISORY_RESOURCE_ID = "res-oan-knowledge-provider-advisories"
SCHEMES_CATALOG_ID = "cat-oan-knowledge-provider-schemes"
SCHEMES_RESOURCE_ID = "res-oan-knowledge-provider-schemes"

ADVISORY_CATALOG_NAME = "Agricultural advisory from documents"
ADVISORY_RESOURCE_NAME = "Agricultural advisory"
SCHEMES_CATALOG_NAME = "Schemes from documents"
SCHEMES_RESOURCE_NAME = "Government schemes"

# OnDemand means "invoke me for the specifics", and the schema forbids the
# fields that would carry them.
INFORMATION_MODE = "OnDemand"

SERVED_LANGUAGES = ["en"]

ADVISORY_SUBJECT_CATEGORIES = ["Crop"]
ADVISORY_TOPICS = ["Crop production"]
SCHEME_SUBJECT_CATEGORIES = ["Scheme"]
SCHEME_TOPICS = ["Government schemes"]

DEFAULT_DOCUMENT_KIND = "document"

# Knowledge kinds the pipeline serves, and the network schema each maps to.
# ``catalogs.type`` stores the schema name; code asks this map what a type means
# instead of comparing ``openagrinet:*`` strings. The scheme mapping is a
# placeholder (ADR 0003 types schemes as KnowledgeAdvisory) - change it here only.
KIND_SCHEME = "scheme"
KIND_ADVISORY = "advisory"
KIND_TO_CATALOG_TYPE = {
    KIND_SCHEME: "openagrinet:AgricultureResource",
    KIND_ADVISORY: "openagrinet:KnowledgeAdvisory",
}
CATALOG_TYPE_TO_KIND = {schema: kind for kind, schema in KIND_TO_CATALOG_TYPE.items()}


def catalog_type_for_kind(kind: str) -> str:
    """``catalogs.type`` for a knowledge kind; ValueError for an unknown kind."""
    try:
        return KIND_TO_CATALOG_TYPE[(kind or "").strip().lower()]
    except KeyError:
        raise ValueError(
            f"Unknown document kind {kind!r}. Use one of: {', '.join(KIND_TO_CATALOG_TYPE)}"
        ) from None


def kind_for_catalog_type(catalog_type: str) -> str:
    """Knowledge kind a ``catalogs.type`` stands for; ValueError for an unknown type."""
    try:
        return CATALOG_TYPE_TO_KIND[catalog_type]
    except KeyError:
        raise ValueError(f"Unknown catalog type {catalog_type!r}") from None

# Same provider on every catalog - this pipeline publishes as one provider
# regardless of knowledge kind.
CATALOG_PROVIDER = {
    "id": "knowledge-provider",
    "descriptor": {
        "code": "oan-knowledge-provider",
        "name": "OAN Knowledge Provider",
    },
}
