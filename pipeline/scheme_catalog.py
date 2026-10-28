"""
Master Scheme Catalog — source of truth for AI tool prompts and scheme lists.

Owned by docs-pipeline. Consumers (bharat-oan-api, bharat-provider-backend)
poll GET /catalog/v1/snapshot and cache the result.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from datetime import datetime, timezone
from typing import Any, Optional

from . import db, document_validity
from .network_constants import catalog_type_for_kind

SCHEME_CODE_RE = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")


_SCHEME_CODE_STOPWORDS = {
    "a", "an", "the", "of", "on", "for", "and", "or", "to", "in", "at", "by",
    "with", "&",
}
_SCHEME_CODE_WORD_RE = re.compile(r"[A-Za-z0-9]+")

# Prompt "vector-indexed" searchable set (13). Exclude nbm (legacy-only).
BOOTSTRAP_VECTOR_SCHEMES: dict[str, dict[str, Any]] = {
    "cdp": {
        "scheme_name": "Crop Diversification Programme",
        "scheme_aliases": [
            "CDP",
            "crop diversification",
            "crop diversification programme",
            "crop diversification program",
        ],
    },
    "cotton-mission": {
        "scheme_name": "Cotton Mission",
        "scheme_aliases": ["cotton mission", "cotton scheme", "nfsm cotton", "NFSM Cotton"],
    },
    "e-nam": {
        "scheme_name": "Electronic National Agriculture Market",
        "scheme_aliases": [
            "e-NAM",
            "eNAM",
            "enam",
            "national agriculture market",
            "electronic nam",
            "electronic national agriculture market",
        ],
    },
    "makhana": {
        "scheme_name": "Central Sector Scheme for Development of Makhana",
        "scheme_aliases": [
            "makhana",
            "makana",
            "foxnut",
            "horticulture makhana",
            "makhana scheme",
            "development of makhana",
        ],
    },
    "midh": {
        "scheme_name": "Mission for Integrated Development of Horticulture",
        "scheme_aliases": [
            "MIDH",
            "horticulture mission",
            "krishonnati horticulture",
            "national horticulture mission",
            "integrated development of horticulture",
        ],
    },
    "mif": {
        "scheme_name": "Micro Irrigation Fund",
        "scheme_aliases": ["MIF", "micro irrigation fund", "Micro Irrigation Fund scheme"],
    },
    "nmeo": {
        "scheme_name": "National Mission on Edible Oils – Oilseeds",
        "scheme_aliases": [
            "NMEO",
            "NMEO-OS",
            "NMEO OS",
            "oilseeds mission",
            "nmoe",
            "national mission on edible oils",
            "edible oils oilseeds",
        ],
    },
    "pkvy": {
        "scheme_name": "Paramparagat Krishi Vikas Yojana",
        "scheme_aliases": [
            "PKVY",
            "paramparagat krishi vikas yojana",
            "paramparagat krishi",
            "organic farming scheme",
        ],
    },
    "pm-ddky": {
        "scheme_name": "Prime Minister Dhan–Dhaanya Krishi Yojana",
        "scheme_aliases": [
            "PM-DDKY",
            "PM DDKY",
            "pm ddky",
            "dhan-dhaanya krishi yojana",
            "dhan dhaanya krishi yojana",
            "dhan-dhaanya",
            "dhan dhaanya",
            "PM Dhan Dhaanya Krishi Yojana",
        ],
    },
    "pm-kmy": {
        "scheme_name": "Pradhan Mantri Kisan Maandhan Yojana",
        "scheme_aliases": [
            "PM-KMY",
            "PMKMY",
            "pm kmy",
            "Kisan Maandhan Yojana",
            "Kisan Mandhan Yojana",
            "kisan mandhan",
            "PMKMY Yojana",
        ],
    },
    "pm-rkvy": {
        "scheme_name": "Pradhan Mantri Rashtriya Krishi Vikas Yojana",
        "scheme_aliases": [
            "PM-RKVY",
            "PM RKVY",
            "pm rkvy",
            "RKVY",
            "rashtriya krishi vikas",
            "rashtriya krishi vikas yojana",
        ],
    },
    "pulses-mission": {
        "scheme_name": "Mission for Aatmanirbharta in Pulses",
        "scheme_aliases": [
            "pulses mission",
            "pulses scheme",
            "nfsm pulses",
            "aatmanirbharta in pulses",
            "mission for aatmanirbharta in pulses",
            "aatmanirbharta pulses mission",
            "National Food Security Mission – Pulses",
        ],
    },
    "rwbcis": {
        "scheme_name": "Restructured Weather Based Crop Insurance Scheme",
        "scheme_aliases": [
            "RWBCIS",
            "weather based crop insurance",
            "weather insurance",
            "weather based crop insurance scheme",
        ],
    },
}

# Legacy integrated schemes (get_scheme_info / Hasura). Not zero-redeploy.
BOOTSTRAP_LEGACY_SCHEMES: list[dict[str, str]] = [
    {"scheme_code": "kcc", "scheme_name": "Kisan Credit Card scheme", "tool": "get_scheme_info", "tool_routing": "legacy"},
    {"scheme_code": "pmkisan", "scheme_name": "Pradhan Mantri Kisan Samman Nidhi scheme", "tool": "get_scheme_info", "tool_routing": "legacy"},
    {"scheme_code": "pmfby", "scheme_name": "Pradhan Mantri Fasal Bima Yojana scheme", "tool": "get_scheme_info", "tool_routing": "legacy"},
    {"scheme_code": "shc", "scheme_name": "Soil Health Card scheme", "tool": "get_scheme_info", "tool_routing": "legacy"},
    {"scheme_code": "pmksy", "scheme_name": "Pradhan Mantri Krishi Sinchayee Yojana", "tool": "get_scheme_info", "tool_routing": "legacy"},
    {"scheme_code": "sathi", "scheme_name": "SATHI Seed Authentication, Traceability & Holistic Inventory", "tool": "get_scheme_info", "tool_routing": "legacy"},
    {"scheme_code": "pmasha", "scheme_name": "Pradhan Mantri Annadata Aay Sanrakshan Abhiyan", "tool": "get_scheme_info", "tool_routing": "legacy"},
    {"scheme_code": "aif", "scheme_name": "Agriculture Infrastructure Fund", "tool": "get_scheme_info", "tool_routing": "legacy"},
    {"scheme_code": "smam", "scheme_name": "Sub-Mission on Agricultural Mechanization", "tool": "get_scheme_info", "tool_routing": "legacy"},
    {"scheme_code": "pdmc", "scheme_name": "Per Drop More Crop scheme", "tool": "get_scheme_info", "tool_routing": "legacy"},
    {"scheme_code": "pkvy", "scheme_name": "Paramparagat Krishi Vikas Yojana", "tool": "get_scheme_info", "tool_routing": "legacy"},
    {"scheme_code": "nfsm", "scheme_name": "National Food Security Mission", "tool": "get_scheme_info", "tool_routing": "legacy"},
    {"scheme_code": "rad", "scheme_name": "Rainfed Area Development", "tool": "get_scheme_info", "tool_routing": "legacy"},
    {"scheme_code": "ffs", "scheme_name": "Framework for Fertilizer Sales", "tool": "get_scheme_info", "tool_routing": "legacy"},
    {"scheme_code": "nbm", "scheme_name": "National Bamboo Mission", "tool": "get_scheme_info", "tool_routing": "legacy"},
    {"scheme_code": "nbhm", "scheme_name": "National Beekeeping & Honey Mission", "tool": "get_scheme_info", "tool_routing": "legacy"},
]

ROUTING_EXCEPTIONS: dict[str, str] = {
    "pkvy": "search_schemes",
    "nbm": "get_scheme_info",
}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def central_groups() -> set[str]:
    raw = (os.environ.get("CENTRAL_INSTANCES") or "default").strip()
    return {p.strip().lower() for p in raw.split(",") if p.strip()} or {"default"}


def default_network_visible(group: str | None) -> bool:
    code = (group or "default").strip().lower() or "default"
    return code in central_groups()


def scheme_collection_name() -> str:
    return (
        os.environ.get("PROD_SCHEME_VECTOR_DB_COLLECTION_NAME")
        or "schemes-index"
    ).strip()


def documents_collection_name() -> str:
    return (
        os.environ.get("PROD_VECTOR_DB_COLLECTION_NAME")
        or os.environ.get("VECTOR_DB_COLLECTION_NAME")
        or "documents-index"
    ).strip()


def validate_scheme_code(code: str) -> str:
    normalized = (code or "").strip().lower()
    if not normalized or not SCHEME_CODE_RE.match(normalized):
        raise ValueError(
            f"Invalid scheme_code '{code}'. Use lowercase slug: ^[a-z0-9]+(?:-[a-z0-9]+)*$"
        )
    return normalized


def _acronym_from_title(title: str) -> str:
    """First letter of each significant word, lowercase — 'National Mission on
    Natural Farming' -> 'nmnf' (skips the stopword 'on')."""
    words = [w for w in _SCHEME_CODE_WORD_RE.findall(title or "") if w.lower() not in _SCHEME_CODE_STOPWORDS]
    letters = "".join(w[0].lower() for w in words if w)
    return letters


def derive_scheme_code(
    title: str, exclude_workflow_id: Optional[str] = None, kind: str = "scheme"
) -> str:
    """Auto-generate a scheme_code from a document title: acronym of significant
    words, falling back to a truncated slug if the title has too few words to
    produce a useful acronym (e.g. a single-word title). Appends -2, -3, ... on
    collision against every code already in use for the kind (``documents.code``)
    so two documents of a kind never share a code across tenants.
    """
    base = _acronym_from_title(title)
    if len(base) < 2:
        slug = re.sub(r"[^a-z0-9]+", "", (title or "").strip().lower())
        base = slug[:8] or "scheme"

    used = db.list_used_scheme_codes(exclude_workflow_id=exclude_workflow_id, kind=kind)
    if base not in used:
        return base
    n = 2
    while f"{base}-{n}" in used:
        n += 1
    return f"{base}-{n}"


def parse_aliases(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(a).strip() for a in value if str(a).strip()]
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        try:
            parsed = json.loads(text)
            if isinstance(parsed, list):
                return [str(a).strip() for a in parsed if str(a).strip()]
        except json.JSONDecodeError:
            return [p.strip() for p in text.split(",") if p.strip()]
    return []


# Words that carry no distinguishing signal in a scheme name, so they are
# dropped when building an acronym or a short-form alias.
_ALIAS_STOPWORDS = {
    "of", "on", "for", "the", "and", "in", "to", "a", "an", "&",
}


def _derive_aliases(code: str, name: str) -> list[str]:
    """Build search aliases from a scheme's code and name.

    Used when a document carries no curated aliases: without this, a scheme
    indexes with ``scheme_aliases: []`` and only matches on its exact name, so
    a farmer asking about "oil palm mission" misses a document titled
    "National Mission on Edible Oils - Oil Palm".
    """
    derived: list[str] = []
    code = (code or "").strip().lower()
    name = (name or "").strip()

    if code:
        derived.append(code)

    if not name:
        return derived

    derived.append(name)
    derived.append(name.lower())

    # Punctuation-free form: "National Mission on Edible Oils - Oil Palm"
    # also matches "national mission on edible oils oil palm".
    flattened = re.sub(r"[^\w\s]", " ", name.lower())
    flattened = re.sub(r"\s+", " ", flattened).strip()
    if flattened:
        derived.append(flattened)

    words = [w for w in flattened.split(" ") if w and w not in _ALIAS_STOPWORDS]

    # Acronym of the significant words: "NMEOOP".
    if len(words) > 1:
        acronym = "".join(w[0] for w in words).upper()
        if len(acronym) >= 2:
            derived.append(acronym)

    # Trailing qualifier after a dash is often how people refer to a
    # sub-scheme: "... - Oil Palm" → "oil palm".
    for separator in ("–", "-", ":", "—"):
        if separator in name:
            tail = name.rsplit(separator, 1)[-1].strip().lower()
            if tail and tail != name.lower():
                derived.append(tail)
            break

    return derived


def _dedupe_aliases(aliases: list[str]) -> list[str]:
    """Drop blanks and case-insensitive duplicates, preserving first-seen order."""
    seen: set[str] = set()
    result: list[str] = []
    for alias in aliases:
        text = str(alias).strip()
        if not text:
            continue
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        result.append(text)
    return result


def resolve_scheme_aliases(
    code: str | None,
    name: str | None,
    explicit: Any = None,
) -> list[str]:
    """Aliases to index for a scheme, in precedence order.

    1. Aliases a curator typed on the document (never overridden)
    2. The curated bootstrap list for this scheme code, when one exists
    3. Aliases derived from the code and name

    All three are merged rather than short-circuited, so a curator adding one
    alias does not silently discard the bootstrap and derived ones.
    """
    code = (code or "").strip().lower()
    merged: list[str] = list(parse_aliases(explicit))

    bootstrap = BOOTSTRAP_VECTOR_SCHEMES.get(code)
    if bootstrap:
        merged.extend(bootstrap.get("scheme_aliases") or [])
        if not (name or "").strip():
            name = bootstrap.get("scheme_name")

    merged.extend(_derive_aliases(code, name or ""))
    return _dedupe_aliases(merged)


def rebuild_catalog_from_documents(*, reason: str = "rebuild") -> int:
    """
    Bump the catalog version.

    Status, groups, chunk counts and the rest are computed by the
    ``catalog_status`` view, so there is nothing to recompute here.
    """
    return db.bump_catalog_version(reason)


def on_scheme_document_promoted(workflow_id: str) -> Optional[int]:
    """Call after successful PROD promote of a scheme document."""
    doc = db.get_document(workflow_id)
    if not doc:
        return None
    if doc.get("kind") != "scheme":
        return None
    # Clear reindex flag on successful promote (new code is live)
    if int(doc.get("reindex_required") or 0):
        db.update_document_fields(
            workflow_id,
            reindex_required=0,
            reindex_reason=None,
        )
    return rebuild_catalog_from_documents(reason=f"promote_scheme:{workflow_id}")


def on_scheme_document_disabled(workflow_id: str, code: Optional[str] = None) -> Optional[int]:
    return rebuild_catalog_from_documents(reason=f"disable_scheme:{workflow_id}")


def build_tool_prompts(vector_schemes: list[dict[str, Any]]) -> dict[str, str]:
    """Fragments for AI system prompts / tool docstrings."""
    codes = [s["scheme_code"] for s in vector_schemes]
    bullets = []
    flat = []
    identifiers = []
    for s in vector_schemes:
        code = s["scheme_code"]
        name = s.get("scheme_name") or code
        bullets.append(f"- **{name}** ({code})")
        flat.append(f"- {name} ({code})")
        aliases = s.get("scheme_aliases") or []
        alias_hint = f" / {aliases[0]}" if aliases else ""
        identifiers.append(f"- `{code}`{alias_hint}")
    legacy_codes = ", ".join(f'"{s["scheme_code"]}"' for s in BOOTSTRAP_LEGACY_SCHEMES)
    search_doc = (
        "Available Qdrant scheme codes: "
        + ", ".join(f'"{c}"' for c in codes)
        + ". Use search_schemes for guideline PDF chunks (Government Scheme Information)."
    )
    return {
        "search_schemes_doc": search_doc,
        "vector_schemes_bullets_en": "\n".join(bullets),
        "legacy_schemes_codes_line": legacy_codes,
        "available_schemes_flat_en": "\n".join(flat),
        "vector_identifiers_block_en": "\n".join(identifiers),
        "vector_scheme_count": str(len(vector_schemes)),
        "legacy_scheme_count": str(len(BOOTSTRAP_LEGACY_SCHEMES)),
    }


def build_snapshot(
    *,
    instance: Optional[str] = None,
    status: str = "live",
    include_pending: bool = False,
    network_visible: Optional[bool] = None,
) -> dict[str, Any]:
    meta = db.get_catalog_meta()
    version = int(meta.get("version") or 0)
    updated_at = meta.get("updated_at") or utc_now_iso()

    statuses = {status} if status else {"live"}
    if include_pending:
        statuses |= {"pending_prod", "pending_reindex"}

    entries = db.list_catalog_entries()
    vector_schemes: list[dict[str, Any]] = []
    for e in entries:
        st = e.get("status") or "disabled"
        if st not in statuses:
            continue
        # Live snapshot for warmers: only status=live (pending excluded unless include_pending)
        if not include_pending and st != "live":
            continue
        groups = e["groups"]
        if instance:
            inst = instance.strip().lower()
            if inst not in {g.lower() for g in groups} and groups:
                continue
        nv = bool(e.get("network_visible"))
        if network_visible is not None and nv != network_visible:
            continue
        vector_schemes.append(
            {
                "scheme_code": e["code"],
                "scheme_name": e.get("name") or e["code"],
                "scheme_aliases": e["aliases"],
                "tool": "search_schemes",
                "tool_routing": "qdrant",
                "instances": groups or ["default"],
                "network_visible": nv,
                "chunk_count": int(e.get("chunk_count") or 0),
                "workflow_ids": e["workflow_ids"],
                "promoted_at": e.get("promoted_at"),
                "source": e.get("source") or "pipeline",
                "status": st,
            }
        )

    vector_schemes.sort(key=lambda s: s["scheme_code"])
    etag_src = f"{version}-" + hashlib.sha256(
        json.dumps([s["scheme_code"] for s in vector_schemes]).encode()
    ).hexdigest()[:8]
    etag = f'"{etag_src}"'

    return {
        "api_version": "1",
        "catalog_version": version,
        "updated_at": updated_at,
        "etag": etag,
        "collections": {
            "scheme_qdrant": {
                "name": scheme_collection_name(),
                "embedding_model": "intfloat/multilingual-e5-large",
                "filter_type": "scheme",
            },
            "documents": {
                "name": documents_collection_name(),
            },
        },
        "legacy_schemes": list(BOOTSTRAP_LEGACY_SCHEMES),
        "vector_schemes": vector_schemes,
        "tool_prompts": build_tool_prompts(
            [s for s in vector_schemes if s.get("status", "live") == "live"]
            if include_pending
            else vector_schemes
        ),
        "routing_exceptions": dict(ROUTING_EXCEPTIONS),
    }


def apply_scheme_metadata(
    workflow_id: str,
    *,
    kind: Optional[str] = None,
    code: Optional[str] = None,
    name: Optional[str] = None,
    aliases: Optional[list[str]] = None,
    tool_routing: Optional[str] = None,
    catalog_visible: Optional[bool] = None,
    network_visible: Optional[bool] = None,
    valid_from: Optional[str] = None,
    valid_to: Optional[str] = None,
) -> dict[str, Any]:
    """Update document catalog fields and rebuild catalog when needed.

    `valid_from`/`valid_to` set the document's validity period (see
    `pipeline/document_validity.py`). Either end alone is enough - the other is
    taken from what is stored, or from the period's own default when the
    document has none - so a reviewer who only moves the end date does not have
    to restate the start.
    """
    doc = db.get_document(workflow_id)
    if not doc:
        raise LookupError(f"Document not found: {workflow_id}")

    updates: dict[str, Any] = {}
    old_code = (doc.get("code") or "").strip().lower() or None
    old_kind = (doc.get("kind") or "").strip().lower()
    code_renamed = False
    pending_reindex = False

    if kind is not None:
        new_kind = kind.strip().lower()
        catalog_type_for_kind(new_kind)  # ValueError for a kind with no catalog type
        updates["kind"] = new_kind
    else:
        new_kind = old_kind

    new_code = old_code
    if code is not None:
        new_code = validate_scheme_code(code) if code else None
        updates["code"] = new_code
        if old_code and new_code and old_code != new_code:
            code_renamed = True

    if name is not None:
        updates["name"] = name.strip() if name else None

    # Auto-derive the code from the title when the caller set a name but no
    # explicit code (the UI form only collects a name and previews the code;
    # this is the authoritative, collision-checked source of truth for it).
    final_name = (updates["name"] if "name" in updates else doc.get("name")) or ""
    if code is None and not new_code and new_kind == "scheme" and final_name.strip():
        new_code = derive_scheme_code(final_name, exclude_workflow_id=workflow_id, kind=new_kind)
        updates["code"] = new_code

    if aliases is not None:
        updates["aliases"] = parse_aliases(aliases)

    if tool_routing is not None:
        tr = tool_routing.strip().lower()
        if tr not in ("qdrant", "legacy", "both"):
            raise ValueError("tool_routing must be qdrant|legacy|both")
        updates["tool_routing"] = tr
    elif new_kind == "scheme" and not doc.get("tool_routing"):
        updates["tool_routing"] = "qdrant"

    if catalog_visible is not None:
        updates["catalog_visible"] = 1 if catalog_visible else 0
    elif new_kind == "scheme" and doc.get("catalog_visible") is None:
        updates["catalog_visible"] = 1

    if network_visible is not None:
        updates["network_visible"] = 1 if network_visible else 0
    elif new_kind == "scheme" and doc.get("network_visible") is None:
        updates["network_visible"] = 1 if default_network_visible(doc.get("group")) else 0

    if valid_from is not None or valid_to is not None:
        # Validated here rather than at the SQL layer, and against the stored
        # period rather than today, so a reviewer editing one end keeps the
        # other exactly as it was.
        period = document_validity.parse_period(
            valid_from if valid_from is not None else doc.get("valid_from"),
            valid_to if valid_to is not None else doc.get("valid_to"),
        )
        updates["valid_from"] = period.start_date
        updates["valid_to"] = period.end_date

    if code_renamed and (doc.get("stage") == "completed") and not int(doc.get("is_disabled") or 0):
        # Rule (b): hide new code until reindex
        updates["reindex_required"] = 1
        updates["reindex_reason"] = f"scheme_code_rename:{old_code}->{new_code}"
        pending_reindex = True

    if updates:
        db.update_document_fields(workflow_id, **updates)

    # Rebuild catalog if scheme-related
    final = db.get_document(workflow_id) or doc
    final_kind = (final.get("kind") or "").strip().lower()
    catalog_version = None
    if final_kind == "scheme" or old_kind == "scheme" or code_renamed:
        reason = "scheme_metadata_patch"
        if pending_reindex:
            reason = "scheme_code_rename_pending_reindex"
        catalog_version = rebuild_catalog_from_documents(reason=reason)

    return {
        "document": final,
        "catalog_version": catalog_version,
        "code_renamed": code_renamed,
        "pending_reindex": pending_reindex,
        "requires_reindex": pending_reindex,
    }
