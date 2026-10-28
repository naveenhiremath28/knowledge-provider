"""Group (tenant / state) access helpers — Keycloak groups + document groups.

Who-sees-what-state comes from the JWT (Keycloak groups). The database only
stores which group a *document* belongs to (``groups`` via
``documents.catalog_id -> catalogs.group_id``); the document dict carries its
tenant code as ``group``.

Conventions:
  - State codes: lowercase (``mh``, ``up``, …) matching Keycloak ``/states/MH/...``
  - Portal / BV platform docs: ``bv`` (Bharat Vistaar)
  - Super admin (``/global/super-admin``): unrestricted groups
"""

from __future__ import annotations

import os

from fastapi import HTTPException

from .models import AuthUser

# Bharat Vistaar portal / platform-wide documents (not a state).
PORTAL_GROUP = "bv"


def default_group() -> str:
    """Fallback group code for new docs and legacy rows without a value."""
    return (os.environ.get("DEFAULT_INSTANCE") or "default").strip().lower() or "default"


def normalize_group(value: str | None) -> str:
    text = (value or "").strip().lower()
    return text or default_group()


def unrestricted(user: AuthUser) -> bool:
    """True when the caller may see all groups (super admin / local bypass)."""
    return user.is_group_unrestricted()


def allowed_groups(user: AuthUser) -> set[str] | None:
    """
    Return the set of group codes the user may access, or None if unrestricted.
    """
    if unrestricted(user):
        return None
    return {normalize_group(i) for i in user.instances if str(i).strip()}


def user_can_access_group(user: AuthUser, group: str | None) -> bool:
    allowed = allowed_groups(user)
    if allowed is None:
        return True
    if not allowed:
        return False
    return normalize_group(group) in allowed


def assert_group_access(user: AuthUser, group: str | None) -> str:
    """Raise 403 if user cannot access group; return normalized group code."""
    normalized = normalize_group(group)
    if not user_can_access_group(user, normalized):
        raise HTTPException(403, f"No access to group: {normalized}")
    return normalized


def resolve_create_group(user: AuthUser, requested: str | None = None) -> str:
    """Pick the document's ``group`` at create/upload time.

    - Super admin / bypass: use requested, else portal ``bv``, else DEFAULT_INSTANCE.
    - Single-state user: that state (requested must match if provided).
    - Multi-state user: must pass ``requested`` among allowed states.
    - No states in token: 403 (Keycloak group missing).
    """
    requested_norm = (requested or "").strip().lower() or None

    if unrestricted(user):
        if requested_norm:
            return normalize_group(requested_norm)
        # Prefer portal tag for platform operators when nothing chosen.
        portal = (os.environ.get("PORTAL_INSTANCE") or PORTAL_GROUP).strip().lower()
        return normalize_group(portal or default_group())

    allowed = allowed_groups(user) or set()
    if not allowed:
        raise HTTPException(
            403,
            "No state assigned. Join a Keycloak group such as /states/MH/admin or /states/MH/view.",
        )

    if requested_norm:
        if requested_norm not in allowed:
            raise HTTPException(403, f"No access to group: {requested_norm}")
        return requested_norm

    if len(allowed) == 1:
        return next(iter(allowed))

    raise HTTPException(
        400,
        "group is required when you have multiple states; "
        f"choose one of: {', '.join(sorted(allowed))}",
    )


def assert_document_group_access(user: AuthUser, doc: dict | None) -> dict:
    """
    Ensure the document exists and the user may access its group.
    Missing / forbidden both return 404 to avoid leaking other tenants' ids.
    """
    if not doc:
        raise HTTPException(404, "Document not found")
    if not user_can_access_group(user, doc.get("group")):
        raise HTTPException(404, "Document not found")
    return doc
