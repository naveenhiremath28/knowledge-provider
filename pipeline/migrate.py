"""Versioned SQL migration runner.

Applies ``migrations/NNNN_<name>.sql`` files in order, once each, recording
them in ``schema_migrations``. A session-level advisory lock serialises
concurrent api/worker startups so only one process applies a given file.
"""

import logging
import re
from pathlib import Path

import psycopg

logger = logging.getLogger(__name__)

MIGRATIONS_DIR = Path(__file__).resolve().parent.parent / "migrations"
_MIGRATION_FILE_RE = re.compile(r"^(\d{4})_[a-z0-9_]+\.sql$")
_ADVISORY_LOCK_KEY = 7_204_911_338_105_221


def list_migrations(directory: Path = MIGRATIONS_DIR) -> list[tuple[str, Path]]:
    found = []
    for path in sorted(directory.glob("*.sql")):
        match = _MIGRATION_FILE_RE.match(path.name)
        if not match:
            raise ValueError(f"Migration file name must be NNNN_name.sql: {path.name}")
        found.append((match.group(1), path))
    return found


def apply_migrations(conninfo: str, directory: Path = MIGRATIONS_DIR) -> list[str]:
    """Apply every pending migration. Returns the versions applied by this call."""
    applied: list[str] = []
    with psycopg.connect(conninfo, autocommit=True) as conn:
        conn.execute("SELECT pg_advisory_lock(%s)", (_ADVISORY_LOCK_KEY,))
        try:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    version    TEXT PRIMARY KEY,
                    name       TEXT NOT NULL,
                    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            done = {row[0] for row in conn.execute("SELECT version FROM schema_migrations")}
            for version, path in list_migrations(directory):
                if version in done:
                    continue
                with conn.transaction():
                    conn.execute(path.read_text(encoding="utf-8"))
                    conn.execute(
                        "INSERT INTO schema_migrations (version, name) VALUES (%s, %s)",
                        (version, path.name),
                    )
                logger.info("applied migration %s", path.name)
                applied.append(version)
        finally:
            conn.execute("SELECT pg_advisory_unlock(%s)", (_ADVISORY_LOCK_KEY,))
    return applied


def current_version(conninfo: str) -> str | None:
    with psycopg.connect(conninfo, autocommit=True) as conn:
        exists = conn.execute("SELECT to_regclass('schema_migrations')").fetchone()[0]
        if not exists:
            return None
        row = conn.execute("SELECT max(version) FROM schema_migrations").fetchone()
        return row[0]


def is_current(conninfo: str, directory: Path = MIGRATIONS_DIR) -> bool:
    migrations = list_migrations(directory)
    if not migrations:
        return True
    return current_version(conninfo) == migrations[-1][0]
