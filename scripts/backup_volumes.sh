#!/usr/bin/env bash
set -euo pipefail

# Backup all persistent data volumes used by the docs pipeline:
# - Temporal Postgres database
# - MinIO object storage
# - Application Postgres volume (documents, workflows, pages, chunks, audit)
#   (a logical pg_dump is also written when the postgres container is running)
#
# Usage:
#   ./scripts/backup_volumes.sh                # writes to ./backups
#   ./scripts/backup_volumes.sh my-backups    # writes to ./my-backups
#
# NOTES:
# - For a fully consistent snapshot, stop the stack first:
#     cd deployment/docs-pipeline
#     docker compose stop
# - This script auto-detects the actual Docker volume names based on suffixes.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

BACKUP_DIR="${1:-backups}"
mkdir -p "$BACKUP_DIR"

TIMESTAMP="$(date +%Y%m%d-%H%M%S)"

backup_volume() {
  local logical_name="$1"
  local suffix="$2"

  # Find the actual docker volume whose name ends with the given suffix
  local vol_name
  vol_name="$(docker volume ls --format '{{.Name}}' | grep "${suffix}$" || true)"

  if [[ -z "$vol_name" ]]; then
    echo "WARNING: volume for ${logical_name} (*${suffix}) not found; skipping" >&2
    return 0
  fi

  echo "Backing up ${logical_name} volume: ${vol_name}"
  docker run --rm \
    -v "${vol_name}":/from \
    -v "${ROOT_DIR}/${BACKUP_DIR}":/backup \
    alpine sh -c "cd /from && tar czf /backup/${vol_name}-${TIMESTAMP}.tar.gz ."
}

echo "Backing up docs-pipeline volumes into ${BACKUP_DIR} (timestamp: ${TIMESTAMP})"

# 1) Temporal Postgres DB
backup_volume "temporal-db" "temporal-db-data"

# 2) MinIO object storage (all PDFs / objects)
backup_volume "minio" "minio-data"

# 3) Application Postgres volume
backup_volume "app-db" "app-db-data"

# 4) Logical dump of the application database, consistent while the stack runs
PG_CONTAINER="$(docker ps --format '{{.Names}}' | grep 'postgres' | grep -v 'temporal\|keycloak' | head -n 1 || true)"
if [[ -n "$PG_CONTAINER" ]]; then
  echo "Dumping application database from ${PG_CONTAINER}"
  docker exec "$PG_CONTAINER" sh -c 'pg_dump -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Fc' \
    > "${BACKUP_DIR}/app-db-${TIMESTAMP}.dump"
else
  echo "WARNING: application postgres container not running; skipped pg_dump" >&2
fi

echo
echo "Backup complete. Files in ${BACKUP_DIR}:"
ls -lh "${BACKUP_DIR}"

