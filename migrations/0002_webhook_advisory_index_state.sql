-- Advisory indexing state: the payload_hash last written to the dev / PROD Qdrant collection.
ALTER TABLE webhook_records ADD COLUMN indexed_hash TEXT;
ALTER TABLE webhook_records ADD COLUMN promoted_hash TEXT;
