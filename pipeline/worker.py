"""
Temporal worker for the OCR pipeline.
"""

import asyncio
import logging
import os

from temporalio.client import Client
from temporalio.worker import Worker

from . import db
from .activities import (
    auto_tag_chunks_from_db,
    create_chunks,
    create_chunks_from_db,
    detect_and_translate_pages,
    detect_and_translate_pages_from_db,
    index_webhook_advisory,
    ingest_document_from_db,
    ingest_to_vector_db,
    persist_document_content,
    prepare_for_ingestion,
    promote_document_to_prod_qdrant,
    promote_webhook_advisory,
    publish_catalog_to_network,
    run_ocr,
    run_ocr_and_store,
    update_document_state,
)
from .advisory_start import backfill_on_start
from .workflows import (
    AdvisoryIndexWorkflow,
    AdvisoryPromoteWorkflow,
    ChunkingOnlyWorkflow,
    DocumentPipelineWorkflow,
    OcrOnlyWorkflow,
    PromoteToProdWorkflow,
    ReingestionWorkflow,
    TranslationOnlyWorkflow,
)

LOG_LEVEL = getattr(logging, os.environ.get("LOG_LEVEL", "INFO").strip().upper(), logging.INFO)

# Configure verbose logging
logging.basicConfig(
    level=LOG_LEVEL,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%H:%M:%S'
)

# Set Temporal SDK logging to INFO regardless of LOG_LEVEL - its own DEBUG
# output is noisy and rarely what LOG_LEVEL=DEBUG is set to see.
logging.getLogger("temporalio").setLevel(logging.INFO)

TASK_QUEUE = "ocr-pipeline"


async def main():
    """Start the worker."""
    translation_provider = os.environ.get("TRANSLATION_PROVIDER", "gemma_vllm").strip().lower()
    if translation_provider in {"gemma_vllm", "gemma4", "gemma"}:
        if not os.environ.get("TRANSLATION_VLLM_BASE_URL", "").strip():
            print("Error: TRANSLATION_VLLM_BASE_URL not set")
            return

    # Initialize Postgres database
    print("Initializing database...")
    db.init_db()

    temporal_host = os.environ.get("TEMPORAL_HOST", "localhost:7233")
    max_concurrent_activities = int(os.environ.get("TEMPORAL_MAX_CONCURRENT_ACTIVITIES", "4"))
    print(f"Connecting to Temporal at {temporal_host}")
    print(f"Worker activity concurrency: {max_concurrent_activities}")

    client = await Client.connect(temporal_host)

    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        max_concurrent_activities=max_concurrent_activities,
        workflows=[
            DocumentPipelineWorkflow,
            ReingestionWorkflow,
            PromoteToProdWorkflow,
            TranslationOnlyWorkflow,
            OcrOnlyWorkflow,
            ChunkingOnlyWorkflow,
            AdvisoryIndexWorkflow,
            AdvisoryPromoteWorkflow,
        ],
        activities=[
            run_ocr,
            run_ocr_and_store,
            create_chunks,
            create_chunks_from_db,
            auto_tag_chunks_from_db,
            prepare_for_ingestion,
            ingest_to_vector_db,
            ingest_document_from_db,
            promote_document_to_prod_qdrant,
            index_webhook_advisory,
            promote_webhook_advisory,
            publish_catalog_to_network,
            update_document_state,
            detect_and_translate_pages,
            detect_and_translate_pages_from_db,
            persist_document_content,
        ],
    )

    backfilled = await backfill_on_start(client)
    if backfilled:
        print(f"Advisory startup backfill queued {backfilled} workflows")

    print(f"Worker started on queue: {TASK_QUEUE}")
    print("Waiting for workflows...")
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
