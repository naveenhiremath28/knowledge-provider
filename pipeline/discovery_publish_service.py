"""
Client for the external Discovery Service's Beckn-style catalog/publish endpoint.

Kept separate from pipeline/activities.py so the outbound call and envelope
shape are testable without Temporal, and reusable outside the pipeline activity.

pipeline/catalog_builder.py decides what travels on the wire; this module owns
the environment and the transport.
"""

import logging
import os
import uuid
from datetime import datetime
from typing import Optional

import httpx

from .catalog_builder import build_catalog
from .network_constants import BECKN_VERSION, RESULT_ACCEPTED

logger = logging.getLogger(__name__)


class DiscoveryPublishService:
    """POSTs a Beckn-shaped catalog/publish envelope to the Discovery Service."""

    def __init__(
        self,
        endpoint: Optional[str] = None,
        sender_id: Optional[str] = None,
        timeout: float = 30.0,
    ):
        self.endpoint = endpoint or os.environ.get("DISCOVERY_SERVICE_ENDPOINT")
        # Doubles as context.networkId.
        self.sender_id = sender_id or os.environ.get("NETWORK_SENDER_ID")

        if not self.endpoint:
            raise RuntimeError("DISCOVERY_SERVICE_ENDPOINT must be set to publish to the network.")
        if not self.sender_id:
            raise RuntimeError("NETWORK_SENDER_ID must be set to publish to the network.")

        self.timeout = timeout

    def publish(
        self,
        transaction_id: str,
        kind: Optional[str],
        workflow_id: Optional[str] = None,
    ) -> dict:
        """
        POST a catalog/publish envelope for `kind`.

        `transaction_id` is supplied by the caller (generated once per publish,
        reused across retries) - only `messageId` is generated fresh here, per
        Beckn convention.

        A kind with no catalog mapped to it makes no HTTP call and comes back
        with `skipped=True`: the spec declares `message.catalogs` as
        `minItems: 1`, so there is no valid "publish nothing" request to send.
        """
        catalog = build_catalog(kind)
        if catalog is None:
            logger.info(
                "workflow_id=%s kind=%s network_publish_skipped=True",
                workflow_id,
                kind,
            )
            return {
                "skipped": True,
                "envelope": None,
                "status_code": None,
                "response_body": None,
                "result_status": None,
                "errors": [],
            }

        envelope = {
            "context": {
                "action": "catalog/publish",
                "version": BECKN_VERSION,
                "messageId": str(uuid.uuid4()),
                "transactionId": transaction_id,
                "timestamp": datetime.utcnow().isoformat(timespec="milliseconds") + "Z",
                "senderId": self.sender_id,
                "networkId": self.sender_id,
            },
            "message": {
                "catalogs": [catalog],
                # MERGE is the documented default, but FULL deletes whatever a
                # payload omits - too sharp a difference to leave implicit.
                "publishDirectives": [
                    {
                        "catalogId": catalog["id"],
                        "catalogType": "REGULAR",
                        "updateMode": "MERGE",
                    }

                ],
            },
        }
        url = f"{self.endpoint.rstrip('/')}/publish"
        logger.info(
            "workflow_id=%s catalog_id=%s transaction_id=%s message_id=%s "
            "network_publish_url=%s",
            workflow_id,
            catalog["id"],
            transaction_id,
            envelope["context"]["messageId"],
            url,
        )
        logger.debug("Request to %s with \n body %s", url, envelope)
        try:
            with httpx.Client(timeout=self.timeout) as client:
                response = client.post(url, json=envelope)
                response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            # The activity records no artifact when publish raises, so this is
            # the only trace of a transport failure.
            logger.error(
                "workflow_id=%s catalog_id=%s network_publish_failed=True transaction_id=%s "
                "error=%s response_body=%s",
                workflow_id,
                catalog["id"],
                transaction_id,
                exc,
                exc.response.text,
            )
            raise
        except httpx.HTTPError as exc:
            logger.error(
                "workflow_id=%s catalog_id=%s network_publish_failed=True transaction_id=%s error=%s",
                workflow_id,
                catalog["id"],
                transaction_id,
                exc,
            )
            raise

        logger.info(
            "workflow_id=%s catalog_id=%s network_publish_status=%s transaction_id=%s",
            workflow_id,
            catalog["id"],
            response.status_code,
            transaction_id
        )

        result_status, errors = self._read_result(response, catalog["id"], workflow_id)
        return {
            "skipped": False,
            "envelope": envelope,
            "status_code": response.status_code,
            "response_body": response.text,
            "result_status": result_status,
            "errors": errors,
        }

    def _read_result(
        self,
        response: httpx.Response,
        catalog_id: str,
        workflow_id: Optional[str],
    ) -> tuple[Optional[str], list]:
        """This catalog's result status and errors from an on_publish body.

        Deliberately lenient: an unreadable or absent `message.results` reads as
        an unknown status, never a failure - a peer that deviates from the spec
        should not stall the pipeline. Only an explicit REJECTED is a rejection,
        and the caller decides what that costs.
        """
        try:
            results = response.json()["message"]["results"]
            if not isinstance(results, list) or not results:
                raise ValueError("no results")
        except Exception:
            logger.warning(
                "workflow_id=%s catalog_id=%s network_publish_result=unreadable "
                "response_body=%.200s",
                workflow_id,
                catalog_id,
                response.text,
            )
            return None, []

        result = next(
            (r for r in results if isinstance(r, dict) and r.get("catalogId") == catalog_id),
            results[0] if isinstance(results[0], dict) else {},
        )
        result_status = result.get("status")
        errors = result.get("errors") or []

        if result_status != RESULT_ACCEPTED:
            logger.warning(
                "workflow_id=%s catalog_id=%s network_publish_result=%s errors=%s",
                workflow_id,
                catalog_id,
                result_status,
                errors,
            )

        return result_status, errors
