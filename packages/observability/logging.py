"""Structured JSON logging (Section 66).

Never logs authentication tokens, API keys, or raw message payloads.
Every log call should go through `log_event` with structured kwargs so
fields stay queryable instead of buried in a free-text message.
"""
from __future__ import annotations

import json
import logging
import sys
import time
from typing import Any

_REDACT_KEYS = {"token", "api_key", "authorization", "password", "secret", "payload"}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        base: dict[str, Any] = {
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(record.created)),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        extra = getattr(record, "fields", None)
        if extra:
            for k, v in extra.items():
                if k.lower() in _REDACT_KEYS:
                    continue
                base[k] = v
        return json.dumps(base, default=str)


def configure_logging(level: str = "INFO") -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(level)


def log_event(logger: logging.Logger, level: int, event: str, **fields: Any) -> None:
    """Log a structured event. `event` should be a short stable string like
    'message_delivered' (see Section 66) so it can be grepped/aggregated."""
    logger.log(level, event, extra={"fields": {"event": event, **fields}})
