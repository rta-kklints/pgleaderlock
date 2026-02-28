from __future__ import annotations

import logging
from typing import Any


class NDLogger:
    """Thin structured-logging wrapper around stdlib logging.Logger."""

    def __init__(self, logger: logging.Logger, context: dict[str, Any] | None = None) -> None:
        self._logger = logger
        self._context: dict[str, Any] = context or {}

    def bind(self, **ctx: Any) -> NDLogger:
        merged = {**self._context, **ctx}
        return NDLogger(self._logger, merged)

    def _format(self, event: str, extra: dict[str, Any]) -> str:
        fields = {**self._context, **extra}
        parts = [event] + [f"{k}={v}" for k, v in fields.items()]
        return " ".join(parts)

    def debug(self, event: str, **kw: Any) -> None:
        self._logger.debug(self._format(event, kw))

    def info(self, event: str, **kw: Any) -> None:
        self._logger.info(self._format(event, kw))

    def warning(self, event: str, **kw: Any) -> None:
        self._logger.warning(self._format(event, kw))

    def error(self, event: str, **kw: Any) -> None:
        self._logger.error(self._format(event, kw))

    def exception(self, event: str, **kw: Any) -> None:
        self._logger.exception(self._format(event, kw))


def get_logger(name: str = "pgleaderlock") -> NDLogger:
    return NDLogger(logging.getLogger(name))
