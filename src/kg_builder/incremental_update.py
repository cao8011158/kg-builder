"""Future scheduled incremental-ingestion entrypoint; disabled scaffold.

No scheduler, APIs, databases or embeddings are invoked by importing this module.
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import Protocol

from .extraction_api import PaperInput, DisabledExtractorError


class Extractor(Protocol):
    def extract(self, paper: PaperInput) -> str: ...


@dataclass(frozen=True)
class IncrementalUpdateConfig:
    enabled: bool = False
    since_date: str = "2025-01-01"


class IncrementalPaperUpdater:
    """One entrypoint for manual and future API-driven extraction workflows."""

    def __init__(self, extractor: Extractor, config: IncrementalUpdateConfig):
        self.extractor = extractor
        self.config = config

    def process_one(self, paper: PaperInput) -> str:
        if not self.config.enabled:
            raise DisabledExtractorError("Incremental updates are disabled by default")
        # TODO: persist candidate, validate extraction, resolve entities,
        # stage changes, upsert graph, update indexes and checkpoint.
        return self.extractor.extract(paper)


def scheduled_entrypoint() -> None:
    """Placeholder to be called by an external scheduler once implemented."""
    raise DisabledExtractorError("Scheduled ingestion is not implemented or enabled")
