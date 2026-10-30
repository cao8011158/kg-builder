"""Design-only scheduled ETL entrypoint. NEVER enabled by default; not tested.

A future external scheduler may call this after implementing data sources,
review/validation, Neo4j writes, checkpoints and Drive snapshot replacement.
"""
from __future__ import annotations
import os


def run_scheduled_refresh():
    if os.getenv('ENABLE_SCHEDULED_KG_REFRESH') != 'true':
        raise RuntimeError('Scheduled incremental ingestion is disabled by default')
    raise NotImplementedError('Production scheduling/graph write path is not implemented')


if __name__=='__main__':
    run_scheduled_refresh()
