"""Opt-in scheduler entrypoint for publishing an already approved batch."""
from __future__ import annotations
import os
from pathlib import Path
import sys


def run_scheduled_refresh(argv=None):
    if os.getenv('ENABLE_SCHEDULED_KG_REFRESH') != 'true':
        raise RuntimeError('Scheduled incremental ingestion is disabled by default')
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from scripts.publish_incremental_graph import main

    return main(argv)


if __name__=='__main__':
    run_scheduled_refresh()
