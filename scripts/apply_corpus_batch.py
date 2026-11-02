"""Retired SQLite corpus batch entry point.

New preparation and graph jobs use native PostgreSQL. This historical entry
point refuses both CLI and imported calls before inspecting inputs or opening
any storage, so it cannot partially commit an obsolete SQLite workflow.
"""

RETIREMENT_MESSAGE = (
    'Historical SQLite corpus batch workflow retired. '
    'Use scripts/prepare_pipeline.py --run-id RUN_ID --run-dir ARTIFACT_DIR run '
    'with KG_DATABASE_URL, then scripts/build_snapshot_graph.py for the frozen snapshot.'
)


def run(master_path, sidecar_path, manifest, reviews, extractions, batch_id, apply=False):
    """Refuse the retired imported API without touching files or databases."""
    raise RuntimeError(RETIREMENT_MESSAGE)


def main(argv=None):
    raise SystemExit(RETIREMENT_MESSAGE)


if __name__ == '__main__':
    main()
