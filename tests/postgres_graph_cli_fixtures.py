"""Isolated PostgreSQL graph CLI/acceptance fixtures; require native database."""
import json
import os
import unittest
from pathlib import Path
from unittest.mock import patch
import psycopg
from psycopg import sql
from kg_builder.postgres_graph import GraphStore
from kg_builder.snapshot_graph import SnapshotGraph


class NativeGraphCase(unittest.TestCase):
    def setUp(self):
        self.database_url = os.environ.get('KG_TEST_DATABASE_URL')
        if not self.database_url:
            raise RuntimeError('KG_TEST_DATABASE_URL is required for PostgreSQL tests')
        self._schemas = set()
        self.addCleanup(self._drop_graph_schemas)
        environment = patch.dict(os.environ, {'KG_GRAPH_DATABASE_URL': self.database_url})
        environment.start()
        self.addCleanup(environment.stop)

    def make_job(self, *args, **kwargs):
        kwargs.setdefault('database_url', self.database_url)
        job = SnapshotGraph(*args, **kwargs)
        self._schemas.add(job.graph_store.schema)
        self.addCleanup(job.close)
        return job

    def connection(self, job_dir=None):
        directory = Path(job_dir or self.job_dir)
        run_id = json.loads((directory/'graph-job.json').read_text())['run_id']
        store = GraphStore(run_id, database_url=self.database_url, initialize=False)
        self._schemas.add(store.schema)
        # The caller's native connection context closes and commits on success.
        return store.db

    @staticmethod
    def scalar(row):
        return next(iter(row.values()))

    def _drop_graph_schemas(self):
        if not self._schemas:return
        with psycopg.connect(self.database_url, autocommit=True) as db:
            for schema in self._schemas:
                db.execute(sql.SQL('DROP SCHEMA IF EXISTS {} CASCADE').format(sql.Identifier(schema)))
