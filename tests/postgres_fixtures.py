"""Isolated native PostgreSQL fixtures for preparation integration suites."""
import os
from pathlib import Path
import tempfile
import unittest
import uuid

from psycopg import sql

from kg_builder.extraction_validation import RETRIEVAL_SCHEMA_VERSION


class PostgresCase(unittest.TestCase):
    def setUp(self):
        self.database_url = os.environ.get('KG_TEST_DATABASE_URL')
        if not self.database_url:
            raise RuntimeError('KG_TEST_DATABASE_URL is required for PostgreSQL tests')
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self._runs = {}
        self._workflows = []
        self.addCleanup(self._clean_database)

    def workflow(self, cls, run_dir=None, **kwargs):
        run_dir = Path(run_dir or self.tmp.name)
        key = str(run_dir.resolve())
        run_id = self._runs.setdefault(key, 'integration-' + uuid.uuid4().hex)
        workflow = cls(run_dir, run_id=run_id, database_url=self.database_url, **kwargs)
        self._workflows.append(workflow)
        return workflow

    @staticmethod
    def scalar(row):
        return next(iter(row.values()))

    def _clean_database(self):
        schemas = {}
        for workflow in self._workflows:
            schemas[workflow.store.workflow_schema] = workflow.store.source_schema
            workflow.close()
        if not schemas:
            return
        # Only schemas derived from run IDs allocated by this test are dropped.
        from psycopg import connect
        with connect(self.database_url, autocommit=True) as conn:
            for workflow_schema, source_schema in schemas.items():
                conn.execute(sql.SQL('DROP SCHEMA IF EXISTS {} CASCADE').format(sql.Identifier(workflow_schema)))
                conn.execute(sql.SQL('DROP SCHEMA IF EXISTS {} CASCADE').format(sql.Identifier(source_schema)))

    @staticmethod
    def db(workflow):
        """Return a native psycopg connection; caller releases it to the workflow pool."""
        return workflow._db()


def source(n):
    return dict(title=f'Agent memory paper {n}', abstract=f'We introduce memory system {n} for an agent.',
                year=2026, published_at='2026-01-01', url=f'https://arxiv.org/abs/2601.{n:05d}v1',
                arxiv_id=f'2601.{n:05d}', version_label='v1', source_id=f'https://arxiv.org/abs/2601.{n:05d}v1')


def draft(task):
    return dict(schema_version=RETRIEVAL_SCHEMA_VERSION, paper_id=task['paper_id'], version_id=task['version_id'],
                paper_version=task['paper_version'], extractor_type='external_unreviewed',
                extractor_version='fake-model-1', source_scope='supplied_paper_text',
                status='draft_requires_review', contributions=[dict(
                    key=task['version_id']+'#c1', description='Introduces a memory system')],
                research_problems=[], methods=[], benchmarks=[])


class Embedding:
    model_id = 'fake:local'

    def encode(self, texts):
        return [[1., 0.] for _ in texts]


def scope(items, client, min_confidence):
    return [dict(paper_id=i['paper_id'], decision='include', topics=['agent_memory'],
                 reason='model yes', raw={'choices': 'yes'}) for i in items]


def review(items, client, min_confidence):
    return [dict(paper_id=i['paper_id'], passed=True, issues=[], raw={'items': 'supported'}) for i in items]
