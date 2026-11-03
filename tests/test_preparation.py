"""Preparation behavior moved to postgres_preparation.py (explicit PostgreSQL suite).

The synthetic source, draft and embedding fixtures remain importable for archived
examples; this module contains no tests and requires no database connection.
"""
from tests.postgres_fixtures import source, draft, Embedding, scope, review
