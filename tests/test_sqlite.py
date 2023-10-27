import pytest
import os
import json
import random

from s2cache.sqlite_backend import SQLiteBackend
from s2cache.models import PaperDetails, PaperData
from s2cache import semantic_scholar as ss


def test_sqlite_init_and_convert_metadata(s2):
    sql = SQLiteBackend(s2._cache_dir, s2._logger_name)
    sql._create_dbs()
    request_ids, result = ss.ensure_corpus_ids(s2, s2._metadata)
    metadata = sql.load_metadata()
    duplicates = sql.load_duplicates_metadata()
    assert len(metadata) == 31
    assert len(duplicates)


@pytest.mark.skip
def test_sqlite_load_and_update_metadata(s2_sqlite):
    s2 = s2_sqlite
    ID = "8e0be569ea77b8cb29bb0e8b031887630fe7a96c"
    s2_sqlite._cache_backend.get_paper_data(ID)
    sql = s2_sqlite._cache_backend
    corpus = sql._load_corpus()
    if not corpus:
        request_ids, result = s2._ensure_corpus_ids()
        sql._dump_corpus(result, request_ids)
        sql._dump_metadata(s2._metadata)
    metadata = sql.load_metadata()
    sql._dump_all_paper_data(metadata, s2._cache_dir)
