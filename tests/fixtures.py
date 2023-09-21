import os
import sys
import logging
import shutil
import pytest

from s2cache.semantic_scholar import SemanticScholar


logger = logging.getLogger("s2-test")
logger.setLevel(logging.DEBUG)
fmt = '[%(levelname)s] %(asctime)s %(message)s'
formatter = logging.Formatter(fmt=fmt)
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setLevel(logging.DEBUG)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)


@pytest.fixture(scope="session")
def s2():
    shutil.copy("tests/cache_data/metadata.jsonl.bak",
                "tests/cache_data/metadata.jsonl")
    s2 = SemanticScholar(cache_dir="tests/cache_data/",
                         config_file="tests/config.yaml",
                         logger_name="s2-test")
    s2_key = os.environ.get("S2_API_KEY")
    if s2_key:
        s2._api_key = s2_key
    yield s2


@pytest.fixture(scope="session")
def s2_sqlite():
    s2 = SemanticScholar(cache_dir="tests/cache_data/",
                         config_file="tests/config.yaml",
                         cache_backend="sqlite",
                         logger_name="s2-test")
    s2_key = os.environ.get("S2_API_KEY")
    if s2_key:
        s2._api_key = s2_key
    sql = s2._cache_backend
    # sql.delete_rows(*sql._dbs["metadata"], "paperid is null")
    if "79464be4efb538055ebb3d20c4720c8f77218644" in sql._paper_pks:
        sql.delete_paper_with_id("79464be4efb538055ebb3d20c4720c8f77218644")
    s2.load_metadata()
    yield s2


@pytest.fixture
def cache():
    shutil.copy("tests/cache_data/metadata.json.bak",
                "tests/cache_data/metadata.jsonl")
    cache = FilesCache(Path("tests/cache_data/"))
    return cache


@pytest.fixture
def cache_files():
    cache_dir = "tests/cache_data"
    return [x for x in os.listdir(cache_dir) if "metadata" not in x]
