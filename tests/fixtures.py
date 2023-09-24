import os
import sys
from multiprocessing import Process
import logging
import shutil
import pytest

from s2cache.semantic_scholar import SemanticScholar


def server_proc():
    import service
    from pathlib import Path
    from s2cache.semantic_scholar import SQLiteBackend
    sql = SQLiteBackend(Path(__file__).parent.joinpath("service_data"), "test-logger")
    server = service.Service(sql, "127.0.0.1", 1221)
    server.server.run()


if os.environ.get("FAKE_S2"):
    import time
    proc = Process(target=server_proc)
    proc.start()
    time.sleep(.5)
else:
    proc = None


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
    if os.environ.get("FAKE_S2"):
        s2._root_url = "http://127.0.0.1:1221/graph/v1"
        s2_key = None
    if s2_key:
        s2._api_key = s2_key
    yield s2
    if proc is not None and proc.is_alive():
        proc.terminate()


@pytest.fixture(scope="session")
def s2_sqlite():
    s2 = SemanticScholar(cache_dir="tests/cache_data/",
                         config_file="tests/config.yaml",
                         cache_backend="sqlite",
                         logger_name="s2-test")
    s2_key = os.environ.get("S2_API_KEY")
    if os.environ.get("FAKE_S2"):
        s2._root_url = "http://127.0.0.1:1221/graph/v1"
        s2_key = None
    if s2_key:
        s2._api_key = s2_key
    sql = s2._cache_backend
    # sql.delete_rows(*sql._dbs["metadata"], "paperid is null")
    if "79464be4efb538055ebb3d20c4720c8f77218644" in sql._paper_pks:
        sql.delete_paper_with_id("79464be4efb538055ebb3d20c4720c8f77218644")
    s2.load_metadata()
    yield s2
    if proc is not None and proc.is_alive():
        proc.terminate()


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
