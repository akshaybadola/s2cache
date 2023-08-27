import os
import logging
import shutil
import pytest

from s2cache.semantic_scholar import SemanticScholar


@pytest.fixture
def s2():
    with open("tests/cache_data/metadata.bak") as f:
        temp = filter(None, f.read().split("\n"))
    temp = [t.rsplit(",", 1) for t in temp]
    for t in temp:
        t.insert(-1, ",,")
    with open("tests/cache_data/metadata", "w") as f:
        f.write("\n".join([",".join(t) for t in temp]))
    logger = logging.getLogger("s2-test")
    logger.setLevel(logging.DEBUG)
    s2 = SemanticScholar(cache_dir="tests/cache_data/",
                         config_file="tests/config.yaml")
    s2_key = os.environ.get("S2_API_KEY")
    if s2_key:
        s2._api_key = s2_key
    return s2


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
