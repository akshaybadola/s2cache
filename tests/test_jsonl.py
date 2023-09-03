import pytest
import os
import json
import random

from s2cache.models import PaperDetails, PaperData
from s2cache import semantic_scholar as ss


def get_metadata(cache_dir):
    with open(os.path.join(cache_dir, "metadata.jsonl")) as f:
        metadata = f.read().split("\n")
    metadata = [*filter(None, metadata)]
    return metadata


def delete_random_metadata_line(cache_dir):
    metadata = get_metadata(cache_dir)
    line = random.choice(metadata)
    metadata.remove(line)
    with open(os.path.join(cache_dir, "metadata.jsonl"), "w") as f:
        f.write("\n".join(metadata))
    return line


def test_jsonl_load_cache_with_dups(s2):
    metadata = get_metadata(s2._cache_dir)
    for _ in range(5):
        metadata.append(random.choice(metadata))
    assert len(metadata) == 36
    with open(os.path.join(s2._cache_dir, "metadata.jsonl"), "w") as f:
        f.write("\n".join(metadata))
    api = ss.SemanticScholar(cache_dir="tests/cache_data/")
    assert len(api._metadata) == 31


def test_jsonl_rebuild_jsonl_metadata(s2):
    metadata = s2._metadata.copy()
    metadata_file = s2._cache_dir.joinpath("metadata.jsonl")
    os.remove(metadata_file)
    s2.rebuild_metadata()
    assert metadata_file.exists()
    assert metadata == s2._metadata
