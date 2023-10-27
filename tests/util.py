import os
import pickle
import random


def get_random_ID(s2):
    return random.choice([*s2._metadata.keys()])


def remove_ID_from_store(s2, ID):
    if s2._cache_backend_name == "jsonl":
        fpath = s2._cache_backend._root_dir.joinpath(ID)
        if fpath.exists():
            os.remove(fpath)
            assert not fpath.exists()
    else:
        if ID in s2._cache_backend._all_papers:
            s2._cache_backend.delete_paper_with_id(ID)


def remove_ID_from_memory(s2, ID):
    """Remove a random item from metadata

    """
    if ID in s2._in_memory:
        s2._in_memory.pop(ID)


def remove_random_item_from_store(s2):
    """Remove a random item from metadata

    """
    ID = get_random_ID(s2)
    remove_ID_from_store(s2, ID)
    assert ID in s2._metadata
    return ID


def remove_random_item_from_metadata(s2):
    """Remove a random item from metadata

    """
    ID = get_random_ID(s2)
    s2._metadata.pop(ID)
    return ID


def check_ID_in_store(s2, ID):
    """Check if the ID is in s2 metadata and on the backend

    """
    if s2._cache_backend_name == "jsonl":
        return s2._cache_backend._root_dir.joinpath(ID).exists()
    else:
        return s2._cache_backend.get_paper_data(ID)


def get_random_corpus_id_from_refs_cache(s2):
    corpus_cache = s2.corpus_cache
    if corpus_cache:
        pkl_file = [*corpus_cache.files.values()][0]
        with open(pkl_file, "rb") as f:
            corpus_data = pickle.load(f)
        return random.choice([*corpus_data.keys()])
