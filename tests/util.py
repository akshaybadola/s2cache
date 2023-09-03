import os
import random


def get_random_ID(s2):
    if s2._cache_backend_name == "jsonl":
        return random.choice([*s2._metadata.keys()])
    else:
        raise NotImplementedError


def remove_ID_from_store(s2, ID):
    if s2._cache_backend_name == "jsonl":
        fpath = s2._cache_backend._root_dir.joinpath(ID)
        if fpath.exists():
            os.remove(fpath)
            assert not fpath.exists()
    else:
        raise NotImplementedError


def remove_ID_from_memory(s2, ID):
    """Remove a random item from metadata

    """
    if ID in s2._in_memory:
        s2._in_memory.pop(ID)


def remove_random_item_from_store(s2):
    """Remove a random item from metadata

    """
    if s2._cache_backend_name == "jsonl":
        ID = get_random_ID(s2)
        remove_ID_from_store(s2, ID)
        assert ID in s2._metadata
        return ID
    else:
        assert False


def remove_random_item_from_metadata(s2):
    """Remove a random item from metadata

    """
    if s2._cache_backend_name == "jsonl":
        ID = get_random_ID(s2)
        s2._metadata.pop(ID)
        return ID
    else:
        assert False


def check_ID_in_store(s2, ID):
    """Check if the ID is in s2 metadata and on the backend

    """
    if s2._cache_backend_name == "jsonl":
        return s2._cache_backend._root_dir.joinpath(ID).exists()
    else:
        assert False
