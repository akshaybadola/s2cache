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


def test_s2_init(s2):
    assert s2._metadata is not None
    assert s2._extid_metadata is not None
    assert len(s2._metadata) == 31


def test_s2_rebuild_jsonl_metadata(s2):
    metadata = s2._metadata.copy()
    metadata_file = s2._cache_dir.joinpath("metadata.jsonl")
    os.remove(metadata_file)
    s2.rebuild_jsonl_metadata()
    assert metadata_file.exists()
    assert metadata == s2._metadata


def test_s2_load_cache_with_dups(s2):
    metadata = get_metadata(s2._cache_dir)
    for _ in range(5):
        metadata.append(random.choice(metadata))
    assert len(metadata) == 36
    with open(os.path.join(s2._cache_dir, "metadata.jsonl"), "w") as f:
        f.write("\n".join(metadata))
    api = ss.SemanticScholar(cache_dir="tests/cache_data/")
    assert len(api._metadata) == 31


def test_s2_cache_get_details_on_disk(s2, cache_files):
    ID = random.choice(cache_files)
    data = s2.get_details_for_id(ss.IdTypes.ss, ID, False, False)
    assert isinstance(data, PaperDetails)


@pytest.mark.inconsistent
def test_s2_cache_force_get_details_on_disk(s2, cache_files):
    ID = random.choice(cache_files)
    data = s2.get_details_for_id(ss.IdTypes.ss, ID, True, False)
    assert isinstance(data, PaperDetails)
    data = s2._check_cache(ID)
    assert data.details and data.citations and data.references


@pytest.mark.inconsistent
def test_s2_cache_get_in_metadata_not_on_disk(s2, cache_files):
    ID = random.choice(cache_files)
    fpath = s2._cache_dir.joinpath(ID)
    os.remove(fpath)
    assert not fpath.exists()
    data = s2.get_details_for_id(ss.IdTypes.ss, ID, False, False)
    assert fpath.exists()
    assert isinstance(data, PaperDetails)
    assert data.paperId in s2._metadata


def test_s2_cache_get_ssid_when_not_in_metadata_and_disk(s2):
    line = delete_random_metadata_line(s2._cache_dir)
    s2 = ss.SemanticScholar(cache_dir="tests/cache_data/")
    key = [*json.loads(line).keys()][0]
    # doesn't exist
    assert key not in s2._metadata
    fpath = s2._cache_dir.joinpath(key)
    if fpath.exists():
        os.remove(fpath)
    data = s2.get_details_for_id(ss.IdTypes.ss, key, False, False)
    assert fpath.exists()
    assert isinstance(data, PaperDetails)
    assert data.paperId in s2._metadata


def test_s2_cache_get_other_than_ssid_and_data_not_in_metadata_and_disk(s2):
    arxiv_id = "2010.06775"
    id_name = s2.id_names[ss.IdTypes.arxiv]
    metadata = get_metadata(s2._cache_dir)
    if arxiv_id in s2._extid_metadata[id_name]:
        key = s2._extid_metadata[id_name][arxiv_id]
        metadata.remove(key)
        assert len(metadata) == 30
        with open(os.path.join(s2._cache_dir, "metadata.jsonl"), "w") as f:
            f.write("\n".join(metadata))
    else:
        key = None
    s2 = ss.SemanticScholar(cache_dir="tests/cache_data/")
    assert arxiv_id not in s2._extid_metadata[s2.id_names[ss.IdTypes.arxiv]]
    if key:
        fpath = s2._cache_dir.joinpath(key)
        if fpath.exists():
            os.remove(fpath)
    data = s2.get_details_for_id(ss.IdTypes.arxiv, arxiv_id, False, False)
    assert data.paperId
    fpath = s2._cache_dir.joinpath(data.paperId)
    assert fpath.exists()
    assert isinstance(data, PaperDetails)
    assert data.paperId in s2._metadata


# def test_s2_cache_get_other_than_ssid_and_data_in_metadata_and_disk(s2):
#     arxiv_id = "1908.03795"
#     data = s2.get_details_for_id("arxiv", arxiv_id, False)
#     with open(os.path.join(s2._cache_dir, "metadata")) as f:
#         metadata = f.read().split("\n")
#     if arxiv_id in s2._cache[s2.id_types("arxiv")]:
#         key = s2._cache[s2.id_types("arxiv")][arxiv_id]
#         metadata.remove(key)
#         assert len(metadata) == 30
#         with open(os.path.join(s2._cache_dir, "metadata"), "w") as f:
#             f.write("\n".join(metadata))
#     else:
#         key = None
#     s2 = SemanticScholar(cache_dir="tests/cache_data/")
#     assert arxiv_id not in s2._cache[s2.id_types("arxiv")]
#     if key:
#         fpath = s2._cache_dir.joinpath(key)
#         if fpath.exists():
#             os.remove(fpath)
#     assert "paperId" in data
#     fpath = s2._cache_dir.joinpath(data["paperId"])
#     assert fpath.exists()
#     assert isinstance(data, dict)
#     assert len(data) > 0
#     assert data["paperId"] in s2._metadata


@pytest.mark.inconsistent
def test_s2_details_fetches_correct_format_both_on_and_not_on_disk(s2, cache_files):
    fl = random.choice(cache_files)
    details = s2.get_details_for_id(ss.IdTypes.ss, fl, False, False)
    assert details.paperId and details.citations and details.references
    fl = "5d9e7dbf28382eb3d8e1bbd2cae6a1c8d223ce4a"
    if fl in cache_files:
        os.remove(f"tests/cache_data/{fl}")
        cache_files.remove(fl)
    details = s2.get_details_for_id(ss.IdTypes.ss, fl, False, False)
    assert details.paperId and details.citations and details.references


def test_s2_graph_search(s2):
    result = s2.search("breiman random forests")
    assert isinstance(result, dict)
    assert "error" not in result
    assert result["data"][0]["paperId"] == "8e0be569ea77b8cb29bb0e8b031887630fe7a96c"


# def test_s2_update_citation_count_before_writing(s2):
#     pass


def test_s2_get_corpus_id_from_references(s2):
    pass


def test_s2_fetch_data_with_no_transform_is_correct(s2, cache_files):
    ID = random.choice(cache_files)
    data = s2.fetch_from_cache_or_api(True, ID, False, True)
    assert data.details and data.references and data.citations
    assert data.details.paperId


def test_s2_load_metadata_invalid_entries_fixes_them(s2):
    pass


def test_s2_get_updated_paper_id(s2):
    pass


def test_s2_id_to_corpus_id_fails_for_invalid_id(s2):
    pass


def test_s2_id_to_corpus_id_correct_for_valid_id_already_in_cache(s2):
    pass


def test_s2_id_to_corpus_id_correct_for_valid_id_not_in_cache(s2):
    pass


def test_s2_get_citations_with_range(s2):
    key = "8e0be569ea77b8cb29bb0e8b031887630fe7a96c"
    # remove existing data
    if s2._cache_dir.joinpath(key).exists():
        os.remove(s2._cache_dir.joinpath(key))
    # fetch again with different citation limit
    old_limit = s2._config.citations.limit
    s2._config.citations.limit = 50
    _ = s2.paper_details(key)
    data = s2.citations(key, 50, 10)  # guaranteed to exist
    assert len(data) == 10
    assert isinstance(data[0], dict)
    s2._in_memory.pop(key)
    result = s2._check_cache(key)  # exists
    assert result is not None
    assert len(result.citations.data) == 60
    s2._config.citations.limit = old_limit


def test_s2_update_citations(s2):
    key = "8e0be569ea77b8cb29bb0e8b031887630fe7a96c"
    if s2._cache_dir.joinpath(key).exists():
        os.remove(f"tests/cache_data/{key}")
    result = s2.paper_details(key)
    result = s2._check_cache(key)
    assert result.citations.next is not None
    assert len(result.citations.data) == s2._config.citations.limit
    num_cites = len(result.citations.data)
    _ = s2.next_citations(key, 100)
    num_cites += 100
    result = s2._check_cache(key)
    assert len(result.citations.data) == num_cites
    _ = s2.next_citations(key, 100)
    num_cites += 100
    result = s2._check_cache(key)
    assert len(result.citations.data) == num_cites


@pytest.mark.corpus
def test_s2_data_fetch_refs(s2):
    assert bool(s2.corpus_cache)
    vals = s2.corpus_cache.get_citations(236227353)
    assert bool(vals)


@pytest.mark.corpus
def test_s2_data_build_citations_without_offset_limit(s2):
    # Has > 140 citations but < 200
    ID = 236511142
    vals = s2.corpus_cache.get_citations(ID)
    # Doesn't exist initially. Will fetch from API
    data = s2.get_details_for_id(ss.IdTypes.corpus, ID, False, False)
    existing_ids = [ss.get_corpus_id(PaperDetails(**x)) for x in data.citations]
    total_fetchable = len(set(vals).union(set(existing_ids)))
    citations = s2._build_citations_from_stored_data(corpus_id=ID,
                                                     existing_ids=existing_ids,
                                                     cite_count=data.citationCount)
    assert citations.offset == 0
    assert len(citations.data) == total_fetchable - len(existing_ids)
    assert hasattr(citations.data[0], "citingPaper") and hasattr(citations.data[0], "contexts")
    assert (set(s2._config.citations.fields) -
            citations.data[0].citingPaper.keys()) == {"contexts"}


def test_s2_ensure_and_update_all_citations_below_10000(s2):
    ID = "dd1c45eb999c23603aa61fd2806dde0e9cd3ada4"
    paper_data = s2.paper_data(ID)
    citation_count = paper_data.details.citationCount
    citations = s2._ensure_all_citations(ID)
    assert abs(len(citations.data) - citation_count) < 10
    new_citations = s2._update_citations(paper_data.citations, citations)
    paper_data.citations = new_citations
    assert abs(len(paper_data.citations.data) - citation_count) < 10
    new_citation_count = len(paper_data.citations.data)
    s2._dump_paper_data(ID, paper_data, force=True)
    for k, v in paper_data.details.externalIds.items():
        if s2.external_id_to_name(k) in s2._extid_metadata and str(v):
            s2._extid_metadata[s2.external_id_to_name(k)][str(v)] = ID
    s2._metadata[ID] = {s2.external_id_to_name(k): str(v)
                        for k, v in paper_data.details.externalIds.items()}
    s2.update_jsonl_metadata_on_disk(ID)
    s2._in_memory[ID] = paper_data
    s2._in_memory.pop(ID)
    paper_data = s2.paper_data(ID)
    assert len(paper_data.citations.data) == new_citation_count


def test_s2_citations_greater_than_10000(s2):
    pass


# def test_s2_data_build_citations_with_offset_limit(s2):
#     # Has > 140 citations but < 200
#     ID = 236511142
#     vals = s2.corpus_cache.get_citations(ID)
#     data = s2.get_details_for_id(ss.IdTypes.corpus, ID, False, False)
#     existing_ids = [ss.get_corpus_id(x) for x in data["citations"]]
#     total_fetchable = len(set(vals).union(set(existing_ids)))
#     citations = s2._build_citations_from_stored_data(corpus_id=ID,
#                                                      existing_ids=existing_ids,
#                                                      cite_count=data["citationCount"],
#                                                      offset=100,
#                                                      limit=50)
#     assert citations["offset"] == 0
#     assert len(citations["data"]) == 50
#     citations = s2._build_citations_from_stored_data(236511142, [], 100, 50)
#     assert citations["offset"] == 0
#     assert len(citations["data"]) == len(vals) % 50


# def test_s2_data_next_citations_below_10000(s2):
#     # Has > 140 citations but < 200
#     ssid = "8e0be569ea77b8cb29bb0e8b031887630fe7a96c"
#     offset = 11000
#     vals = s2.next_citations(ssid, offset=offset)
#     assert citations["offset"] == 0
#     assert len(citations["data"]) == 50
#     citations = s2._build_citations_from_stored_data(236511142, [], 100, 50)
#     assert citations["offset"] == 0
#     assert len(citations["data"]) == len(vals) % 50


# def test_s2_data_next_citations_above_10000(s2):
#     # Has > 140 citations but < 200
#     ssid = "8e0be569ea77b8cb29bb0e8b031887630fe7a96c"
#     offset = 11000
#     vals = s2.next_citations(ssid, offset=offset)
#     assert citations["offset"] == 0
#     assert len(citations["data"]) == 50
#     citations = s2._build_citations_from_stored_data(236511142, [], 100, 50)
#     assert citations["offset"] == 0
#     assert len(citations["data"]) == len(vals) % 50

