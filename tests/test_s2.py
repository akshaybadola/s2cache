import pytest
import time
import os
import json
import random
from s2cache.models import PaperDetails, PaperData
from s2cache import semantic_scholar as ss

from util import (get_random_ID, check_ID_in_store,
                  remove_ID_from_store, remove_ID_from_memory,
                  remove_random_item_from_store, remove_random_item_from_metadata)


def test_s2_init(s2):
    assert s2._metadata is not None
    assert s2._extid_metadata is not None
    assert len(s2._metadata) == 31


def test_s2_cache_get_details_when_ID_in_store(s2):
    ID = get_random_ID(s2)
    data = s2.get_details_for_id("SS", ID, False, False)
    assert isinstance(data, PaperDetails)


@pytest.mark.inconsistent
def test_s2_cache_force_get_details_when_ID_in_store(s2):
    ID = get_random_ID(s2)
    data = s2.get_details_for_id("SS", ID, True, False)
    assert isinstance(data, PaperDetails)
    data = s2._check_cache(ID)
    assert data.details and data.citations and data.references


@pytest.mark.inconsistent
def test_s2_cache_get_when_ID_in_metadata_but_not_in_store(s2):
    ID = remove_random_item_from_store(s2)
    data = s2.get_details_for_id("SS", ID, False, False)
    time.sleep(0.2)
    assert check_ID_in_store(s2, ID)
    assert isinstance(data, PaperDetails)
    assert data.paperId in s2._metadata


def test_s2_cache_get_ssid_when_not_in_metadata_and_store(s2):
    ID = remove_random_item_from_metadata(s2)
    remove_ID_from_memory(s2, ID)
    remove_ID_from_store(s2, ID)
    assert ID not in s2._metadata
    data = s2.get_details_for_id("SS", ID, False, False)
    assert check_ID_in_store(s2, ID)
    assert isinstance(data, PaperDetails)
    assert data.paperId in s2._metadata


def test_s2_cache_get_ID_other_than_ssid_when_data_not_in_metadata_and_store(s2):
    arxiv_id = "2010.06775"
    id_name = ss.id_to_name("arxiv")
    if arxiv_id in s2._extid_metadata[id_name]:
        ID = s2._extid_metadata[id_name][arxiv_id]
        remove_ID_from_memory(s2, ID)
        remove_ID_from_store(s2, ID)
    else:
        ID = None
    assert arxiv_id not in s2._extid_metadata[ss.id_to_name("arxiv")]
    data = s2.get_details_for_id("arxiv", arxiv_id, False, False)
    ID = s2._extid_metadata[id_name][arxiv_id]
    assert data.paperId
    assert check_ID_in_store(s2, ID)
    assert isinstance(data, PaperDetails)
    assert data.paperId in s2._metadata


def test_s2_cache_get_ID_other_than_ssid_when_data_in_metadata_but_not_in_store(s2):
    arxiv_id = "1908.03795"
    id_name = ss.id_to_name("arxiv")
    data = s2.get_details_for_id("arxiv", arxiv_id, False, False)
    ID = s2._extid_metadata[id_name][arxiv_id]
    remove_ID_from_memory(s2, ID)
    remove_ID_from_store(s2, ID)
    assert arxiv_id in s2._extid_metadata[ss.id_to_name("arxiv")]
    assert ID in s2._metadata
    assert not check_ID_in_store(s2, ID)
    data = s2.get_details_for_id("arxiv", arxiv_id, False, False)
    assert check_ID_in_store(s2, ID)
    assert isinstance(data, PaperDetails)
    assert data.paperId in s2._metadata


@pytest.mark.inconsistent
def test_s2_details_fetches_correct_format_when_both_in_and_not_in_store(s2):
    ID = get_random_ID(s2)
    details = s2.get_details_for_id("SS", ID, False, False)
    time.sleep(0.2)
    assert details.paperId and details.citations and details.references
    ID = "5d9e7dbf28382eb3d8e1bbd2cae6a1c8d223ce4a"
    remove_ID_from_memory(s2, ID)
    remove_ID_from_store(s2, ID)
    details = s2.get_details_for_id("SS", ID, False, False)
    assert details.paperId and details.citations and details.references


def test_s2_graph_search(s2):
    result = s2.search("breiman random forests")
    assert isinstance(result, dict)
    assert "error" not in result
    assert result["data"][0]["paperId"] == "8e0be569ea77b8cb29bb0e8b031887630fe7a96c"


def test_s2_update_citation_count_before_writing(s2):
    pass


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
    ID = "c1229904319da71ac87d80c90976666944b4323f"
    data = s2.paper_details(ID)
    assert data.paperId != ID
    assert data.duplicateId == ID
    cached_data = s2._cache_backend.get_paper_data(data.paperId, True)
    assert cached_data is not None
    assert cached_data["details"]["duplicateId"] is None
    # fetch again but from cache
    data = s2.paper_details(ID)
    assert data.paperId != ID
    assert data.duplicateId == ID
    # now fetch the ID duplicate points to
    ID = data.paperId
    data = s2.paper_details(ID)
    assert data.paperId == ID
    assert data.duplicateId is None


def test_s2_id_to_corpus_id_fails_for_invalid_id(s2):
    pass


def test_s2_id_to_corpus_id_correct_for_valid_id_already_in_cache(s2):
    pass


def test_s2_id_to_corpus_id_correct_for_valid_id_not_in_cache(s2):
    pass


def test_s2_get_citations_with_range(s2):
    ID = "8e0be569ea77b8cb29bb0e8b031887630fe7a96c"
    # remove existing data
    remove_ID_from_memory(s2, ID)
    remove_ID_from_store(s2, ID)
    # fetch again with different citation limit
    old_limit = s2._config.citations.limit
    s2._config.citations.limit = 50
    _ = s2.paper_details(ID)
    data = s2.citations(ID, 50, 10)  # guaranteed to exist
    assert len(data) == 10
    assert isinstance(data[0], dict)
    s2._in_memory.pop(ID)
    result = s2._check_cache(ID)  # exists
    assert result is not None
    assert len(result.citations.data) == 60
    s2._config.citations.limit = old_limit


def test_s2_update_citations(s2):
    ID = "8e0be569ea77b8cb29bb0e8b031887630fe7a96c"
    # remove existing data
    remove_ID_from_memory(s2, ID)
    remove_ID_from_store(s2, ID)
    result = s2.paper_details(ID)
    result = s2._check_cache(ID)
    assert result.citations.next is not None
    assert len(result.citations.data) == s2._config.citations.limit
    num_cites = len(result.citations.data)
    _ = s2.next_citations(ID, 100)
    num_cites += 100
    result = s2._check_cache(ID)
    assert len(result.citations.data) == num_cites
    _ = s2.next_citations(ID, 100)
    num_cites += 100
    result = s2._check_cache(ID)
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
    data = s2.get_details_for_id("corpusid", ID, False, False)
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
    s2.store_paper_data(ID, paper_data, force=True)
    for k, v in paper_data.details.externalIds.items():
        if ss.id_to_name(k) in s2._extid_metadata and str(v):
            s2._extid_metadata[ss.id_to_name(k)][str(v)] = ID
    s2._metadata[ID] = {ss.id_to_name(k): str(v)
                        for k, v in paper_data.details.externalIds.items()}
    s2.update_paper_metadata(ID)
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

