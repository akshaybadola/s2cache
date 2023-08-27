import pytest
from s2cache.models import PaperDetails


def test_filters_citations(s2):
    key = "a925f818f787e142c5f6bcb7bbd7ede2deb34860"
    details = s2.paper_details(key)
    assert len(details.citations) == 100
    result = s2.filter_citations(key, filters={"citationcount": {"min": 10, "max": 10000}})
    assert len(result) > 0
    assert result[0]
    assert isinstance(result[0], PaperDetails)
    assert result[0].paperId


def test_filters_references(s2):
    key = "a925f818f787e142c5f6bcb7bbd7ede2deb34860"
    details = s2.paper_details(key)
    assert len(details.citations) == 100
    result = s2.filter_citations(key, filters={"citationcount": {"min": 10, "max": 10000}})
    assert len(result) > 0
    assert result[0]
    assert isinstance(result[0], PaperDetails)
    assert result[0].paperId

