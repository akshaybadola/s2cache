from typing import Optional
from pathlib import Path
import dataclasses
from dataclasses import dataclass


Cache = dict[str, dict[str, str]]
CitationData = dict[int, set]
Pathlike = str | Path


@dataclass
class SubConfig:
    limit: int
    fields: list[str]


@dataclass
class Config:
    search: SubConfig
    details: SubConfig
    citations: SubConfig
    references: SubConfig
    author: SubConfig
    author_papers: SubConfig
    cache_dir: str
    api_key: Optional[str] = None
    cite_data_dir: Optional[str] = None

    def __post_init__(self):
        self._keys = ["cache_dir", "cite_data_dir",
                      "search", "details",
                      "citations", "references", "author",
                      "author_papers", "api_key"]
        if set([x.name for x in dataclasses.fields(self)]) != set(self._keys):
            raise AttributeError("keys should be same as fields")

    def __setattr__(self, k, v):
        if k == "api_key":
            super().__setattr__(k, v)
        else:
            if isinstance(v, dict):
                super().__setattr__(k, SubConfig(**v))
            else:
                super().__setattr__(k, v)

    def __iter__(self):
        return iter(self._keys)

    def __setitem__(self, k, v):
        setattr(self, k, v)

    def __getitem__(self, k):
        return getattr(self, k)


@dataclass
class Details:
    paperId: str
    title: str
    citationCount: int
    influentialCitationCount: int
    venue: str
    year: str
    authors: list[dict]
    externalIds: dict[str, int | str]
    citations: Optional["Citations"] = None
    references: Optional["Citations"] = None


@dataclass
class Citation:
    contexts: list[str]
    citingPaper: Details


@dataclass
class Citations:
    next: int
    offset: int
    data: list[Citation]


@dataclass
class StoredData:
    details: Details
    citations: Citations
    references: Citations
