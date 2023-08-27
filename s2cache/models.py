import json
from typing import Optional
from pathlib import Path
import dataclasses
from dataclasses import dataclass, field


Metadata = dict[str, dict[str, str]]  # "Entry"
CitationData = dict[int, set]
Pathlike = str | Path


@dataclass
class Error:
    message: str
    error: str = "error"


@dataclass
class SubConfig:
    limit: int
    fields: list[str]


@dataclass
class Entry:
    DOI: str
    ARXIV: str
    MAG: str
    ACL: str
    PUBMED: str
    CorpusId: str
    DBLP: str
    URL: str


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
    batch_size: int = 500
    corpus_cache_dir: Optional[str] = None

    def __post_init__(self):
        self._keys = ["cache_dir", "corpus_cache_dir",
                      "search", "details",
                      "citations", "references", "author",
                      "author_papers", "api_key",
                      "batch_size"]
        if set([x.name for x in dataclasses.fields(self)]) != set(self._keys):
            raise AttributeError("self._keys should be same as fields")

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
class PaperDetails:
    paperId: str
    title: str
    citationCount: int
    influentialCitationCount: int
    authors: list[dict]
    abstract: str = ""
    venue: str = ""
    year: str = ""
    url: str = ""
    externalIds: dict[str, int | str] = field(default_factory=dict)
    citations: list["PaperDetails"] = field(default_factory=list)
    references: list["PaperDetails"] = field(default_factory=list)


@dataclass
class Citation:
    contexts: list[str]
    citingPaper: PaperDetails


@dataclass
class Citations:
    offset: int
    data: list[Citation]
    next: Optional[int] = None


@dataclass
class PaperData:
    details: PaperDetails
    citations: Citations
    references: Citations

    def __post_init__(self):
        self.details = PaperDetails(**self.details)  # type: ignore
        self.citations = Citations(**self.citations)  # type: ignore
        self.references = Citations(**self.references)  # type: ignore
