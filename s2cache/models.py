import json
from enum import auto, Enum
from typing import Optional
from pathlib import Path
import dataclasses
from dataclasses import dataclass, field


Metadata = dict[str, dict[str, str]]  # "Entry"
CitationData = dict[int, set]
Pathlike = str | Path


class IdTypes(Enum):
    """:class:`Enum` of types of Paper IDs

    These have changed as the API spec has evolved. We keep track of the following

    :code:`["acl", "arxiv", "corpus", "doi", "mag", "url", "dblp", "pubmed"]`

    """
    doi = auto()
    mag = auto()
    arxiv = auto()
    acl = auto()
    pubmed = auto()
    url = auto()
    corpus = auto()
    ss = auto()
    dblp = auto()


IdNames = {
    "DOI": IdTypes.doi,
    "MAG": IdTypes.mag,
    "ARXIV": IdTypes.arxiv,
    "ACL": IdTypes.acl,
    "PUBMED": IdTypes.pubmed,
    "URL": IdTypes.url,
    "CorpusId": IdTypes.corpus,
    "SS": IdTypes.ss,
    "DBLP": IdTypes.dblp,
}

IdKeys = [x for x in IdNames.keys() if "corpus" not in x.lower() or x == "SS"]
IdKeys.append("CorpusId")
IdKeys.sort()


@dataclass
class Error:
    message: str
    error: str = "error"


@dataclass
class Duplicate:
    old_id: list[str]
    preferred_id: str


@dataclass
class SubConfig:
    limit: int
    fields: list[str]

    def __setitem__(self, k, v):
        setattr(self, k, v)

    def __getitem__(self, k):
        return getattr(self, k)



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
    """The configuration dataclass

    In addition to :class:`SemanticScholar` attributes :code:`cache_dir`, :code:`api_key` etc.,
    this defines the detailed parameters when fetching from the API.

    The first six parameters correspond to fields of the API calls.
    Currently implemented ones are:

    - details: corresponds to https://api.semanticscholar.org/graph/v1/paper/[PAPERID]
    - references: corresponds to https://api.semanticscholar.org/graph/v1/paper/[PAPERID]/references
    - citations: corresponds to https://api.semanticscholar.org/graph/v1/paper/[PAPERID]/citations
    - search: corresponds to https://api.semanticscholar.org/graph/v1/paper/search
    - author: corresponds to https://api.semanticscholar.org/graph/v1/author/[AUTHORID]
    - author_papers: corresponds to https://api.semanticscholar.org/graph/v1/author/[AUTHORID]/papers

    The rest of the parameters are:

    - cache_dir: The directory for the papers cache
    - api_key: API key for Semantic Scholar API
    - batch_size: Batch size for fetching URLs in parallel. It's preferable not
      to make this very high as this may overload the API server and cause errors
    - client_timeout: aiohttp session client timeout
    - cache_backend: One of "jsonl" or "sqlite". Currently only these two are implemented.
      Right now "jsonl" is default.
    - corpus_cache_dir: Directory where the full citations corpus is stored.

    For each of those API calls the fields, limit etc. can be customized.
    A default config is generated if not defined with :func:`s2cache.config.default_config`


    Args:
        search: SubConfig
        details: SubConfig
        citations: SubConfig
        references: SubConfig
        author: SubConfig
        author_papers: SubConfig
        cache_dir: str
        api_key: Optional[str] = None
        batch_size: int = 500
        client_timeout: int = 10
        cache_backend: str = jsonl
        corpus_cache_dir: Optional[str] = None


    """
    search: SubConfig
    details: SubConfig
    citations: SubConfig
    references: SubConfig
    author: SubConfig
    author_papers: SubConfig
    cache_dir: str
    api_key: Optional[str] = None
    batch_size: int = 500
    client_timeout: int = 10
    cache_backend: str = "jsonl"
    corpus_cache_dir: Optional[str] = None

    def __post_init__(self):
        self._keys = ["cache_dir", "corpus_cache_dir",
                      "search", "details",
                      "citations", "references", "author",
                      "author_papers", "api_key",
                      "cache_backend", "batch_size", "client_timeout"]
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
    duplicateId: Optional[str] = None


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


def _maybe_fix_citation_data(citation_data):
    if isinstance(citation_data.data[0], dict):
        data = []
        for x in citation_data.data:
            try:
                data.append(Citation(**x))  # type: ignore
            except Exception:
                pass
        citation_data.data = data
