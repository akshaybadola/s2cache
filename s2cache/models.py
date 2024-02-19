from typing import Optional, cast
import json
from enum import auto, Enum
from pathlib import Path
import dataclasses
from dataclasses import dataclass, field

# from .api_models import DataConfig, DataParams



Metadata = dict[str, dict[str, str | int]]
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
    pubmedcentral = auto()
    url = auto()
    corpus = auto()
    ss = auto()
    dblp = auto()


NameToIds = {
    "DOI": IdTypes.doi,
    "MAG": IdTypes.mag,
    "ARXIV": IdTypes.arxiv,
    "ACL": IdTypes.acl,
    "PUBMED": IdTypes.pubmed,
    "PUBMEDCENTRAL": IdTypes.pubmedcentral,
    "URL": IdTypes.url,
    "corpusId": IdTypes.corpus,
    "SS": IdTypes.ss,
    "DBLP": IdTypes.dblp,
}

IdKeys = [x for x in NameToIds.keys() if x != "SS"]
IdKeys.sort()

IdPrefixes = {
    IdTypes.doi: "DOI",
    IdTypes.mag: "MAG",
    IdTypes.arxiv: "ARXIV",
    IdTypes.acl: "ACL",
    IdTypes.pubmed: "PMID",
    IdTypes.pubmedcentral: "PMCID",
    IdTypes.url: "URL",
    IdTypes.corpus: "CorpusId",
}


InternalFields = ["duplicateId"]
DetailsFields = ["tldr", "citations", "references"]
CitationsFields = ["contexts", "intents"]


@dataclass
class Error:
    message: str
    error: str = "error"


@dataclass
class Duplicate:
    old_id: list[str]
    preferred_id: str


@dataclass
class PaperDetails:
    """
    Details of the paper

    Args:
        paperId: Always included. A unique (string) identifier for this paper
        corpusId: A second unique (numeric) identifier for this paper
        url: URL on the Semantic Scholar website
        title: Included if no fields are specified
        venue: Normalized venue name
        publicationVenue: Publication venue meta-data for the paper
        year: Year of publication
        authors: Up to 500 will be returned. Will include: authorId & name
        externalIds: IDs from external sources: Supports ArXiv, MAG, ACL,
                    PubMed, Medline, PubMedCentral, DBLP, DOI
        abstract: The paper's abstract if available
        referenceCount: Total number of papers referenced by this paper
        citationCount: Total number of citations S2 has found for this paper
        influentialCitationCount: More information here
        isOpenAccess: More information here
        openAccessPdf: A link to the paper if it is open access, and we have a direct link to the pdf
        fieldsOfStudy: A list of high-level academic categories from external sources
        s2FieldsOfStudy: A list of academic categories
        publicationTypes: Journal Article, Conference, Review, etc
        publicationDate: YYYY-MM-DD, if available
        journal: Journal name, volume, and pages, if available
        citationStyles: Generates bibliographical citation of paper.
    """
    # Paper Info
    paperId: str
    title: str
    authors: list[dict]
    abstract: str
    venue: str
    year: str
    url: str
    corpusId: Optional[int] = None

    # citation and reference data
    referenceCount: Optional[int] = None
    citationCount: Optional[int] = None
    influentialCitationCount: Optional[int] = None

    # publication data
    journal: dict[str, str] = field(default_factory=dict)
    publicationVenue: dict[str, str] = field(default_factory=dict)
    publicationTypes: list[str] = field(default_factory=list)
    publicationDate: str = ""

    # pdf data
    isOpenAccess: bool = False
    openAccessPdf: dict[str, str] = field(default_factory=dict)

    # fields of study
    fieldsOfStudy: list[str] = field(default_factory=list)
    s2FieldsOfStudy: dict[str, str] = field(default_factory=dict)

    # misc
    citationStyles: dict[str, str] = field(default_factory=dict)
    tldr: dict[str, str] = field(default_factory=dict)

    # external ids
    externalIds: dict[str, int | str] = field(default_factory=dict)
    citations: list["PaperDetails"] = field(default_factory=list)
    references: list["PaperDetails"] = field(default_factory=list)
    duplicateId: Optional[str] = None

    # def __post_init__(self):
    #     """This post_init is only for backwards compatibility with JSONL files
    #     storage as that had :attr:`CorpusId` in :attr:`externalIds`.

    #     This initializes :attr:`CorpusId` from :attr:`externalIds`


    #     """
    #     self.CorpusId = cast(int, self.externalIds.get("CorpusId", None))


@dataclass
class DataParams:
    limit: int

    def __setitem__(self, k, v):
        setattr(self, k, v)

    def __getitem__(self, k):
        return getattr(self, k)


@dataclass
class DataConfig:
    details: DataParams
    references: DataParams
    citations: DataParams
    search: DataParams
    author: DataParams
    author_papers: DataParams

    def __setattr__(self, k, v):
        super().__setattr__(k, DataParams(**v))

    def __setitem__(self, k, v):
        setattr(self, k, v)

    def __getitem__(self, k):
        return getattr(self, k)


@dataclass
class Config:
    """The configuration dataclass

    In addition to :class:`SemanticScholar` attributes :code:`cache_dir`, :code:`api_key` etc.,
    this defines the detailed parameters when fetching data from the API.

    The parameters for :class:`SemanticScholar` are:

    - cache_dir: The directory for the papers cache
    - api_key: API key for Semantic Scholar API
    - batch_size: Batch size for fetching URLs in parallel. It's preferable not
      to make this very high as this may overload the API server and cause errors
    - client_timeout: aiohttp session client timeout
    - cache_backend: One of "jsonl" or "sqlite". Currently only these two are implemented.
      Right now "jsonl" is default.
    - corpus_cache_dir: Directory where the full citations corpus is stored.

    The field :code:`api` specifies the configuration for all the API calls.
    This is a :class:`dict` of the supported API calls and their parameters.

    Currently supported ones are:

    - details: corresponds to https://api.semanticscholar.org/graph/v1/paper/[PAPERID]
    - references: corresponds to https://api.semanticscholar.org/graph/v1/paper/[PAPERID]/references
    - citations: corresponds to https://api.semanticscholar.org/graph/v1/paper/[PAPERID]/citations
    - search: corresponds to https://api.semanticscholar.org/graph/v1/paper/search
    - author: corresponds to https://api.semanticscholar.org/graph/v1/author/[AUTHORID]
    - author_papers: corresponds to https://api.semanticscholar.org/graph/v1/author/[AUTHORID]/papers

    For each of these, a :code:`limit` can be given.

    .. admonition: Note

        While the API also supports a :code:`fields` keyword we always fetch ALL the fields
        and let the downstream application filter whichever fields it wants to keep.

    Args:
        cache_dir: str
        data: DataConfig
        api_key: Optional[str] = None
        batch_size: int = 500
        client_timeout: int = 10
        cache_backend: str = jsonl
        corpus_cache_dir: Optional[str] = None

    """
    cache_dir: str
    data: DataConfig
    api_key: Optional[str] = None
    batch_size: int = 500
    client_timeout: int = 10
    cache_backend: str = "jsonl"
    corpus_cache_dir: Optional[str] = None
    no_contexts_intents: Optional[bool] = False

    def __post_init__(self):
        self._keys = ["cache_dir", "corpus_cache_dir",
                      "api_key", "data",
                      "cache_backend", "batch_size", "client_timeout",
                      "no_contexts_intents"]
        if set([x.name for x in dataclasses.fields(self)]) != set(self._keys):
            raise AttributeError("self._keys should be same as fields")

    def __setattr__(self, k, v):
        if k == "data":
            super().__setattr__(k, DataConfig(**v))
        else:
            super().__setattr__(k, v)

    def __iter__(self):
        return iter(self._keys)

    def __setitem__(self, k, v):
        setattr(self, k, v)

    def __getitem__(self, k):
        return getattr(self, k)


@dataclass
class AuthorDetails:
    """Author Details. The fields are same as for https://api.semanticscholar.org/graph/v1/author

    Args:
        authorId: S2 unique ID for this author
        externalIds: ORCID/DBLP IDs for this author, if known
        url: URL on the Semantic Scholar website
        name: Author's name
        aliases: List of names the author has used on publications over time
        affiliations: Author's affiliations
        homepage: Author's homepage
        paperCount: Author's total publications count
        citationCount: Author's total citations count
        hIndex: See the S2 FAQ on h-index
        papers: List of author's papers
    """
    authorId: str
    externalIds: dict
    url: str
    name: str
    aliases: list[str]
    affiliations: list
    homepage: str
    paperCount: int
    citationCount: int
    hIndex: int


@dataclass
class Citation:
    contexts: list[str]
    intents: list[str]
    citingPaper: PaperDetails


@dataclass
class Reference:
    contexts: list[str]
    intents: list[str]
    citedPaper: PaperDetails


@dataclass
class References:
    offset: int
    data: list[Reference]
    next: Optional[int] = None


@dataclass
class Citations:
    offset: int
    data: list[Citation]
    next: Optional[int] = None


@dataclass
class PaperData:
    details: PaperDetails
    citations: Citations
    references: References

    def __post_init__(self):
        if isinstance(self.details, dict):
            self.details = PaperDetails(**self.details)
        if isinstance(self.citations, dict):
            self.citations = Citations(**self.citations)
        if isinstance(self.references, dict):
            self.references = References(**self.references)


def _maybe_fix_citation_data(citation_data, citation_type, key):
    if isinstance(citation_data.data[0], dict):
        data = []
        for x in citation_data.data:
            try:
                if "contexts" not in x:
                    x["contexts"] = []
                if "intents" not in x:
                    x["intents"] = []
                data.append(citation_type(**x))
            except Exception:
                pass
        citation_data.data = data
    citation_data.data = list(filter(lambda x: getattr(x, key), citation_data.data))
