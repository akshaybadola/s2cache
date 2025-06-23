"""This primary means of interacting with Semantic Scholar API.

The class of concern is :class:`SemanticScholar`. It stores each paper
fetched via API into a local JSON file and the metadata in a JSON lines file.

The :attr:`Metadata` is a :class:`dict` with :code:`paperId` as key and :code:`externalId`s
as values so that any ID associated with the paper can be used to retrieve it.

Currently we stored :code:`["acl", "arxiv", "corpus", "doi", "mag", "url", "dblp", "pubmed"]`.
More can be added as required.

A paper lookup can be performed by giving the ID and the type of ID.

"""

from typing import Optional, Callable, Any, cast
import os
import re
import json
import math
import time
import random
import logging
from pathlib import Path
import requests
import asyncio
import dataclasses

import aiohttp
from common_pyutil.monitor import Timer

from .models import (Pathlike, Metadata, ExtIDMetadata, Config, PaperDetails, AuthorDetails,
                     Reference, References, Citation, Citations, PaperData, Error,
                     IdTypes, NameToIds, IdKeys, IdPrefixes,
                     InternalFields, DetailsFields, CitationsFields)
from .filters import (year_filter, author_filter, num_citing_filter,
                      num_influential_count_filter, venue_filter, title_filter)
from .jsonl_backend import JSONLBackend
from .sqlite_backend import SQLiteBackend
from .corpus_data import PapersCache, CitationsCache, ReferencesCache
from .config import default_config, load_config
from .util import dumps_json, id_to_name, field_names, _maybe_fix_citation_data


_timer = Timer()


def get_corpus_id(data: Reference | Citation | PaperDetails) -> Optional[int]:
    """Get :code:`corpusId` field from :class:`Citation` or :class:`PaperDetails`

    Args:
        data: PaperDetails or Citation data


    """
    if isinstance(data, PaperDetails):
        if data.corpusId is not None:
            return data.corpusId
    elif isinstance(data, Reference):
        return data.citedPaper.corpusId
    elif isinstance(data, Citation):
        return data.citingPaper.corpusId
    return None


def get_existing_corpus_ids(data: list[Citation] | list[Reference] | list[PaperDetails]) ->\
        list[int]:
    result = []
    for x in data:
        cid = get_corpus_id(x)
        if cid is not None:
            result.append(int(cid))
    return result


# FIXME: 1. Corpusid is upppercase in metadata while I still seem to be accessing
#           it in lower case (FIXED)
#        2. The metadata structure is (paperid, corpusid), (corpusid, *external_ids)
#           This is creating conflicts
#        3. Corpusid type is inconsistent (int/str)
#           NOTE: I think sqlite converts int to str automatically though I should fix that.
class SemanticScholar:
    """A Semantic Scholar API client with a files based cache.

    :attr:`cache_dir` is the only one required for the client (and the cache)
    to be initialized. Rest are configurable according to preferences.

    Both :attr:`cache_dir` and :attr:`citations_cache_dir` are assumed to exist
    beforehand and an error will be raised if not found.

    Each file in the :attr:`cache_dir` is stored as JSON with the filename same
    as semanticscholar paper hash, or :code:`paperId` without any extension.
    If there are already files in the :attr:`cache_dir`, they are all
    assumed to be paper data files and the library will try to load them
    and build metadata for them.


    .. admonition:: Note

        Any arguments given in :meth:`__init__` will override those
        given in the config file


    Args:
        cache_dir: The directory where all the metadata and the
            files and data are/will be kept
        api_key: Optional API_KEY for Semantic Scholar API
        citations_cache_dir: Optional Cache directory for Semantic Scholar Citation Data
        config_file: Config File path
        logger_name: Logger name for logging.
            The logger output and formatter etc. have to be pre-configured.
            This is a convienence var to hook into the logger.


    """

    @property
    def filters(self) -> dict[str, Callable]:
        """Allowed filters on the entries.

        ["year", "author", "num_citing", "influential_count", "venue", "title"]

        """
        _filters: dict[str, Callable] = {"year": year_filter,
                                         "author": author_filter,
                                         "num_citing": num_citing_filter,
                                         "citationcount": num_citing_filter,
                                         "influential_count": num_influential_count_filter,
                                         "influentialcitationcount": num_influential_count_filter,
                                         "venue": venue_filter,
                                         "title": title_filter}
        return _filters

    @property
    def old_fields_map(self):
        """Fields mapping from old to new.

        Sometimes SemanticScholar throws up a surprise and sends us data in old format. LOL


        """
        return {
            'citationVelocity': None,
            'is_open_access': 'isOpenAccess',
            'is_publisher_licensed': None,
            'isPublisherLicensed': None,
            'numCitedBy': 'citationCount',
            'numCiting': 'referenceCount',
            'topics': None,
            'arxivId': ('externalIds', 'ArXiv'),
            'doi': ('externalIds', 'DOI'),
            'corpusId': ('externalIds', 'CorpusId'),
        }

    def __init__(self, *,
                 cache_dir: Optional[Pathlike] = None,
                 api_key: Optional[str] = None,
                 batch_size: Optional[int] = None,
                 citations_cache_dir: Optional[Pathlike] = None,
                 config_or_file: dict | Pathlike | None = None,
                 cache_backend: Optional[str] = None,
                 client_timeout: Optional[int] = None,
                 logger_name: Optional[str] = None):
        """Semantic Scholar

        .. admonition:: Note

            Any arguments given in :meth:`__init__` will override those
            given in the config file

        Args:
            cache_dir: The directory where all the metadata and the
            files and data are/will be kept
            api_key: Optional API_KEY for Semantic Scholar API
            citations_cache_dir: Optional Cache directory for Semantic Scholar Citation Data
            config_file: Config File path
            client_timeout: Timeout for client session
            logger_name: Logger name for logging.
                The logger output and formatter etc. have to be pre-configured.
                This is a convienence var to hook into the logger.

        """
        self._config = default_config()
        self._cache_dir = cache_dir
        self._api_key = api_key
        self._batch_size = batch_size
        self._citations_cache_dir = citations_cache_dir
        self._client_timeout: int | None = client_timeout
        self._cache_backend_name = cache_backend
        self._logger_name = logger_name or "s2cache"
        self.logger = logging.getLogger(self._logger_name)
        if config_or_file:
            load_config(self._config, config_or_file)
        self._init_some_vars()
        self._init_cache()
        self.initialize_backend()
        self.load_metadata()
        self.maybe_load_corpus_cache()
        self.maybe_init_papers_cache()

    def _init_some_vars(self):
        """Initialize some config and internal variables

        """
        self._api_key = self._api_key or self._config.api_key
        self._root_url = "https://api.semanticscholar.org/graph/v1"
        self._batch_size = self._batch_size or self.config.batch_size
        self._tolerance = 10
        self._dont_build_citations: set = set()
        self._client_timeout = self._client_timeout or self.config.client_timeout
        self._cache_backend_name = self._cache_backend_name or self.config.cache_backend
        self._aio_timeout = aiohttp.ClientTimeout(self._client_timeout)
        # TODO: Replace with dict[int, Externalids]
        self._metadata: Metadata = {}
        self._extid_metadata: ExtIDMetadata = {}
        self._known_duplicates: dict[str, str] = {}
        self._inferred_duplicates: dict[int, list[str]] = {}
        self._inferred_duplicates_map: dict[str, int] = {}
        self._corpus_map: dict[str, int] = {}
        self._details_fields = [x.name for x in dataclasses.fields(PaperDetails)
                                if x.name not in InternalFields and x.name not in CitationsFields
                                and x.name not in {"references", "citations"}]
        self._citations_fields = [x.name for x in dataclasses.fields(PaperDetails)
                                  if x.name not in InternalFields and x.name not in DetailsFields]
        self._author_fields = [x.name for x in dataclasses.fields(AuthorDetails)]
        self.citations_fields.extend(CitationsFields)
        if self._config.no_contexts_intents:
            self.logger.warn("Removing \"contexts\" and \"intents\" from citations"
                             " and references due to S2 timeouts")
            self.citations_fields.remove("contexts")
            self.citations_fields.remove("intents")

    def _init_cache(self):
        """Initialize the cache from :code:`cache_dir`

        Args:
            cache_dir: The directory where cache files reside


        """
        _cache_dir = (self._cache_dir or self.config.cache_dir)
        if not _cache_dir:
            raise ValueError("Empty cache dir given")
        elif not Path(_cache_dir).exists():
            os.makedirs(_cache_dir)
            self.logger.debug(f"Creating cache dir {_cache_dir}")
        self._cache_dir = Path(_cache_dir)
        self._in_memory: dict[str, PaperData] = {}
        self._rev_cache: dict[str, list[str]] = {}

    @property
    def known_duplicates(self):
        """:code:`dict` of known duplicate :code:`paperId`s.
        Keys are duplicate ids and values are canonical ids

        """
        return self._known_duplicates

    @property
    def inferred_duplicates(self):
        """:code:`dict` of duplicates inferred from database.
        Keys are :code:`corpusid`s and values are list of :code:`paperId`s

        """
        return self._inferred_duplicates

    @property
    def inferred_duplicates_map(self):
        """:code:`dict` mapping ssid to corpusId for inferred duplicates
        from database
        """
        return self._inferred_duplicates_map

    @property
    def corpus_map(self):
        """:code:`dict` mapping ssid to corpusId
        from database
        """
        return self._corpus_map

    @property
    def rev_corpus_map(self):
        """:code:`dict` mapping ssid to corpusId
        from database
        """
        return self._rev_corpus_map

    @property
    def metadata(self) -> Metadata:
        """:code:`dict` of :code:`paperId` and :code:`externalId`s
        """
        return self._metadata

    @property
    def details_fields(self) -> list[str]:
        """List of default fields to select for paper details
        """
        return self._details_fields

    @property
    def citations_fields(self) -> list[str]:
        """List of default fields to select for citations
        """
        return self._citations_fields

    @property
    def author_fields(self) -> list[str]:
        """List of default fields to select for citations
        """
        return self._author_fields

    def initialize_backend(self):
        if self._cache_backend_name == "jsonl":
            self._cache_backend: JSONLBackend | SQLiteBackend =\
                JSONLBackend(self._cache_dir, self._logger_name)  # type: ignore
        elif self._cache_backend_name == "sqlite":
            self._cache_backend = SQLiteBackend(self._cache_dir, self._logger_name)  # type: ignore
            self._cache_backend._create_dbs()
        else:
            raise ValueError(f"Unkonwn backend {self._cache_backend_name}")

    def load_metadata(self):
        self._metadata, self._known_duplicates, self._inferred_duplicates, self._corpus_map =\
            self._cache_backend.load_metadata()
        self._extid_metadata = {k: {} for k in IdKeys if k.lower() != "ss"}
        self._rev_corpus_map = {v: k for k, v in self._corpus_map.items()}
        if self._metadata:
            for corpus_id, extids in self._metadata.items():
                for idtype, ID in extids.items():
                    idtype = id_to_name(idtype)
                    if ID and id_to_name(idtype) in self._extid_metadata:
                        self._extid_metadata[idtype][ID] = corpus_id
        for k, v in self._inferred_duplicates.items():
            for _v in v:
                self._inferred_duplicates_map[_v] = k

    def get_paper_data(self, ID: str, quiet: bool = False) -> Optional[dict]:
        if self._cache_backend_name == "jsonl":
            return self._cache_backend.get_paper_data(ID=ID, quiet=quiet)
        else:
            if not isinstance(self._cache_backend, SQLiteBackend):
                raise TypeError
            details = self._cache_backend.get_paper_data(ID=ID, quiet=quiet)
            if details is None:
                return None
            references_citations = self._cache_backend.get_references_and_citations(ID)
            references = [{"citedPaper": self._cache_backend.get_paper_data(x["citedPaper"])}
                          for x in references_citations["references"]]
            citations = [{"citingPaper": self._cache_backend.get_paper_data(x["citingPaper"])}
                         for x in references_citations["citations"]]
            return {"details": details,
                    "references": {"offset": 0, "next": None, "data": references},
                    "citations": {"offset": 0, "next": None, "data": citations}}

    def store_paper_data(self, ID: str, data: PaperData, force: bool = False):
        self._cache_backend.dump_paper_data(ID, data, force)

    def rebuild_metadata(self):
        self._cache_backend.rebuild_metadata()

    def update_paper_metadata(self, ID: str):
        self._cache_backend.update_metadata(ID, self._metadata[self.corpus_map[ID]])

    def update_duplicates_metadata(self, ID: str):
        self._cache_backend.update_duplicates_metadata(ID, self.known_duplicates[ID])

    @property
    def client_timeout(self) -> int:
        """Timeout for the client."""
        if self._client_timeout is None:
            raise TypeError("Client timeout should not be None")
        return self._client_timeout

    @client_timeout.setter
    def client_timeout(self, x):
        self._client_timeout = x
        self._aio_timeout = aiohttp.ClientTimeout(self._client_timeout)

    @property
    def config(self) -> Config:
        """Configuration object.

        See :class:`models.Config`

        """
        return self._config

    @property
    def batch_size(self) -> int:
        """Batch size for fetching urls in batches
        """
        if self._batch_size is None:
            raise TypeError("Batch size should not be None")
        return self._batch_size

    @property
    def tolerance(self) -> int:
        """Difference allowed between citations fetched and :code:`citationCount` given by
        S2 API.
        """
        return self._tolerance

    @property
    def headers(self) -> dict[str, str]:
        """Extra headers to include in the requests. Primarily the api key.

        """
        if self._api_key:
            return {"x-api-key": self._api_key}
        else:
            return {}

    @property
    def citations_cache(self) -> Optional[CitationsCache]:
        """Return the Citations Cache if it exists

        """
        return self._citations_cache

    @property
    def refs_cache(self) -> Optional[ReferencesCache]:
        """Return the References Cache if it exists

        """
        return self._refs_cache

    @property
    def citations_cache_dir(self) -> Optional[Path]:
        """Directory where the FULL citation data from Semantic Scholar is stored.

        The data has to be parsed and indexed by :class:`CitationsCache`.
        This is used only when the :code:`citationCount` > 10000 as that
        is the SemanticScholar limit
        """
        return cast(Path, self._citations_cache_dir)

    @property
    def cache_dir(self) -> Path:
        """Directory where the local paper cache files are kept"""
        return cast(Path, self._cache_dir)

    @property
    def all_paper_ids(self) -> list[str]:
        """Return a list of all :code:`paperId` of papers that are stored in cache"""
        return list(self.corpus_map.keys())

    def maybe_load_corpus_cache(self):
        """Load :class:`CitationsCache` if given
        """
        # prefer init arg over config
        citations_cache_dir = self._citations_cache_dir or self.config.citations_cache_dir
        if citations_cache_dir is not None:
            citations_cache_dir = Path(citations_cache_dir)
        else:
            citations_cache_dir = None
        self._citations_cache_dir = citations_cache_dir
        refs_cache_file = self.config.references_cache
        if citations_cache_dir and Path(citations_cache_dir).exists():
            self._citations_cache: CitationsCache | None = CitationsCache(citations_cache_dir)
            self.logger.debug(f"Loaded Full Semantic Scholar Citations Cache from {citations_cache_dir}")
        else:
            self._citations_cache = None
            self.logger.debug("Citation Corpus Cache doesn't exist. Not loading")
        if refs_cache_file and Path(refs_cache_file).exists():
            self._refs_cache = ReferencesCache(refs_cache_file)
            self.logger.debug(f"Loaded Semantic Scholar Reference Cache from {refs_cache_file}")
        else:
            self._refs_cache = None
            self.logger.debug("Refs Cache doesn't exist. Not loading")

    def maybe_init_papers_cache(self):
        if self.config.papers_cache_params is not None:
            self._papers_cache = PapersCache(**self.config.papers_cache_params)

    def _update_references_from_existing_to_new(self, existing_reference_data: References,
                                                new_reference_data: References) -> References:
        """Update from :code:`existing_reference_data` from :code:`new_reference_data`

        Args:
            existing_reference_data: Existing data
            new_reference_data: New data

        """
        if not existing_reference_data.data and not new_reference_data.data:
            return new_reference_data
        if existing_reference_data.data:
            _maybe_fix_citation_data(existing_reference_data, Reference, "citedPaper")
        if new_reference_data.data:
            _maybe_fix_citation_data(new_reference_data, Reference, "citedPaper")
        new_data_ids = {x.citedPaper.paperId
                        for x in new_reference_data.data if x.citedPaper}
        for x in existing_reference_data.data:
            if x.citedPaper and x.citedPaper.paperId and\
               x.citedPaper.paperId not in new_data_ids:
                new_reference_data.data.append(x)
                if new_reference_data.next:
                    new_reference_data.next += 1
        return new_reference_data

    def _update_citations_from_existing_to_new(self, existing_citation_data: Citations,
                                               new_citation_data: Citations) -> Citations:
        """Update from :code:`existing_citation_data` from :code:`new_citation_data`

        Args:
            existing_citation_data: Existing data
            new_citation_data: New data

        """
        if not existing_citation_data.data and not new_citation_data.data:
            return new_citation_data
        if existing_citation_data.data:
            _maybe_fix_citation_data(existing_citation_data, Citation, "citingPaper")
        if new_citation_data.data:
            _maybe_fix_citation_data(new_citation_data, Citation, "citingPaper")
        new_data_ids = {x.citingPaper.paperId
                        for x in new_citation_data.data if x.citingPaper}
        for x in existing_citation_data.data:
            if x.citingPaper and x.citingPaper.paperId and\
               x.citingPaper.paperId not in new_data_ids:
                new_citation_data.data.append(x)
                if new_citation_data.next:
                    new_citation_data.next += 1
        return new_citation_data

    def _check_duplicate(self, ID) -> tuple[str, str | None]:
        duplicate_id = None
        if ID in self.known_duplicates:
            duplicate_id = ID
            ID = self.known_duplicates[ID]
            self.logger.debug(f"ID {duplicate_id} is duplicate of {ID}")
        return ID, duplicate_id

    def _update_paper_details_in_backend(self, ID: str, data: PaperData,
                                         quiet: bool = False,
                                         discard_existing: bool = False) -> Error | None:
        """Update paper details, references and citations on backend.

        We read and write data for individual papers instead of one big json
        object. The data is stored in the backend according to the backend.

        If it is :class:`jsonl_backend.JSONLBackend`, the each paper is stored
        as a dictionary with keys :code:`["details", "references", "citations"]`.

        If it is :class:`sqlite_backend.SQLiteBackend`, then the data for the
        paper is stored as a dictionary, and only paperIDs of citations and
        references are stored.  See the documentation of individual backend for
        details.

        Args:
            ID: PaperID
            data: data for the paper
            quiet: If :code:`quiet` is :code:`True`, then some of the logging is suppressed
            discard_existing: Whether to discard existing data. Useful in case data
                              for a particular paper is corrupted.

        """
        def maybe_add_inferred_duplicate_to_known_duplicates(ID: str):
            if ID in self.inferred_duplicates_map and ID not in self.known_duplicates.values():
                duplicates = self.inferred_duplicates[self.inferred_duplicates_map[ID]]
                duplicates.remove(ID)
                for dup in duplicates:
                    self.known_duplicates[dup] = ID
                    self.update_duplicates_metadata(dup)

        # FIXME:
        def update_paper_metadata(details: PaperDetails):
            paper_id = details.paperId
            corpus_id = details.corpusId
            for k, v in details.externalIds.items():
                if id_to_name(k) in self._extid_metadata and str(v):
                    self._extid_metadata[id_to_name(k)][str(v)] = corpus_id
            self.metadata[corpus_id] = {id_to_name(k): str(v)
                                        for k, v in details.externalIds.items()}
            self.corpus_map[paper_id] = corpus_id
            self.rev_corpus_map[corpus_id] = paper_id
            self.update_paper_metadata(paper_id)

        def update_existing_data(paper_id: str):
            # NOTE: In case force updated and already some citations exist on backend
            if not discard_existing:
                existing_data = self._check_cache(paper_id, quiet=quiet)
                if existing_data is not None:
                    self._update_citations_from_existing_to_new(existing_data.citations,
                                                                data.citations)
                    self._update_references_from_existing_to_new(existing_data.references,
                                                                 data.references)

        def update_duplicates(paper_id: str, ID: str):
            if ID != paper_id:
                self.known_duplicates[ID] = paper_id
                self.update_duplicates_metadata(ID)

        details = data.details
        paper_id = details.paperId

        update_existing_data(paper_id)

        # FIXME: `force=discard_existing` is only used in case of jsonl_backend and the meaning is
        #        different
        self.store_paper_data(paper_id, data, force=discard_existing)

        update_paper_metadata(details)

        maybe_add_inferred_duplicate_to_known_duplicates(ID)

        update_duplicates(paper_id, ID)

        self._in_memory[paper_id] = data

        return None

    def to_details(self, data: PaperData) -> PaperDetails:
        """Combine :code:`references` and :code:`citations` into the paper
        details. The :code:`references`, :code:`citations` and paper details
        are stored separately on the backend but they can be combined into
        one mapping.

        Args:
            data: data for the paper

        """
        _data = PaperData(**dataclasses.asdict(data))
        _data.details.references = [x.citedPaper for x in _data.references.data]
        _data.details.citations = [x.citingPaper for x in _data.citations.data]
        return _data.details

    def details_url(self, ID: str) -> str:
        """Return the paper url for a given `ID`

        Args:
            ID: paper identifier

        """
        fields = ",".join(self.details_fields)
        return f"{self._root_url}/paper/{ID}?fields={fields}"

    def citations_url(self, ID: str, num: int = 0, offset: Optional[int] = None) -> str:
        """Generate the citations url for a given `ID`

        Args:
            ID: paper identifier
            num: number of citations to fetch in the url
            offset: offset from where to fetch in the url

        """
        fields = ",".join(self.citations_fields)
        limit = num or self.config.data.citations.limit
        url = f"{self._root_url}/paper/{ID}/citations?fields={fields}&limit={limit}"
        if offset is not None:
            return url + f"&offset={offset}"
        else:
            return url

    def references_url(self, ID: str, num: int = 0) -> str:
        """Generate the references url for a given :code:`ID`

        Args:
            ID: paper identifier
            num: number of citations to fetch in the url

        """
        fields = ",".join(self.citations_fields)
        limit = num or self.config.data.references.limit
        return f"{self._root_url}/paper/{ID}/references?fields={fields}&limit={limit}"

    def _get(self, url: str) -> dict:
        """Synchronously get a URL with the API key if present.

        Args:
            url: URL

        """
        try:
            result = requests.get(url, headers=self.headers, timeout=self.client_timeout)
        except requests.Timeout:
            self.logger.error(f"Got timeout for {url}")
            return {"error": f"Got timeout for {url}"}
        return result.json()

    async def _aget(self, session: aiohttp.ClientSession, url: str) -> dict:
        """Asynchronously get a url.

        Args:
            sesssion: An :class:`aiohttp.ClientSession` instance
            url: The url to fetch

        """
        resp = await session.request('GET', url=url)
        data = await resp.json()
        return data

    async def _get_some_urls(self, urls: list[str], timeout: Optional[int] = None) -> list:
        """Get some URLs asynchronously

        Args:
            urls: list of URLs

        URLs are fetched with :class:`aiohttp.ClientSession` with api_key included
        The results parsed as JSON, stored in a list and returned.

        """
        if timeout is None:
            timeout = self._aio_timeout  # type: ignore
        else:
            timeout = aiohttp.ClientTimeout(timeout)  # type: ignore
        try:
            async with aiohttp.ClientSession(headers=self.headers,
                                             timeout=timeout) as session:
                tasks = [self._aget(session, url) for url in urls]
                results: list = await asyncio.gather(*tasks)
        except asyncio.exceptions.TimeoutError:
            return []
        return results

    def _post(self, url: str, params=None, data=None):
        """Synchronously get a URL with the API key if present.

        Args:
            url: URL

        """
        params = params or []
        if data is None:
            raise ValueError
        result = asyncio.run(self._post_some_urls([url], params, [data]))
        return result[0]

    async def _apost(self, session: aiohttp.ClientSession, url: str,
                     params, data) -> dict:
        """Asynchronously get a url.

        Args:
            sesssion: An :class:`aiohttp.ClientSession` instance
            url: The url to fetch

        """
        resp = await session.request('POST', url=url, params=params, json=dumps_json(data))
        data = await resp.json()
        return data

    async def _post_some_urls(self, urls: list[str], params: list,
                              data: list, timeout: Optional[int] = None) -> list:
        """Get some URLs asynchronously

        Args:
            urls: list of URLs

        URLs are fetched with :class:`aiohttp.ClientSession` with api_key included

        """
        if timeout is None:
            timeout = self._aio_timeout  # type: ignore
        else:
            timeout = aiohttp.ClientTimeout(timeout)  # type: ignore
        try:
            async with aiohttp.ClientSession(headers=self.headers,
                                             timeout=timeout) as session:
                tasks = [self._apost(session, url, _params, _data)
                         for url, _params, _data in zip(urls, params, data)]
                results = await asyncio.gather(*tasks)
        except asyncio.exceptions.TimeoutError:
            return []
        return results

    async def _paper_only(self, ID: str) -> dict:
        """Asynchronously fetch paper details, references and citations.

        Gather and return the data

        Args:
            ID: paper identifier

        """
        urls = [self.details_url(ID)]
        self.logger.debug("Fetching paper with _get_some_urls")
        results = await self._get_some_urls(urls)
        return results[0]

    async def _paper(self, ID: str) -> dict:
        """Asynchronously fetch paper details, references and citations.

        Gather and return the data

        Args:
            ID: paper identifier

        """
        urls = [f(ID) for f in [self.details_url,  # type: ignore
                                self.references_url,
                                self.citations_url]]
        self.logger.debug("FETCHING paper with _get_some_urls")
        results = await self._get_some_urls(urls)
        data = dict(zip(["details", "references", "citations"], results))
        return data

    def _paper_batch(self, IDs: list[str], fields: list[str]) -> dict:
        """Asynchronously fetch paper details in batch.

        This fetches only specific fields and does not fetch citations and references.
        Useful for fetching specific fields and updates.

        Args:
            ID: paper identifier
            fields: The fields to fetch

        """
        url = f"{self._root_url}/paper/batch"
        self.logger.debug("Fetching papers in batch with requests.post")
        with _timer:
            response = requests.post(url, params={"fields": ",".join(fields),
                                                  "limit": self.config.data.details.limit},
                                     json={"ids": IDs})
        self.logger.debug(f"Fetched {len(IDs)} rseults in {_timer.time} seconds")
        return response.json()

    def fetch_store_and_return_details(
            self, ID: str,
            quiet: bool = False,
            update_only_paper_data: bool = False,
            discard_existing: bool = False) -> Error | PaperData:
        """Get paper details asynchronously, store them and return.

        1. Fetch paper details, references and citations async.
        2. Update and store data in cache
        3. Return the dataclass

        Args:
            ID: paper identifier
            update_only_paper_data: Update only the paper data instead of getting
                                    references and citations also.
            quiet: Do not log some messages
            discard_existing: Discard existing data. Helpful when there's data corruption.

        """
        if update_only_paper_data:
            result = self._get(self.details_url(ID))
            errors = ["error" in x and x["error"] in x for x in result]
            if errors:
                return Error(message=str(errors[0]))
            data = PaperData(details=PaperDetails(**{k: v for k, v in result.items()
                                                     if k not in {"citations", "references"}}),
                             references=References(data=[], offset=0),
                             citations=Citations(data=[], offset=0))
        else:
            result = asyncio.run(self._paper(ID))
            try:
                data = PaperData(**result)
            except TypeError:
                details: dict[str, str | dict] = {}
                if "externalIds" not in result["details"]:
                    details["externalIds"] = {}
                for k, v in result["details"].items():
                    if k in self.old_fields_map:
                        k = self.old_fields_map[k]
                    if isinstance(k, tuple):
                        if v:
                            # NOTE: This is for converting old format data
                            details["externalIds"][k[-1]] = v
                    elif k:
                        details[k] = v
                result["details"] = details
                if "references" in details:
                    details.pop("references")
                if "citations" in details:
                    details.pop("citations")
                try:
                    data = PaperData(**result)
                except TypeError:
                    return Error(message="Could not parse data", error=dumps_json(result))

        if ":" in ID:
            ID, duplicate_id = self._check_duplicate(data.details.paperId)
        maybe_error = self._update_paper_details_in_backend(
            ID, data, quiet=quiet, discard_existing=discard_existing)
        if maybe_error:
            return maybe_error
        ID, duplicate_id = self._check_duplicate(ID)
        data = self._in_memory[ID]
        data.details.duplicateId = duplicate_id
        return data

    def fetch_from_cache_or_api(self, have_metadata: bool,
                                ID: str, force: bool,
                                no_transform: bool)\
            -> Error | PaperData | PaperDetails:
        """Subroutine to fetch from either backend or Semantic Scholar.

        Args:
            have_metadata: We already had the metadata
            ID: paper identifier
            force: Force fetch from Semantic Scholar server if True, ignoring cache
            no_transform: Return raw data and not paper details The default
                          behaviour is to return paper details

        """
        if ":" not in ID:
            ID, duplicate_id = self._check_duplicate(ID)
        else:
            duplicate_id = None
        if have_metadata or duplicate_id:
            self.logger.debug(f"Checking for cached data for {ID}")
            data: PaperData | Error | None = self._check_cache(ID)
            if not force:
                if data is None:
                    self.logger.debug(f"PaperDetails for {ID} stale or not present on backend. Will fetch.")
                    data = self.fetch_store_and_return_details(ID, quiet=True,
                                                               update_only_paper_data=True,
                                                               discard_existing=True)
            else:
                self.logger.debug(f"Force fetching from Semantic Scholar for {ID}")
                data = self.fetch_store_and_return_details(ID, update_only_paper_data=True)
            if duplicate_id and isinstance(data, PaperData):
                data.details.duplicateId = duplicate_id
        else:
            self.logger.debug(f"Fetching from Semantic Scholar for {ID}")
            data = self.fetch_store_and_return_details(ID)
        if isinstance(data, Error) or no_transform:
            return data
        else:
            return self.to_details(data)

    def update_and_fetch_paper(self, ID: str, update_keys: list[str],
                               paper_data: bool = False)\
            -> Error | PaperData | PaperDetails:
        """Update part of paper data fetch it.

        Args:
            ID: Paper ID
            update_keys: Which parts to update. They coincide with attributes :class:`PaperData`
            paper_data: Return :class:`PaperData` instead of :class:`PaperDetails`


        """
        invalid_keys = set(update_keys) - {"details", "references", "citations"}
        if invalid_keys:
            return Error(message="Some update keys are invalid",
                         error=json.dumps(list(*invalid_keys)))
        urls_dict = {"details": self.details_url(ID),
                     "references": self.references_url(ID, num=500),
                     "citations": self.citations_url(ID)}
        data = self._check_cache(ID)
        if data is None:
            update_keys = list(urls_dict.keys())
        urls = [urls_dict[k] for k in update_keys]
        results = asyncio.run(self._get_some_urls(urls))
        update_dict = dict(zip(update_keys, results))
        if not update_dict or any("error" in x for x in update_dict.values()):
            return Error(message="Error occured", error=dumps_json(update_dict))
        data_dict = dataclasses.asdict(data)
        data_dict.update(update_dict)
        data = PaperData(**data_dict)
        if ":" in ID:
            ID, duplicate_id = self._check_duplicate(data.details.paperId)
        maybe_error = self._update_paper_details_in_backend(ID, data)
        if maybe_error:
            return maybe_error
        ID, duplicate_id = self._check_duplicate(ID)
        data = self._in_memory[ID]
        data.details.duplicateId = duplicate_id
        if paper_data:
            return data
        else:
            return self.to_details(data)

    def check_for_dblp(self, id_name) -> Optional[Error]:
        if NameToIds[id_to_name(id_name)] == IdTypes.dblp:
            return Error(message="Details for DBLP IDs cannot be fetched directly from SemanticScholar")
        else:
            return None

    def id_to_corpus_id(self, id_type: str, ID: str) ->\
            Error | int:
        """Fetch :code:`corpusId` for a given paper ID of type :code:`id_type`

        Args:
            id_type: Type of ID
            ID: The ID

        If paper data is not in cache, it's fetched first. Used primarily by
        external services.

        """
        maybe_error = self.id_to_prefix_and_id(id_type, ID)
        if isinstance(maybe_error, Error):
            return maybe_error
        id_name, ssid, have_metadata = maybe_error
        if have_metadata:
            return self.corpus_map[ssid]
        if dblp_error := not ssid and self.check_for_dblp(id_name):
            return dblp_error
        data = self.fetch_from_cache_or_api(False, f"{id_name}:{ID}", False, False)
        if isinstance(data, Error):
            return data
        data = cast(PaperDetails, data)
        return int(data.corpusId)

    def id_to_prefix_and_id(self, id_type, ID) ->\
            tuple[str, str, bool] | Error:
        ID = str(ID)
        id_name = ""
        ssid = ""
        if id_to_name(id_type) not in NameToIds:
            return Error(message=f"Invalid ID type {id_to_name(id_type)}")
        else:
            id_ = NameToIds[id_to_name(id_type)]
        if id_ == IdTypes.ss:
            ssid = ID
            have_metadata = ssid in self.metadata
            have_metadata = have_metadata or ssid in self.corpus_map
        else:
            id_name = id_to_name(id_type)
            corpus_id = self._extid_metadata[id_name].get(ID, None)
            have_metadata = bool(corpus_id)
            if corpus_id:
                ssid = self.rev_corpus_map[int(corpus_id)]
        if ssid in self.known_duplicates:
            ssid = self.known_duplicates[ssid]
        elif ssid in self.inferred_duplicates_map:
            ssid = self.inferred_duplicates[self.inferred_duplicates_map[ssid]][0]
        return IdPrefixes.get(id_, ""), ssid, have_metadata

    def get_details_for_id(self, id_type: str, ID: str, force: bool)\
            -> Error | PaperDetails:
        """Get paper details from Semantic Scholar Graph API

        The on backend cache is checked first and if it's a miss then the
        details are fetched from the server and stored in the cache.

        `force` force fetches the data from the API and updates the cache
        on the backend also.

        Args:
            id_type: Type of the paper identifier. One of IdTypes
            ID: paper identifier
            force: Force fetch from Semantic Scholar server, ignoring cache
            paper_data: Get PaperData instead of PaperDetails.

        """
        maybe_error: tuple | Error = self.id_to_prefix_and_id(id_type, ID)
        if isinstance(maybe_error, Error):
            return maybe_error
        id_prefix, ssid, have_metadata = maybe_error
        if dblp_error := not ssid and self.check_for_dblp(id_prefix):
            return dblp_error
        data = self.fetch_from_cache_or_api(
            have_metadata, ssid or f"{id_prefix}:{ID}", force, no_transform=False)
        if isinstance(data, Error):
            return data
        else:
            return self.apply_limits(cast(PaperDetails, data))

    def get_data_for_id(self, id_type: str, ID: str, force: bool)\
            -> Error | PaperData:
        """Get paper data from Semantic Scholar Graph API

        The on backend cache is checked first and if it's a miss then the
        details are fetched from the server and stored in the cache.

        `force` force fetches the data from the API and updates the cache
        on the backend also.

        Args:
            id_type: Type of the paper identifier. One of IdTypes
            ID: paper identifier
            force: Force fetch from Semantic Scholar server, ignoring cache
            paper_data: Get PaperData instead of PaperDetails.

        """
        maybe_error = self.id_to_prefix_and_id(id_type, ID)
        if isinstance(maybe_error, Error):
            return maybe_error
        id_prefix, ssid, have_metadata = maybe_error
        if dblp_error := not ssid and self.check_for_dblp(id_prefix):
            return dblp_error
        data = self.fetch_from_cache_or_api(
            have_metadata, ssid or f"{id_prefix}:{ID}", force, no_transform=True)
        return cast(PaperData, data)

    def get_data_from_corpus_cache(self, corpus_id: Optional[int] = None,
                                   ssid: Optional[str] = None) -> Error | PaperData:
        """Get paper details from locally stored data cache.

        Similar to :meth:`get_data_for_id`, except this relies on stored
        :class:`CitationsCache` and :class:`PapersCache` data stored locally.
        No outgoing requests to Semantic Scholar API are made.

        Args:
            corpusid: The corpusid of the paper

        """
        if self.refs_cache is None or self.citations_cache is None:
            return Error("One of refs cache or citations cache not initialized")
        if corpus_id is None:
            if ssid is None:
                raise ValueError
            maybe_error = self.id_to_corpus_id("ss", ssid)
            if isinstance(maybe_error, Error):
                return maybe_error
            else:
                corpus_id = maybe_error
        maybe_details = self._papers_cache.get_paper(corpus_id)
        if maybe_details is not None:
            maybe_cite_ids = self.citations_cache.get_citations(int(maybe_details.corpusId))
            if maybe_cite_ids is not None:
                citations = self._papers_cache.get_some_papers(maybe_cite_ids)
            else:
                citations = []
            maybe_ref_ids = self.refs_cache.get_references(corpus_id)
            if maybe_ref_ids is not None:
                references = self._papers_cache.get_some_papers(maybe_ref_ids)
            else:
                references = []
            return PaperData(details=maybe_details,
                             references=References(offset=0,
                                                   data=[Reference(citedPaper=x)
                                                         for x in references]),
                             citations=Citations(offset=0,
                                                 data=[Citation(citingPaper=x)
                                                       for x in citations]))
        else:
            return Error("Could not load paper")

    def paper_data(self, ID: str, force: bool = False) ->\
            Error | PaperData:
        """Get paper data exactly as stored on backend, for paper with SSID :code:`ID`

        This is basically a convenience function instead of :meth:`get_details_for_id`
        where :code:`id_type` is set to :code:`ss`

        Args:
            ID: SSID of the paper
            force: Whether to force fetch from service

        """
        return self.get_data_for_id("SS", ID, force)

    def paper_details(self, ID: str, force: bool = False) ->\
            Error | PaperDetails:
        """Get details for paper with SSID :code:`ID`

        Like :meth:`paper_data` but return only the :code:`details` part
        with the :code:`citations` and :code:`references` moved to details
        itself. This is to hide :code:`next` and :code:`offset` fields
        from the user.

        Args:
            ID: SSID of the paper
            force: Whether to force fetch from service

        """
        return self.get_details_for_id("SS", ID, force)

    def batch_paper_details(self, IDs: list[tuple[str, str]], force: bool = False):
        ids = {}
        result = {}
        for id_type, ID in IDs:
            maybe_error = self.id_to_prefix_and_id(id_type, ID)
            if isinstance(maybe_error, Error):
                return maybe_error
            id_name, ssid, have_metadata = maybe_error
            if dblp_error := not ssid and self.check_for_dblp(id_name):
                return dblp_error
            if force or not have_metadata:
                ids[ssid or f"{id_name}:{ID}"] = (id_type, ID)
            else:
                result[ID] = self.fetch_from_cache_or_api(True, ssid, force, False)
        if ids:
            batch_result = self._paper_batch([*ids.keys()], self.details_fields)
            for paper in batch_result:
                paper_data = PaperData(details=PaperDetails(**{k: v for k, v in paper.items()
                                                               if k not in {"citations", "references"}}),
                                       references=References(data=[], offset=0),
                                       citations=Citations(data=[], offset=0))
                self._update_paper_details_in_backend(paper["paperId"], paper_data)
        raise NotImplementedError

    def update_and_fetch_paper_fields_in_batch(self, IDs: list[str], fields: list[str]):
        raise NotImplementedError

    def apply_limits(self, data: PaperDetails) -> PaperDetails:
        """Apply count limits to S2 data citations and references

        Args:
            data: S2 Data

        Limits are defined in configuration

        """
        _data = dataclasses.asdict(data)
        data = PaperDetails(**_data)
        if data.citations:
            limit = self.config.data.citations.limit
            data.citations = data.citations[:limit]
        if data.references:
            limit = self.config.data.references.limit
            data.references = data.references[:limit]
        return data

    def _check_cache(self, ID: str, quiet: bool = False) -> Optional[PaperData]:
        """Check cache and return data for ID if found.

        First the `in_memory` cache is checked and then the on backend cache.

        Args:
            ID: Paper ID

        """
        ID, duplicate_id = self._check_duplicate(ID)
        if ID not in self._in_memory:
            if not quiet:
                self.logger.debug(f"Data for {ID} not in memory")
            data = self.get_paper_data(ID, quiet=quiet)
            if data:
                if not quiet:
                    self.logger.debug(f"Data for {ID} in backend")
                try:
                    paper_data = PaperData(**data)
                    self._in_memory[ID] = paper_data
                except TypeError:
                    if not quiet:
                        self.logger.debug(f"Stale data for {ID} in backend")
                    return None
            else:
                self.logger.debug(f"Tried to load data for {ID} from backend but could not")
                return None
        else:
            self.logger.debug(f"Data for {ID} in memory")
        paper_data = self._in_memory.get(ID, None)  # type: ignore
        if paper_data is not None:
            paper_data.details.duplicateId = duplicate_id
        return paper_data

    # TODO: For some papers, there would be parse errors for
    #       references/citations and those show up with null paperid's. So if
    #       paper's referencecount/citationcount is n, actual useful refs may be
    #       (n-k). In those cases, we might keep fetching it again and again.  I
    #       remember there's a no fetch flag somewhere.  I can possibly amend
    #       the refs/citation count also after fetching if there are errors.
    def references(self, ID: str, offset: Optional[int] = None,
                   limit: Optional[int] = None) -> Error | References:
        existing_data = self._check_cache(ID)
        if existing_data is None or existing_data.details.referenceCount is None:
            update_keys = field_names(PaperData)
            maybe_error = self.update_and_fetch_paper(ID, update_keys, paper_data=True)
            if isinstance(maybe_error, Error):
                return maybe_error
            else:
                maybe_error = cast(PaperData, maybe_error)
                return maybe_error.references
        else:
            update_keys = ["references"]
            if existing_data.details.referenceCount - len(existing_data.references.data) > 5:
                maybe_error = self.update_and_fetch_paper(ID, update_keys, paper_data=True)
                if isinstance(maybe_error, Error):
                    return maybe_error
                else:
                    maybe_error = cast(PaperData, maybe_error)
                    return maybe_error.references
            else:
                return existing_data.references

    def citations(self, ID: str, offset: int, limit: Optional[int]):
        """Fetch citations for a paper in a specific range.

        The range is defined by arguments :code:`offset` and :code:`limit`

        Args:
            ID: SSID of the paper
            offset: offset
            limit: limit

        If none of :code:`beg`, :code:`end`, :code:`limit` are given, then
        the default number of citations is returned.

        """
        data = self.fetch_from_cache_or_api(True, ID, False, True)
        data = cast(PaperData, data)
        existing_citations = data.citations.data
        citation_count = data.details.citationCount
        if not citation_count:
            raise ValueError

        limit = limit or citation_count - offset
        if offset + limit > citation_count:
            limit = citation_count - offset
        if offset + limit > len(existing_citations):
            self.next_citations(ID, (offset + limit) - len(existing_citations))
            data = cast(PaperData, self._check_cache(ID))
            if data is None:
                self.logger.error("Got None from cache after fetching. This should not happen")
            existing_citations = data.citations.data
        retval = existing_citations[offset:offset+limit]
        return [x.citingPaper for x in retval]

    # TODO: Although this fetches the appropriate data based on citations on backend
    #       the offset and limit handling is tricky and is not correct right now.
    # TODO: What if the num_citations change between the time we fetched earlier and now?
    def next_citations(self, ID: str, limit: int) -> Error | Citations | None:
        """Fetch next :code:`limit` citations for a paper if they exist.

        The paper details including initial citations are already assumed to be
        in cache.

        The :code:`offset` for the :code:`limit` is always the offset
        of the data already in cache

        Args:
            ID: The paper ID
            limit: Number of additional citations to fetch

        There is an issue with S2 API in that even though it may show :code:`n`
        number of citing papers, when actually fetching the citation data it
        retrieves fewer citations sometimes.

        """
        data = self._check_cache(ID)
        if data is None:
            return Error(message=f"Data for {ID} not in cache")
        elif data is not None and not data.citations:
            # no new citations
            return None
        else:
            offset = data.citations.offset
            cite_count = data.details.citationCount
            if offset+limit > 10000 and self.citations_cache is not None:
                corpus_id = data.details.corpusId
                if corpus_id:
                    citations = self._build_citations_from_stored_data(
                        corpus_id=corpus_id,
                        existing_ids=get_existing_corpus_ids(data.citations.data),
                        cite_count=cite_count or 0,
                        offset=offset,
                        limit=limit)
            else:
                paper_id = data.details.paperId
                if len(data.citations.data):
                    data.citations.offset = len(data.citations.data)
                    offset = data.citations.offset
                url = self.citations_url(paper_id, limit, offset)
                result = self._get(url)
                try:
                    citations = Citations(**result)
                except Exception:
                    return Error(message=result["error"])
                _maybe_fix_citation_data(citations, Citation, "citingPaper")
            data.citations = self._update_citations_from_existing_to_new(data.citations, citations)
            self.store_paper_data(paper_id, data)
            return data.citations

    def _batch_urls(self, num: int, offset: int, url_prefix: str):
        """Generate a list of urls in batch size of :attr:`batch_size`

        Args:
            num: number of data points to fetch
            offset: offset from where to start
            url_prefix: The prefix for the URL without offset and limit arguments.
                        It'll be formattted with :code:`offset` and :code:`limit`

        The :code:`url_prefix` should be complete with only the :code:`limit`
        and :code:`offset` parts remaining.

        """
        urls = []
        batch_size = self.batch_size
        iters = math.ceil(num / batch_size)
        limit = batch_size
        for i in range(iters):
            urls.append(f"{url_prefix}&limit={limit}&offset={offset}")
            offset += limit
        return urls
        # urls = []
        # batch_size = self.batch_size
        # iters = min(10, math.ceil(n / batch_size))
        # for i in range(iters):
        #     limit = 9999 - (offset + i * batch_size)\
        #         if (offset + i * batch_size) + batch_size > 10000 else batch_size
        #     urls.append(f"{url_prefix}&limit={limit}&offset={offset + i * batch_size}")
        # return urls

    def _ensure_all_citations(self, ID: str, fetch_from: str) -> Citations:
        """Fetch all citations for a given paper_id :code:`ID`

        Args:
            ID: Paper ID

        This will fetch ALL citations which can be fetched from the S2 API
        and after that, using the data available from :attr:`citations_cache`


        """
        data = self._check_cache(ID)
        if data is not None:
            cite_count = data.details.citationCount
            if not cite_count:
                raise ValueError("Citation count should not be None")
            existing_cite_count = len(data.citations.data)
            if cite_count > 10000:
                self.logger.warning("More than 10000 citations cannot be fetched "
                                    "with this function. Use next_citations for that. "
                                    "Will only get first 10000")
            fields = ",".join(self.citations_fields)
            url_prefix = f"{self._root_url}/paper/{ID}/citations?fields={fields}"
            # NOTE: Generally we will fetch from the head of the data, it's
            #       usually sorted by year in reverse.  But, sometimes, we may
            #       need to fetch from the tail.
            num_to_fetch = cite_count - existing_cite_count
            if fetch_from == "head":
                urls = self._batch_urls(num_to_fetch, 0, url_prefix)
            else:
                urls = self._batch_urls(num_to_fetch, existing_cite_count, url_prefix)
            self.logger.debug(f"Will fetch {len(urls)} requests for citations")
            self.logger.debug(f"All urls {urls}")
            with _timer:
                results = asyncio.run(self._get_some_urls(urls))
            self.logger.debug(f"Got {len(results)} results")
            citations: Citations = Citations(next=0, offset=0, data=[])
            cite_list = []
            errors = 0
            for x in results:
                if "error" not in x:
                    if self.config.no_contexts_intents:
                        cite_list.extend([Citation(**{**e, "contexts": [], "intents": []})
                                          for e in x["data"]])
                    else:
                        cite_list.extend([Citation(**e) for e in x["data"]])
                else:
                    errors += 1
            citations.data = cite_list
            self.logger.debug(f"Have {len(cite_list)} citations without errors")
            if errors:
                self.logger.debug(f"{errors} errors occured while fetching all citations for {ID}")
            if results and all("next" in x and x["next"] for x in results):
                citations.next = max([*[x["next"] for x in results if "error" not in x], 10000])
            else:
                citations.next = None
            return citations
        else:
            msg = f"Paper data for {ID} should already exist"
            raise ValueError(msg)

    def _get_some_urls_in_batches(self, urls: list[str]) -> list[dict]:
        """Fetch :attr:`batch_size` examples at a time to prevent overloading the service

        Args:
            urls: URLs to fetch

        """
        batch_size = self.batch_size
        j = 0
        _urls = urls[j*batch_size:(j+1)*batch_size]
        results = []
        _results = []
        while _urls:
            self.logger.debug(f"Fetching for {j+1} out of {(len(urls)//batch_size)+1} urls")
            with _timer:
                _results = asyncio.run(self._get_some_urls(_urls, 5))
                while not _results:
                    wait_time = random.randint(1, 5)
                    self.logger.debug(f"Got empty results. Waiting {wait_time}")
                    time.sleep(wait_time)
                    _results = asyncio.run(self._get_some_urls(_urls))
            results.extend(_results)
            j += 1
            _urls = urls[j*batch_size:(j+1)*batch_size]
        return results

    # TODO: Need to add condition such that if num_citations > 10000, then this
    #       function is called. And also perhaps, fetch first 1000 citations and
    #       update the stored data (if they're sorted by time)
    def _build_citations_from_stored_data(self,
                                          corpus_id: int | str,
                                          existing_ids: list[int],
                                          cite_count: int,
                                          *,
                                          offset: int = 0,
                                          limit: int = 0) -> Optional[Citations]:
        """Build the citations data for a paper entry from cached data

        Args:
            corpus_id: Semantic Scholar corpusId
            existing_ids: Existing ids present if any
            cite_count: Total citationCount as given by S2 API
            limit: Total number of citations to fetch

        """
        if self.citations_cache is not None:
            refs_ids = self.citations_cache.get_citations(int(corpus_id))
            if not refs_ids:
                raise AttributeError(f"Not found for {corpus_id}")
            fetchable_ids = list(refs_ids - set(existing_ids))
            if not limit:
                limit = len(fetchable_ids)
            cite_gap = cite_count - len(fetchable_ids) - len(existing_ids)
            if cite_gap:
                self.logger.warning(f"{cite_gap} citations cannot be fetched. "
                                    "You have stale SS data")
            fetchable_ids = fetchable_ids[offset:offset+limit]
            # Remove contexts as that's not available in paper details
            fields = ",".join(self.citations_fields).replace(",contexts", "").replace(",intents", "")
            urls = [f"{self._root_url}/paper/CorpusID:{ID}?fields={fields}"
                    for ID in fetchable_ids]
            citations = Citations(offset=0, data=[])
            result = self._get_some_urls_in_batches(urls)
            for x in result:
                try:
                    citations.data.append(Citation(citingPaper=PaperDetails(**x),
                                                   contexts=[],
                                                   intents=[]))
                except Exception:
                    pass        # ignore errors
            return citations          # type: ignore
        else:
            self.logger.error("References Cache not present")
            return None

    def _maybe_fetch_citations_greater_than_10000(self, existing_data: PaperData):
        """Fetch citations when their number is > 10000.

        SemanticScholar doesn't allow above 10000, so we have to build that
        from the dumped citation data. See :meth:`_build_citations_from_stored_data`

        Args:
            existing_data: Existing paper details data

        """
        if self.citations_cache is None:
            return None
        cite_count = len(existing_data.citations.data)
        existing_corpus_ids = get_existing_corpus_ids(existing_data.citations.data)
        corpus_id = existing_data.details.corpusId  # type: ignore
        if not corpus_id:
            raise AttributeError("Did not expect corpus_id to be 0")
        if corpus_id not in self._dont_build_citations:
            more_data = self._build_citations_from_stored_data(corpus_id,
                                                               existing_corpus_ids,
                                                               cite_count)
            if more_data is None:
                raise ValueError("more_data should not be None")
            new_ids = set([x.citingPaper.paperId for x in more_data.data
                           if x.citingPaper.paperId])
            # NOTE: Some debug vars commented out
            # new_data_dict = {x["citingPaper"]["paperId"]: x["citingPaper"]
            #                  for x in more_data["data"]
            #                  if "citingPaper" in x and "error" not in x["citingPaper"]
            #                  and "paperId" in x["citingPaper"]}
            # existing_citation_dict = {x["citingPaper"]["paperId"]: x["citingPaper"]
            #                           for x in existing_data["citations"]["data"]}

            existing_ids = set(x.citingPaper.paperId
                               for x in existing_data.citations.data if x.citingPaper)
            something_new = new_ids - existing_ids
            if more_data and more_data.data and something_new:
                self.logger.debug(f"Fetched {len(more_data.data)} in {_timer.time} seconds")
                existing_data.citations =\
                    self._update_citations_from_existing_to_new(more_data,
                                                                existing_data.citations)
                update = True
        self._dont_build_citations.add(corpus_id)
        return update


    def _filter_subr(self, key: str, citation_data: list[Citation] | list[Reference],
                     filters: dict[str, Any],
                     num: int) -> list[PaperDetails]:
        """Subroutine for filtering references and citations

        Each filter function is called with the arguments and the results are AND'ed.

        Args:
            key: One of "references" or "citations"
            citation_data: citation (or references) data to filter
            filters: Filter names and kwargs
            num: Number of results to return

        """
        retvals = []
        for citation in citation_data:
            status = True
            for filter_name, filter_args in filters.items():
                # key is either citedPaper or citingPaper
                # This is a bit redundant as key should always be there but this will
                # catch edge cases
                paper = getattr(citation, key)
                if paper:
                    try:
                        # kwargs only
                        filter_func = self.filters[filter_name]
                        status = status and filter_func(paper, **filter_args)
                    except Exception as e:
                        self.logger.debug(f"Can't apply filter {filter_name} on {citation}: {e}")
                        status = False
                else:
                    status = False
            if status:
                retvals.append(paper)
            if num and len(retvals) == num:
                break
        return retvals

    def filter_citations(self, ID: str, filters: dict[str, Any], num: int = 0)\
            -> list[PaperDetails] | Error:
        """Filter citations based on given filters.

        Filters are json like dictionaries which have the filter name and the
        kwargs to each filter function call.  The functions are called in turn
        and only if all of them return :code:`True` does the filter return `True`.

        We can also do arbitrary combinations of AND and OR but that's a bit much.

        Args:
            ID: Paper ID
            filters: filter names and arguments
            num: Number of results to retrieve

        """
        paper_data = self._check_cache(ID)
        if paper_data is None:
            msg = f"data should not be None for ID {ID}"
            return msg          # type: ignore
        else:
            update = False
            cite_count = paper_data.details.citationCount
            existing_cite_count = len(paper_data.citations.data)
            if not (existing_cite_count and cite_count) or existing_cite_count > cite_count:
                data = self.fetch_store_and_return_details(ID, update_only_paper_data=True)
                if isinstance(data, Error):
                    return data
                paper_data = data
                cite_count = paper_data.details.citationCount
                existing_cite_count = len(paper_data.citations.data)
                if not cite_count:
                    raise ValueError("Citation count should not be None")
            if abs(cite_count - existing_cite_count) > self.tolerance:
                with _timer:
                    # TODO: Add a flag to fetch from "tail" if fetching from head
                    #       doesn't complete citations
                    citations = self._ensure_all_citations(ID, "head")
                self.logger.debug(f"Fetched {len(citations.data)} in {_timer.time} seconds")
                self._update_citations_from_existing_to_new(citations, paper_data.citations)
                if len(paper_data.citations.data) > existing_cite_count:
                    self.logger.debug(f"Fetched {len(paper_data.citations.data) - existing_cite_count}"
                                      " new citations")
                    update = True
                if cite_count > 10000:
                    _update = self._maybe_fetch_citations_greater_than_10000(paper_data)
                    update = update or _update
                if update:
                    self.store_paper_data(ID, paper_data)
            return self._filter_subr("citingPaper", paper_data.citations.data, filters, num)

    def filter_references(self, ID: str, filters: dict[str, Any], num: int = 0, **kwargs):
        """Like :meth:`filter_citations` but for references

        Args:
            ID: Paper ID
            filters: filter names and arguments
            num: Number of results to retrieve

        """
        data = self._check_cache(ID)
        if data is not None:
            references = data.references.data
        else:
            raise ValueError(f"Data for ID {ID} should be present")
        return self._filter_subr("citedPaper", references, filters, num)

    def recommendations(self, pos_ids: list[str], neg_ids: list[str], count: int = 0):
        """Fetch recommendations from S2 API

        Args:
            pos_ids: Positive paper ids
            neg_ids: Negative paper ids
            count: Number of recommendations to fetch

        """
        root_url = "https://api.semanticscholar.org/recommendations/v1/papers"
        if neg_ids:
            response = self._post(root_url,
                                  data={"positivePaperIds": pos_ids,
                                        "negativePaperIds": neg_ids})
        else:
            response = self._get(f"{root_url}/forpaper/{pos_ids[0]}")
        # FIXME: As this has changed from requests, this should be different
        if response.status_code == 200:
            recommendations = json.loads(response.content)["recommendedPapers"]
            urls = [self.details_url(x["paperId"])
                    for x in recommendations]
            if count:
                urls = urls[:count]
            results = asyncio.run(self._get_some_urls(urls))
            return dumps_json(results)
        else:
            return dumps_json({"error": json.loads(response.content)})

    def author_url(self, ID: str) -> str:
        """Return the author url for a given :code:`ID`

        Args:
            ID: author identifier

        """
        fields = ",".join(self._author_fields)
        limit = self.config.data.author.limit
        return f"{self._root_url}/author/{ID}?fields={fields}&limit={limit}"

    def author_papers_url(self, ID: str) -> str:
        """Return the author papers url for a given :code:`ID`

        Args:
            ID: author identifier

        """
        fields = ",".join(self.details_fields)
        limit = self.config.data.author_papers.limit
        return f"{self._root_url}/author/{ID}/papers?fields={fields}&limit={limit}"

    async def _author(self, ID: str) -> dict:
        """Fetch the author data from the API

        Args:
            ID: author identifier

        """
        urls = [f(ID) for f in [self.author_url, self.author_papers_url]]
        results = await self._get_some_urls(urls)
        return dict(zip(["author", "papers"], results))

    def author_details(self, ID: str) -> dict:
        """Return the author papers for a given :code:`ID`

        Args:
            ID: author identifier

        """
        result = asyncio.run(self._author(ID))
        return {"author": result["author"],
                "papers": result["papers"]["data"]}


    def author_papers(self, ID: str) -> dict:
        """Return the author papers for a given :code:`ID`

        Args:
            ID: author identifier

        """
        result = asyncio.run(self._author(ID))
        return {"author": result["author"],
                "papers": result["papers"]["data"]}

    def search(self, query: str) -> dict:
        """Search for query string on Semantic Scholar with graph search API.

        Args:
            query: query to search

        """
        terms = "+".join(re.sub(r"[^a-z0-9]", " ", query, flags=re.IGNORECASE).split(" "))
        fields = ",".join(self.details_fields)
        limit = self.config.data.search.limit
        url = f"{self._root_url}/paper/search?query={terms}&fields={fields}&limit={limit}"
        return self._get(url)
