from typing import Optional, Callable, Any, cast
import os
import re
import json
import math
import time
import random
import logging
from pathlib import Path
import asyncio
from enum import auto, Enum
import dataclasses

import yaml
import aiohttp
from common_pyutil.monitor import Timer

from .models import (Pathlike, Metadata, Config, SubConfig, PaperDetails,
                     Citation, Citations, PaperData, Error, maybe_fix_citation_data)
from .filters import (year_filter, author_filter, num_citing_filter,
                      num_influential_count_filter, venue_filter, title_filter)
from .corpus_data import CorpusCache
from .config import default_config, load_config
from .util import json_dump, json_dumps


timer = Timer()


class IdTypes(Enum):
    doi = auto()
    mag = auto()
    arxiv = auto()
    acl = auto()
    pubmed = auto()
    url = auto()
    corpus = auto()
    ss = auto()
    dblp = auto()


def get_corpus_id(data: Citation | PaperDetails) -> int:
    """Get :code:`corpusId` field from :class:`Citation` or :class:`PaperDetails`

    Args:
        data: PaperDetails or Citation data


    """
    if hasattr(data, "externalIds"):
        if data.externalIds:
            cid = data.externalIds["CorpusId"]
            return int(cid)
    elif hasattr(data, "citingPaper"):
        if data.citingPaper["externalIds"]:  # type: ignore
            cid = data.citingPaper.externalIds["CorpusId"]
            return int(cid)
    return -1


def citations_corpus_ids(data: PaperData) -> list[int]:
    return [int(x["citingPaper"]["externalIds"]["CorpusId"])
            for x in data["citations"]["data"]]



class SemanticScholar:
    """A Semantic Scholar API client with a files based cache.

    The cache is a Dictionary of type :code:`Metadata` where they keys are one of
    :code:`["acl", "arxiv", "corpus", "doi", "mag", "url"]` and values are a dictionary
    of that id type and the associated :code:`ss_id`.

    Each :code:`ss_id` is stored as a file with the same
    name as the :code:`ss_id` and contains the data for the entry in JSON format.

    Args:
        root: root directory where all the metadata and the
              files data will be kept

    """

    id_names = {
        IdTypes.doi: "DOI",
        IdTypes.mag: "MAG",
        IdTypes.arxiv: "ARXIV",
        IdTypes.acl: "ACL",
        IdTypes.pubmed: "PUBMED",
        IdTypes.url: "URL",
        IdTypes.corpus: "CorpusId",
        IdTypes.ss: "SS",
        IdTypes.dblp: "DBLP",
    }

    id_keys = list(id_names.values())
    id_keys.remove("SS")
    id_keys.sort()

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

    def __init__(self, cache_dir: Pathlike,
                 config_file: Optional[Pathlike] = None,
                 logger_name: Optional[str] = None):
        self._config = default_config()
        if config_file:
            load_config(self._config, config_file)
        self.logger = logging.getLogger(logger_name or "s2cache")
        self._init_cache(cache_dir)
        self._init_some_vars()
        self.load_jsonl_metadata()
        self.maybe_load_corpus_cache()

    def _init_some_vars(self):
        """Initialize some config and internal variables

        """
        self._api_key = self._config.api_key
        self._root_url = "https://api.semanticscholar.org/graph/v1"
        self._batch_size = self.config.batch_size
        self._tolerance = 10
        self._dont_build_citations: set = set()
        self._aio_timeout = aiohttp.ClientTimeout(10)

    @property
    def config(self) -> Config:
        """Configuration object"""
        return self._config

    @property
    def batch_size(self) -> int:
        """Batch size for fetching urls in batches
        """
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
    def corpus_cache(self) -> Optional[CorpusCache]:
        """Return the Corpus Cache if it exists

        """
        return self._corpus_cache

    @property
    def cache_dir(self) -> Path:
        """Directory where the local paper cache files are kept"""
        return self._cache_dir

    def _init_cache(self, cache_dir: Pathlike):
        """Initialize the cache from :code:`cache_dir`

        Args:
            cache_dir: The directory where cache files reside


        """
        _cache_dir = (cache_dir or self.config.cache_dir)
        if not _cache_dir or not Path(_cache_dir).exists():
            raise FileNotFoundError(f"{_cache_dir} doesn't exist")
        else:
            self._cache_dir = Path(_cache_dir)
        self._metadata: Metadata = {}
        self._extid_metadata: Metadata = {}
        self._in_memory: dict[str, PaperData] = {}
        self._rev_cache: dict[str, list[str]] = {}
        self._files: list[str] = [*filter(
            lambda x: not x.endswith("~") and "metadata" not in x and x != "cache",
            os.listdir(self._cache_dir))]

    def load_jsonl_metadata(self):
        """Load JSON lines metadata from disk

        This will be default from version :code:`0.2.0`

        """
        metadata_file = self._cache_dir.joinpath("metadata.jsonl")
        if metadata_file.exists():
            with open(metadata_file) as f:
                lines = f.read().split("\n")
            for line in lines:
                if line:
                    self._metadata.update(json.loads(line))
            self.logger.debug(f"Loaded metadata from {metadata_file}")
        else:
            self.logger.warning("Metadata file not found. Intialzing empty metadata")
        if self._metadata:
            ext_ids = [*next(iter(self._metadata.values())).keys()]
            self._extid_metadata = {k: {} for k in ext_ids}
            for paper_id, extids in self._metadata.items():
                for idtype, ID in extids.items():
                    if ID and self.external_id_to_name(idtype) in self._extid_metadata:
                        self._extid_metadata[idtype][ID] = paper_id
        else:
            self._extid_metadata = {k: {} for k in self.id_keys if k.lower() != "ss"}

    def dump_jsonl_metadata(self):
        """Dump JSON lines metadata to disk.

        This will be default from version :code:`0.2.0`

        """
        with open(self._cache_dir.joinpath("metadata.jsonl"), "w") as f:
            for k, v in self._metadata.items():
                f.write(json_dumps({k: v}))
                f.write("\n")
        self.logger.debug("Dumped metadata")

    def update_jsonl_metadata_on_disk(self, paper_id: str):
        """Update the existing data on disk with a single :code:`paper_id`

        Args:
            paper_id: The paper id to update


        """
        with open(os.path.join(self._cache_dir, "metadata.jsonl"), "a") as f:
            f.write("\n")
            f.write(json_dumps({paper_id: self._metadata[paper_id]}))
        self.logger.debug(f"Updated metadata for {paper_id}")

    def rebuild_jsonl_metadata(self):
        """Rebuild the JSON lines metadata file in case it's corrupted
        """
        id_to_name = {"arxivid": "ARXIV",
                      "arxiv": "ARXIV",
                      "doi": "DOI",
                      "url": "URL",
                      "pubmedid": "PUBMED",
                      "pubmed": "PUBMED",
                      "aclid": "ACL",
                      "dblp": "DBLP",
                      "acl": "ACL"}
        with open(self._cache_dir.joinpath("metadata.jsonl"), "w") as wf:
            for fname in self._files:
                with open(self._cache_dir.joinpath(fname)) as f:
                    paper_data = json.load(f)
                details = paper_data["details"] if "details" in paper_data else paper_data
                if "externalIds" in details:
                    ext_ids = {self.external_id_to_name(k): v
                               for k, v in details["externalIds"].items()}
                else:
                    ext_ids = {id_to_name[k.lower()]: details[k] for k in details
                               if k.lower() in id_to_name}
                    ext_ids = {k: ext_ids[k] if k in ext_ids else ""
                               for k in self.id_keys}
                wf.write(json_dumps({fname: ext_ids}))
                wf.write("\n")

    def maybe_load_corpus_cache(self):
        """Load :code:`CorpusCache` if given
        """
        if self.config.corpus_cache_dir is not None:
            corpus_cache_dir = Path(self.config.corpus_cache_dir)
        else:
            corpus_cache_dir = None
        if corpus_cache_dir and Path(corpus_cache_dir).exists():
            self._corpus_cache: CorpusCache | None = CorpusCache(corpus_cache_dir)
            self.logger.debug(f"Loaded Full Semantic Scholar Citations Cache from {corpus_cache_dir}")
        else:
            self._corpus_cache = None
            self.logger.debug("Citation Corpus Cache doesn't exist. Not loading")

    def external_id_to_name(self, ext_id: str):
        """Change the ExternalId returned by the S2 API to the name

        Args:
            ext_id: External ID

        """
        return "CorpusId" if ext_id.lower() == "corpusid" else ext_id.upper()

    def _dump_paper_data(self, ID: str, data: PaperData, force: bool = False):
        """Dump the paper :code:`data` for paperID :code:`ID`

        Args:
            ID: The paper ID
            data: The paper data

        The data is stored as JSON. This function handles it as raw :class:`dict`

        """
        fname = os.path.join(self._cache_dir, str(ID))
        if len(data.citations.data) > data.details.citationCount:
            data.details.citationCount = len(data.citations.data)
        with timer:
            with open(fname, "w") as f:
                json_dump(data, f)
        self.logger.debug(f"Wrote file {fname} in {timer.time} seconds")

    def _update_citations(self, existing_citation_data: Citations,
                          new_citation_data: Citations) -> Citations:
        """Update from :code:`existing_citation_data` from :code:`new_citation_data`

        Args:
            existing_citation_data: Existing data
            new_citation_data: New data

        """
        maybe_fix_citation_data(existing_citation_data)
        maybe_fix_citation_data(new_citation_data)
        new_data_ids = {x.citingPaper["paperId"]  # type: ignore
                        for x in new_citation_data.data}
        for x in existing_citation_data.data:
            if x.citingPaper["paperId"] not in new_data_ids:  # type: ignore
                new_citation_data.data.append(x)
                if new_citation_data.next:
                    new_citation_data.next += 1
        return new_citation_data

    def _update_memory_cache_metadata_and_save_to_disk(
            self, data: PaperData, quiet: bool = False, force: bool = False) -> Error | None:
        """Update paper details, references and citations on disk.

        We read and write data for individual papers instead of one big json
        object.

        The data is strored as a dictionary with keys :code:`["details", "references", "citations"]`

        Args:
            data: data for the paper

        """
        details = data.details
        paper_id = details.paperId
        # NOTE: In case force updated and already some citations exist on disk
        if not force:
            existing_data = self._check_cache(paper_id, quiet=quiet)
            if existing_data is not None:
                self._update_citations(existing_data.citations, data.citations)
        self._dump_paper_data(paper_id, data, force=force)
        for k, v in details.externalIds.items():
            if self.external_id_to_name(k) in self._extid_metadata and str(v):
                self._extid_metadata[self.external_id_to_name(k)][str(v)] = paper_id
        self._metadata[paper_id] = {self.external_id_to_name(k): str(v)
                                    for k, v in details.externalIds.items()}
        self.update_jsonl_metadata_on_disk(paper_id)
        self._in_memory[paper_id] = data
        return None

    def transform(self, data: PaperData) -> PaperDetails:
        """Transform data before sending as json.

        For compatibility with data fetched with older API.

        Args:
            data: data for the paper

        """
        _data = PaperData(**dataclasses.asdict(data))
        _data.details.references = [x["citedPaper"] for x in _data.references.data]  # type: ignore
        _data.details.citations = [x["citingPaper"] for x in _data.citations.data]  # type: ignore
        return _data.details

    def details_url(self, ID: str) -> str:
        """Return the paper url for a given `ID`

        Args:
            ID: paper identifier

        """
        fields = ",".join(self.config.details.fields)
        return f"{self._root_url}/paper/{ID}?fields={fields}"

    def citations_url(self, ID: str, num: int = 0, offset: Optional[int] = None) -> str:
        """Return the citations url for a given `ID`

        Args:
            ID: paper identifier
            num: number of citations to fetch in the url
            offset: offset from where to fetch in the url

        """
        fields = ",".join(self.config.citations.fields)
        limit = num or self.config.citations.limit
        url = f"{self._root_url}/paper/{ID}/citations?fields={fields}&limit={limit}"
        if offset is not None:
            return url + f"&offset={offset}"
        else:
            return url

    def references_url(self, ID: str, num: int = 0) -> str:
        """Return the references url for a given :code:`ID`

        Args:
            ID: paper identifier
            num: number of citations to fetch in the url

        """
        fields = ",".join(self.config.references.fields)
        limit = num or self.config.references.limit
        return f"{self._root_url}/paper/{ID}/references?fields={fields}&limit={limit}"

    def _get(self, url: str):
        """Synchronously get a URL with the API key if present.

        Args:
            url: URL

        """
        # response = requests.get(url, headers=self.headers)
        # return response
        result = asyncio.run(self._get_some_urls([url]))
        return result[0]

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

        """
        if timeout is None:
            timeout = self._aio_timeout  # type: ignore
        else:
            timeout = aiohttp.ClientTimeout(timeout)  # type: ignore
        try:
            async with aiohttp.ClientSession(headers=self.headers,
                                             timeout=timeout) as session:
                tasks = [self._aget(session, url) for url in urls]
                results = await asyncio.gather(*tasks)
        except asyncio.exceptions.TimeoutError:
            return []
        return results

    def _post(self, url: str, data):
        """Synchronously get a URL with the API key if present.

        Args:
            url: URL

        """
        # response = requests.get(url, headers=self.headers)
        # return response
        result = asyncio.run(self._post_some_urls([url], [data]))
        return result[0]

    async def _apost(self, session: aiohttp.ClientSession, url: str, data) -> dict:
        """Asynchronously get a url.

        Args:
            sesssion: An :class:`aiohttp.ClientSession` instance
            url: The url to fetch

        """
        resp = await session.request('POST', url=url, json=json_dumps(data))
        data = await resp.json()
        return data

    async def _post_some_urls(self, urls: list[str], data: list, timeout: Optional[int] = None) -> list:
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
                tasks = [self._apost(session, url, _data) for url, _data in zip(urls, data)]
                results = await asyncio.gather(*tasks)
        except asyncio.exceptions.TimeoutError:
            return []
        return results

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
        # async with aiohttp.ClientSession(headers=self.headers) as session:
        #     tasks = [self._aget(session, url) for url in urls]
        #     results = await asyncio.gather(*tasks)
        # NOTE: mypy can't resolve zip of async gather
        data = dict(zip(["details", "references", "citations"], results))
        return data

    def store_details_and_get(
            self, ID: str, no_transform: bool,
            quiet: bool = False,
            force: bool = False) -> Error | PaperData | PaperDetails:
        """Get paper details asynchronously and store them.

        Fetch paper details, references and citations async.

        Store data in cache

        Args:
            ID: paper identifier
            no_transform: Flag to not apply the transform

        """
        result = asyncio.run(self._paper(ID))
        try:
            data = PaperData(**result)
        except TypeError:
            return Error(message="Could not parse data", error=json_dumps(result))
        maybe_error = self._update_memory_cache_metadata_and_save_to_disk(
            data, quiet=quiet, force=force)
        data = self._in_memory[data.details.paperId]
        if maybe_error:
            return maybe_error
        if no_transform:
            return data
        else:
            return self.transform(data)

    def fetch_from_cache_or_api(self, have_metadata: bool,
                                ID: str, force: bool,
                                no_transform: bool)\
            -> Error | PaperData | PaperDetails:
        """Subroutine to fetch from either disk or Semantic Scholar.

        Args:
            have_metadata: We already had the metadata
            ID: paper identifier
            force: Force fetch from Semantic Scholar server if True, ignoring cache
            no_transform: Return raw data and not paper details
                          The default behaviour is to return paper details

        """
        if have_metadata:
            self.logger.debug(f"Checking for cached data for {ID}")
            data = self._check_cache(ID)
            if not force:
                if data is not None:
                    return data if no_transform else self.transform(data)  # type: ignore
                else:
                    self.logger.debug(f"PaperDetails for {ID} stale or not present on disk. Will fetch.")
                    return self.store_details_and_get(ID, no_transform, quiet=True, force=True)
            else:
                self.logger.debug(f"Force fetching from Semantic Scholar for {ID}")
                return self.store_details_and_get(ID, no_transform)
        else:
            self.logger.debug(f"Fetching from Semantic Scholar for {ID}")
            return self.store_details_and_get(ID, no_transform)

    def id_to_corpus_id(self, id_type: IdTypes, ID: str) ->\
            Error | str | int:
        """Fetch :code:`CorpusId` for a given paper ID of type :code:`id_type`

        Args:
            id_type: Type of ID
            ID: The ID

        If paper data is not in cache, it's fetched first. Used primarily by
        external services.

        """
        ID = str(ID)
        if id_type not in IdTypes:
            return Error(message="INVALID ID TYPE")
        else:
            id_name = self.id_names[id_type]
            ssid = self._extid_metadata[id_name].get(ID, "")
            have_metadata = bool(ssid)
        if have_metadata:
            return self._metadata[ssid]["CorpusId"]
        data = self.fetch_from_cache_or_api(
            False, f"{id_name}:{ID}", False, False)
        if isinstance(data, Error):
            return data
        data = cast(PaperDetails, data)
        return data.externalIds["CorpusId"]

    def get_details_for_id(self, id_type: IdTypes, ID: str, force: bool, paper_data: bool)\
            -> Error | PaperData | PaperDetails:
        """Get paper details from Semantic Scholar Graph API

        The on disk cache is checked first and if it's a miss then the
        details are fetched from the server and stored in the cache.

        `force` force fetches the data from the API and updates the cache
        on the disk also.

        Args:
            id_type: Type of the paper identifier. One of IdTypes
            ID: paper identifier
            force: Force fetch from Semantic Scholar server, ignoring cache
            paper_data: Get PaperData instead of PaperDetails.

        """
        ID = str(ID)
        if id_type not in IdTypes:
            return Error(message="INVALID ID TYPE")
        elif id_type == IdTypes.ss:
            ssid = ID
            have_metadata = ssid in self._metadata
        else:
            id_name = self.id_names[id_type]
            ssid = self._extid_metadata[id_name].get(ID, "")
            have_metadata = bool(ssid)
        data = self.fetch_from_cache_or_api(
            have_metadata, ssid or f"{id_name}:{ID}", force, no_transform=paper_data)
        if paper_data or isinstance(data, Error):
            return data
        else:
            return self.apply_limits(data)

    def paper_data(self, ID: str, force: bool = False) ->\
            Error | PaperData:
        """Get paper data exactly as stored on disk, for paper with SSID :code:`ID`

        This is basically a convenience function instead of :meth:`get_details_for_id`
        where :code:`id_type` is set to :code:`ss`

        Args:
            ID: SSID of the paper
            force: Whether to force fetch from service

        """
        return self.get_details_for_id(IdTypes.ss, ID, force, paper_data=True)

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
        return self.get_details_for_id(IdTypes.ss, ID, force, paper_data=False)

    def apply_limits(self, data: PaperDetails) -> PaperDetails:
        """Apply count limits to S2 data citations and references

        Args:
            data: S2 Data

        Limits are defined in configuration

        """
        if data.citations:
            limit = self.config.citations.limit
            data.citations = data.citations[:limit]
        if data.references:
            limit = self.config.references.limit
            data.references = data.references[:limit]
        return data

    def _get_details_from_disk(self, ID: str, quiet: bool = False) -> Optional[dict]:
        """Fetch S2 details from disk with SSID=ID

        Args:
            ID: SSID of the paper

        """
        data_file = self._cache_dir.joinpath(ID)
        if data_file.exists():
            if not quiet:
                self.logger.debug(f"Data for {ID} is on disk")
            with open(data_file, "rb") as f:
                data = f.read()
            return json.loads(data)  # type: ignore
        else:
            return None

    def _check_cache(self, ID: str, quiet: bool = False) -> Optional[PaperData]:
        """Check cache and return data for ID if found.

        First the `in_memory` cache is checked and then the on disk cache.

        Args:
            ID: Paper ID

        """
        if ID not in self._in_memory:
            if not quiet:
                self.logger.debug(f"Data for {ID} not in memory")
            data = self._get_details_from_disk(ID, quiet=quiet)
            if data:
                try:
                    paper_data = PaperData(**data)
                    self._in_memory[ID] = paper_data
                except TypeError:
                    if not quiet:
                        self.logger.debug(f"Stale data for {ID}")
                    return None
            else:
                self.logger.debug(f"Tried to load data for {ID} from disk but could not")
                return None
        else:
            self.logger.debug(f"Data for {ID} in memory")
        if ID in self._in_memory:
            return self._in_memory[ID]
        else:
            return None

    def citations(self, ID: str, offset: int, limit: int):
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
        if offset + limit > citation_count:
            limit = citation_count - offset
        if offset + limit > len(existing_citations):
            self.next_citations(ID, (offset + limit) - len(existing_citations))
            data = cast(PaperData, self._check_cache(ID))
            if data is None:
                self.logger.error("Got None from cache after fetching. This should not happen")  # type: ignore
            existing_citations = data.citations.data
        retval = existing_citations[offset:offset+limit]
        return [x.citingPaper for x in retval]

    # TODO: Although this fetches the appropriate data based on citations on disk
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
            if offset+limit > 10000 and self.corpus_cache is not None:
                corpus_id = data.details.externalIds["CorpusId"]
                if corpus_id:
                    citations = self._build_citations_from_stored_data(
                        corpus_id=corpus_id,
                        existing_ids=citations_corpus_ids(data),
                        cite_count=cite_count,
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
                maybe_fix_citation_data(citations)
            data.citations = self._update_citations(data.citations, citations)
            self._dump_paper_data(paper_id, data)
            return data.citations

    def filter_subr(self, key: str, citation_data: list[Citation], filters: dict[str, Any],
                    num: int) -> list[dict]:
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
                if key in citation:
                    try:
                        # kwargs only
                        filter_func = self.filters[filter_name]
                        status = status and filter_func(citation[key], **filter_args)
                    except Exception as e:
                        self.logger.debug(f"Can't apply filter {filter_name} on {citation}: {e}")
                        status = False
                else:
                    status = False
            if status:
                retvals.append(citation)
            if num and len(retvals) == num:
                break
        # NOTE: Gives error because x[key] evals to str | dict[str, str]
        return [x[key] for x in retvals]

    def _batch_urls(self, n: int, url_prefix: str):
        """Generate a list of urls in batch size of :attr:`batch_size`

        Args:
            n: number of urls
            url_prefix: The prefix for the URL without offset and limit arguments.
                        It'll be formattted with :code:`offset` and :code:`limit`

        The :code:`url_prefix` should be complete with only the :code:`limit` and :code:`offset`
        parts remaining.

        """
        urls = []
        batch_size = self.batch_size
        offset = 0
        iters = min(10, math.ceil(n / batch_size))
        for i in range(iters):
            limit = 9999 - (offset + i * batch_size)\
                if (offset + i * batch_size) + batch_size > 10000 else batch_size
            urls.append(f"{url_prefix}&limit={limit}&offset={offset + i * batch_size}")
        return urls

    def _ensure_all_citations(self, ID: str) -> Citations:
        """Fetch all citations for a given paper_id :code:`ID`

        Args:
            ID: Paper ID

        This will fetch ALL citations which can be fetched from the S2 API
        and after that, using the data available from :attr:`corpus_cache`


        """
        data = self._check_cache(ID)
        if data is not None:
            cite_count = data.details.citationCount
            existing_cite_count = len(data.citations.data)
            # NOTE: We should always instead get from the head of the stream
            #       and then merge
            if cite_count > 10000:
                self.logger.warning("More than 10000 citations cannot be fetched "
                                    "with this function. Use next_citations for that. "
                                    "Will only get first 10000")
            fields = ",".join(self.config.citations.fields)
            url_prefix = f"{self._root_url}/paper/{ID}/citations?fields={fields}"
            urls = self._batch_urls(cite_count - existing_cite_count, url_prefix)
            self.logger.debug(f"Will fetch {len(urls)} requests for citations")
            self.logger.debug(f"All urls {urls}")
            with timer:
                results = asyncio.run(self._get_some_urls(urls))
            self.logger.debug(f"Got {len(results)} results")
            citations: Citations = Citations(next=0, offset=0, data=[])
            cite_list = []
            errors = 0
            for x in results:
                if "error" not in x:
                    cite_list.extend([Citation(**e) for e in x["data"]])
                else:
                    errors += 1
            citations.data = cite_list
            self.logger.debug(f"Have {len(cite_list)} citations without errors")
            if errors:
                self.logger.debug(f"{errors} errors occured while fetching all citations for {ID}")
            if all("next" in x for x in results):
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
            self.logger.debug(f"Fetching for j {j} out of {len(urls)//batch_size} urls")
            with timer:
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
            corpus_id: Semantic Scholar CorpusId
            existing_ids: Existing ids present if any
            cite_count: Total citationCount as given by S2 API
            limit: Total number of citations to fetch

        """
        if self.corpus_cache is not None:
            refs_ids = self.corpus_cache.get_citations(int(corpus_id))
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
            fields = ",".join(self.config.citations.fields).replace(",contexts", "")
            urls = [f"{self._root_url}/paper/CorpusID:{ID}?fields={fields}"
                    for ID in fetchable_ids]
            citations = Citations(offset=0, data=[])
            result = self._get_some_urls_in_batches(urls)
            for x in result:
                try:
                    citations.data.append(Citation(**{"citingPaper": x, "contexts": []}))  # type: ignore
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
        if self.corpus_cache is None:
            return None
        cite_count = len(existing_data.citations.data)
        existing_corpus_ids = [get_corpus_id(x) for x in existing_data.citations.data]
        if -1 in existing_corpus_ids:
            existing_corpus_ids.remove(-1)
        corpus_id = get_corpus_id(existing_data["details"])  # type: ignore
        if not corpus_id:
            raise AttributeError("Did not expect corpus_id to be 0")
        if corpus_id not in self._dont_build_citations:
            more_data = self._build_citations_from_stored_data(corpus_id,
                                                               existing_corpus_ids,
                                                               cite_count)
            new_ids = set([x.citingPaper["paperId"] for x in more_data.data  # type: ignore
                           if "paperId" in x.citingPaper])                   # type: ignore
            # NOTE: Some debug vars commented out
            # new_data_dict = {x["citingPaper"]["paperId"]: x["citingPaper"]
            #                  for x in more_data["data"]
            #                  if "citingPaper" in x and "error" not in x["citingPaper"]
            #                  and "paperId" in x["citingPaper"]}
            # existing_citation_dict = {x["citingPaper"]["paperId"]: x["citingPaper"]
            #                           for x in existing_data["citations"]["data"]}

            existing_ids = set(x.citingPaper["paperId"]  # type: ignore
                               for x in existing_data.citations.data)
            something_new = new_ids - existing_ids
            if more_data and more_data.data and something_new:
                self.logger.debug(f"Fetched {len(more_data.data)} in {timer.time} seconds")
                existing_data.citations = self._update_citations(more_data, existing_data.citations)
                update = True
        self._dont_build_citations.add(corpus_id)
        return update

    def filter_citations(self, ID: str, filters: dict[str, Any], num: int = 0) -> list[dict]:
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
            if abs(cite_count - existing_cite_count) > self.tolerance:
                with timer:
                    citations = self._ensure_all_citations(ID)
                self.logger.debug(f"Fetched {len(citations.data)} in {timer.time} seconds")
                self._update_citations(citations, paper_data.citations)
                if len(paper_data.citations.data) > existing_cite_count:
                    self.logger.debug(f"Fetched {len(paper_data.citations.data) - existing_cite_count}"
                                      " new citations")
                    update = True
                if cite_count > 10000:
                    _update = self._maybe_fetch_citations_greater_than_10000(paper_data)
                    update = update or _update
                if update:
                    self._dump_paper_data(ID, paper_data)
            return self.filter_subr("citingPaper", paper_data.citations.data, filters, num)

    def filter_references(self, ID: str, filters: dict[str, Any], num: int = 0):
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
        return self.filter_subr("citedPaper", references, filters, num)

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
            return json_dumps(results)
        else:
            return json_dumps({"error": json.loads(response.content)})

    def author_url(self, ID: str) -> str:
        """Return the author url for a given :code:`ID`

        Args:
            ID: author identifier

        """
        fields = ",".join(self.config.author.fields)
        limit = self.config.author.limit
        return f"{self._root_url}/author/{ID}?fields={fields}&limit={limit}"

    def author_papers_url(self, ID: str) -> str:
        """Return the author papers url for a given :code:`ID`

        Args:
            ID: author identifier

        """
        fields = ",".join(self.config.author_papers.fields)
        limit = self.config.author_papers.limit
        return f"{self._root_url}/author/{ID}/papers?fields={fields}&limit={limit}"

    async def _author(self, ID: str) -> dict:
        """Fetch the author data from the API

        Args:
            ID: author identifier

        """
        urls = [f(ID) for f in [self.author_url, self.author_papers_url]]
        results = await self._get_some_urls(urls)
        # async with aiohttp.ClientSession(headers=self.headers) as session:
        #     tasks = [self._aget(session, url) for url in urls]
        #     results = await asyncio.gather(*tasks)
        return dict(zip(["author", "papers"], results))

    def get_author_papers(self, ID: str) -> dict:
        """Return the author papers for a given :code:`ID`

        Args:
            ID: author identifier

        """
        result = asyncio.run(self._author(ID))
        return {"author": result["author"],
                "papers": result["papers"]["data"]}

    def search(self, query: str) -> str | bytes:
        """Search for query string on Semantic Scholar with graph search API.

        Args:
            query: query to search

        """
        terms = "+".join(re.sub(r"[^a-z0-9]", " ", query, flags=re.IGNORECASE).split(" "))
        fields = ",".join(self.config.search.fields)
        limit = self.config.search.limit
        url = f"{self._root_url}/paper/search?query={terms}&fields={fields}&limit={limit}"
        return self._get(url)
