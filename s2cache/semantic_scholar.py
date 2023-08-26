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

import yaml
import aiohttp
from common_pyutil.monitor import Timer

from .models import (Pathlike, Cache, Config, SubConfig, Details, Citation, Citations, StoredData)
from .filters import (year_filter, author_filter, num_citing_filter,
                      num_influential_count_filter, venue_filter, title_filter)
from .corpus_data import CorpusCache
from .config import default_config, load_config


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


def get_corpus_id(data: Citation | Details) -> int:
    """Get :code:`corpusId` field from :class:`Citation` or :class:`Details`

"corpus":    Args:
        data: Detials or Citation data


    """
    if "externalIds" in data:
        if data["externalIds"]:                   # type: ignore
            cid = data["externalIds"]["CorpusId"]  # type: ignore
            return int(cid)
    elif "citingPaper" in data:
        if data["citingPaper"]["externalIds"]:                   # type: ignore
            cid = data["citingPaper"]["externalIds"]["CorpusId"]  # type: ignore
            return int(cid)
    return -1


def citations_corpus_ids(data: StoredData) -> list[int]:
    return [int(x["citingPaper"]["externalIds"]["CorpusId"])
            for x in data["citations"]["data"]]



class SemanticScholar:
    """A Semantic Scholar API client with a files based cache.

    The cache is a Dictionary of type :code:`Cache` where they keys are one of
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
                 corpus_cache_dir: Optional[Pathlike] = None,
                 logger_name: Optional[str] = None):
        self._config = default_config()
        if config_file:
            load_config(self._config, config_file)
        self.logger = logging.getLogger(logger_name or "s2cache")
        self._init_cache(cache_dir)
        self._api_key = self._config.api_key
        self._root_url = "https://api.semanticscholar.org/graph/v1"
        # NOTE: Actually the SS limit is 100 reqs/sec. This applies to all requests I think
        #       and there's no way to change this. This is going to fetch fairly slowly
        #       Since my timeout is 5 secs, I can try fetching with 500
        self._batch_size = 500
        self._tolerance = 10
        self._dont_build_citations: set = set()
        self._aio_timeout = aiohttp.ClientTimeout(10)
        self.load_metadata()
        self.maybe_load_corpus_cache(corpus_cache_dir)

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

    def _init_cache(self, cache_dir: Pathlike):
        """Initialize the cache from :code:`cache_dir`

        Args:
            cache_dir: The directory where cache files reside


        """
        _cache_dir = (cache_dir or self._config.cache_dir)
        if not _cache_dir or not Path(_cache_dir).exists():
            raise FileNotFoundError(f"{_cache_dir} doesn't exist")
        else:
            self._cache_dir = Path(_cache_dir)
        self._cache: Cache = {}
        self._in_memory: dict[str, StoredData] = {}
        self._rev_cache: dict[str, list[str]] = {}
        self._files: list[str] = [*filter(
            lambda x: not x.endswith("~") and "metadata" not in x and x != "cache",
            os.listdir(self._cache_dir))]
        self.id_keys = ['DOI', 'MAG', 'ARXIV', 'ACL', 'PUBMED', 'URL', 'CorpusId']
        self.id_keys.sort()

    def _maybe_fix_invalid_entries(self, metadata: list):
        """Find and fix invalid entries if they exist.

        Args:
            metadata: The metadata :class:`dict`


        """
        entry_length = len(self.id_keys) + 1
        invalid_entries = [[i, x, len(x.split(","))]
                           for i, x in enumerate(metadata)
                           if len(x.split(",")) != entry_length]
        if invalid_entries:
            self.logger.debug(f"Invalid entries in metadata "
                              f"{os.path.join(self._cache_dir, 'metadata')}.\n"
                              f"At lines: {','.join([str(x[0]) for x in invalid_entries])}")
            for k, v, _ in invalid_entries:
                rest, paper_id = v.rsplit(",", 1)
                val = ",".join([rest, ",,", paper_id])
                metadata[k] = val

    # FIXME: metadata is loaded from CSV with positional values. This should be
    #        keyword based YAML or JSON
    def load_metadata(self):
        """Load the Semantic Scholar metadata from the disk.

        The cache is indexed as a file in :code:`metadata` and the file data itself is
        named as the Semantic Scholar :code:`corpusId` for the paper. We load metadata on
        startup and fetch the rest as needed.

        Args:
            data_dir: Directory where the cache is located

        """
        metadata_file = self._cache_dir.joinpath("metadata")
        if metadata_file.exists():
            with open(metadata_file) as f:
                metadata = [*filter(None, f.read().split("\n"))]
        else:
            metadata = []
        self._maybe_fix_invalid_entries(metadata)
        self._cache = {k: {} for k in self.id_keys}
        self._rev_cache = {}
        dups = False
        for _ in metadata:
            c = _.split(",")
            if c[-1] in self._rev_cache:
                dups = True
                self._rev_cache[c[-1]] = [x or y for x, y in zip(self._rev_cache[c[-1]], c[:-1])]
            else:
                self._rev_cache[c[-1]] = c[:-1]
            for ind, key in enumerate(self.id_keys):
                if c[ind]:
                    self._cache[key][c[ind]] = c[-1]
        self.logger.debug(f"Loaded SS cache {len(self._rev_cache)} entries and " +
                          f"{sum(len(x) for x in self._cache.values())} keys.")
        if dups:
            self.logger.debug("There were duplicates. Writing new metadata")
            self.dump_metadata()

    def maybe_load_corpus_cache(self, corpus_cache_dir: Optional[Pathlike]):
        """Load :code:`CorpusCache` if given

        Args:
            corpus_cache_dir: The root directory where the cache is stored


        """
        self.corpus_cache_dir = corpus_cache_dir
        if self.corpus_cache_dir and Path(self.corpus_cache_dir).exists():
            self._corpus_cache: CorpusCache | None = CorpusCache(self.corpus_cache_dir)
            self.logger.debug(f"Loaded Full Semantic Scholar Citations Cache from {self.corpus_cache_dir}")
        else:
            self._corpus_cache = None
            self.logger.debug("Refs Cache doesn't exist, not loading")

    def external_id_to_name(self, ext_id: str):
        """Change the ExternalId returned by the S2 API to the name

        Args:
            ext_id: External ID

        """
        return "CorpusId" if ext_id.lower() == "corpusid" else ext_id.upper()

    def dump_metadata(self):
        """Dump metadata to disk.

        """
        with open(self._cache_dir.joinpath("metadata"), "w") as f:
            f.write("\n".join([",".join([*v, k]) for k, v in self._rev_cache.items()]))
        self.logger.debug("Dumped metadata")

    def update_metadata(self, paper_id: str):
        """Update Metadata on the disk

        Args:
            paper_id: The S2 paper ID

        """
        with open(os.path.join(self._cache_dir, "metadata"), "a") as f:
            f.write("\n" + ",".join([*map(str, self._rev_cache[paper_id]), paper_id]))
        self.logger.debug("Updated metadata")

    def _get(self, url: str):
        """Synchronously get a URL with the API key if present.

        Args:
            url: URL

        """
        # response = requests.get(url, headers=self.headers)
        # return response
        result = asyncio.run(self._get_some_urls([url]))
        return result[0]

    def _dump_paper_data(self, ID: str, data: StoredData):
        """Dump the paper :code:`data` for paperID :code:`ID`

        Args:
            ID: The paper ID
            data: The paper data

        The data is stored as JSON.

        """
        fname = os.path.join(self._cache_dir, str(ID))
        if len(data["citations"]["data"]) > data["details"]["citationCount"]:
            data["details"]["citationCount"] = len(data["citations"]["data"])
        with timer:
            with open(fname, "w") as f:
                json.dump(data, f)
        self.logger.debug(f"Wrote file {fname} in {timer.time} seconds")

    def _update_citations(self, existing_citation_data: Citations, new_citation_data: Citations):
        """Update from :code:`existing_citation_data` from :code:`new_citation_data`

        Args:
            existing_citation_data: Existing data
            new_citation_data: New data

        """
        # NOTE: Ignoring as the dict isn't uniform and it raises error
        existing_citation_data["data"] = [*filter(lambda x: "error" not in x["citingPaper"],
                                                  existing_citation_data["data"])]
        new_citation_data["data"] = [*filter(lambda x: "error" not in x["citingPaper"],
                                             new_citation_data["data"])]
        data_ids_b = {x["citingPaper"]["paperId"]
                      for x in new_citation_data["data"]}  # type: ignore
        for x in existing_citation_data["data"]:
            if x["citingPaper"]["paperId"] not in data_ids_b:  # type: ignore
                new_citation_data["data"].append(x)
                if "next" in new_citation_data:
                    new_citation_data["next"] += 1

    def _update_memory_cache_and_save_to_disk(self, data: StoredData):
        """Update paper details, references and citations on disk.

        We read and write data for individual papers instead of one big json
        object.

        The data is strored as a dictionary with keys :code:`["details", "references", "citations"]`

        Args:
            data: data for the paper

        """
        details = data["details"]
        paper_id = details["paperId"]
        # NOTE: In case force updated and already some citations exist on disk
        existing_data = self._check_cache(paper_id)
        if existing_data is not None:
            self._update_citations(existing_data["citations"], data["citations"])
        self._dump_paper_data(paper_id, data)
        ext_ids = {self.external_id_to_name(k): str(v)
                   for k, v in details["externalIds"].items()}
        other_ids = [ext_ids.get(k, "") for k in self.id_keys]
        for ind, key in enumerate(self.id_keys):
            if other_ids[ind]:
                self._cache[key][other_ids[ind]] = paper_id
        existing = self._rev_cache.get(paper_id, None)
        if existing:
            self._rev_cache[paper_id] = [x or y for x, y in
                                         zip(self._rev_cache[paper_id], other_ids)]
        else:
            self._rev_cache[paper_id] = other_ids
            self.update_metadata(paper_id)
        self._in_memory[paper_id] = data

    def transform(self, data: StoredData | Details) -> Details:
        """Transform data before sending as json.

        For compatibility with data fetched with older API.

        Args:
            data: data for the paper

        """
        if "details" in data:
            data["details"]["references"] = [x["citedPaper"] for x in data["references"]["data"]]  # type: ignore
            data["details"]["citations"] = [x["citingPaper"] for x in data["citations"]["data"]]  # type: ignore
            return data["details"]  # type: ignore
        else:
            return data         # type: ignore

    def details_url(self, ID: str) -> str:
        """Return the paper url for a given `ID`

        Args:
            ID: paper identifier

        """
        fields = ",".join(self._config.details.fields)
        return f"{self._root_url}/paper/{ID}?fields={fields}"

    def citations_url(self, ID: str, num: int = 0, offset: Optional[int] = None) -> str:
        """Return the citations url for a given `ID`

        Args:
            ID: paper identifier
            num: number of citations to fetch in the url
            offset: offset from where to fetch in the url

        """
        fields = ",".join(self._config.citations.fields)
        limit = num or self._config.citations.limit
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
        fields = ",".join(self._config.references.fields)
        limit = num or self._config.references.limit
        return f"{self._root_url}/paper/{ID}/references?fields={fields}&limit={limit}"

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
            timeout = cast(aiohttp.ClientTimeout, self._aio_timeout)
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

    async def _paper(self, ID: str) -> StoredData:
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
        data: StoredData = dict(zip(["details", "references", "citations"], results))  # type: ignore
        return data

    def store_details_and_get(self, ID: str, no_transform: bool) -> StoredData | Details:
        """Get paper details asynchronously and store them.

        Fetch paper details, references and citations async.

        Store data in cache

        Args:
            ID: paper identifier
            no_transform: Flag to not apply the transform

        """
        result = asyncio.run(self._paper(ID))
        self._update_memory_cache_and_save_to_disk(result)
        if no_transform:
            return result
        else:
            return self.transform(result)

    def fetch_from_cache_or_api(self, have_metadata: bool,
                                ID: str, force: bool,
                                no_transform: bool)\
            -> StoredData | Details:
        """Subroutine to fetch from either disk or Semantic Scholar.

        Args:
            have_metadata: We already had the metadata
            ID: paper identifier
            force: Force fetch from Semantic Scholar server if True, ignoring cache

        """
        if have_metadata:
            self.logger.debug(f"Checking for cached data for {ID}")
            data = self._check_cache(ID)
            if not force:
                if data is not None:
                    # NOTE: StoredData and data are both dict
                    return data if no_transform else self.transform(data)  # type: ignore
                else:
                    self.logger.debug(f"Details for {ID} not present on disk. Will fetch.")
                    return self.store_details_and_get(ID, no_transform)
            else:
                self.logger.debug(f"Force fetching from Semantic Scholar for {ID}")
                return self.store_details_and_get(ID, no_transform)
        else:
            self.logger.debug(f"Fetching from Semantic Scholar for {ID}")
            return self.store_details_and_get(ID, no_transform)

    def id_to_corpus_id(self, id_type: IdTypes, ID: str) -> str | int:
        """Fetch :code:`CorpusId` for a given paper ID of type :code:`id_type`

        Args:
            id_type: Type of ID
            ID: The ID

        If paper data is not in cache, it's fetched first. Used primarily by
        external services.

        """
        ID = str(ID)
        if id_type not in IdTypes:
            return "INVALID ID TYPE"
        else:
            id_name = self.id_names[id_type]
            ssid = self._cache[id_name].get(ID, "")
            have_metadata = bool(ssid)
        if have_metadata:
            return self._rev_cache[ssid][2]
        data = self.fetch_from_cache_or_api(
            False, f"{id_name}:{ID}", False, False)
        return data['externalIds']['CorpusId']

    def get_details_for_id(self, id_type: IdTypes, ID: str, force: bool, all_data: bool)\
            -> str | dict:
        """Get paper details from Semantic Scholar Graph API

        The on disk cache is checked first and if it's a miss then the
        details are fetched from the server and stored in the cache.

        `force` force fetches the data from the API and updates the cache
        on the disk also.

        Args:
            id_type: type of the paper identifier one of
                     `['ss', 'doi', 'mag', 'arxiv', 'acl', 'pubmed', 'corpus']`
            ID: paper identifier
            force: Force fetch from Semantic Scholar server, ignoring cache
            all_data: Fetch all details if possible. This can only be done if
                      the data already exists on disk
        """
        ID = str(ID)
        if id_type not in IdTypes:
            return "INVALID ID TYPE"
        elif id_type == IdTypes.ss:
            ssid = ID
            have_metadata = ssid in self._rev_cache
        else:
            id_name = self.id_names[id_type]
            ssid = self._cache[id_name].get(ID, "")
            have_metadata = bool(ssid)
        data = self.fetch_from_cache_or_api(
            have_metadata, ssid or f"{id_name}:{ID}", force, False)
        if all_data:
            return data
        else:
            return self.apply_limits(data)

    def details(self, ID: str, force: bool = False, all_data: bool = False) -> str | dict:
        """Get details for paper with SSID :code:`ID`

        This is basically a convenience function instead of :meth:`get_details_for_id`
        where :code:`id_type` is set to :code:`ss`

        Args:
            ID: SSID of the paper
            force: Whether to force fetch from service
            all_data: Try to fetch all citations

        """
        return self.get_details_for_id(IdTypes.ss, ID, force, all_data)

    def apply_limits(self, data: Details) -> Details:
        """Apply count limits to S2 data citations and references

        Args:
            data: S2 Data

        Limits are defined in configuration

        """
        _data = data.copy()
        if "citations" in data:
            limit = self._config.citations.limit
            _data["citations"] = _data["citations"][:limit]
        if "references" in data:
            limit = self._config.references.limit
            _data["references"] = _data["references"][:limit]
        return _data

    def _get_details_from_disk(self, ID: str) -> Optional[StoredData]:
        """Fetch S2 details from disk with SSID=ID

        Args:
            ID: SSID of the paper

        """
        data_file = self._cache_dir.joinpath(ID)
        if data_file.exists():
            self.logger.debug(f"Data for {ID} is on disk")
            with open(data_file, "rb") as f:
                data = f.read()
            return json.loads(data)
        else:
            return None

    def _validate_fields(self, data: StoredData) -> bool:
        """Validate the fields of data fetched from disk

        Args:
            data: Paper data fetched from disk


        """
        details_fields = self._config.details.fields.copy()
        references_fields = self._config.references.fields.copy()
        check_contexts = False
        if "contexts" in references_fields:
            references_fields.remove("contexts")
            check_contexts = True
        citations_fields = self._config.citations.fields.copy()
        if "contexts" in citations_fields:
            citations_fields.remove("contexts")
        if all(x in data for x in ["details", "references", "citations"]):
            valid_details = all([f in data["details"] for f in details_fields])
            if data["references"]["data"]:
                valid_refs = all([f in data["references"]["data"][0]["citedPaper"]
                                  for f in references_fields])
            else:
                valid_refs = True
            if data["citations"]["data"]:
                valid_cites = all([f in data["citations"]["data"][0]["citingPaper"]
                                   for f in citations_fields])
            else:
                valid_cites = True
            if check_contexts:
                valid_refs = valid_refs and data["references"]["data"] and\
                    "contexts" in data["references"]["data"][0]
                valid_cites = valid_cites and data["citations"]["data"] and\
                    "contexts" in data["citations"]["data"][0]
            return valid_details and valid_refs and valid_cites
        else:
            return False

    def _check_cache(self, ID: str) -> Optional[StoredData]:
        """Check cache and return data for ID if found.

        First the `in_memory` cache is checked and then the on disk cache.

        Args:
            ID: Paper ID

        """
        if ID not in self._in_memory:
            self.logger.debug(f"Data for {ID} not in memory")
            data = self._get_details_from_disk(ID)
            if data:
                if self._validate_fields(data):
                    data["citations"]["data"] =\
                        [x for x in data["citations"]["data"] if "paperId" in x["citingPaper"]]
                    data["references"]["data"] =\
                        [x for x in data["references"]["data"] if "paperId" in x["citedPaper"]]
                    self._in_memory[ID] = data
                else:
                    self.logger.debug(f"Stale data for {ID}")
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
        data = cast(StoredData, data)
        existing_citations = data["citations"]["data"]
        citation_count = data["details"]["citationCount"]
        if offset + limit > citation_count:
            limit = citation_count - offset
        if offset + limit > len(existing_citations):
            self.next_citations(ID, (offset + limit) - len(existing_citations))
            data = cast(StoredData, self._check_cache(ID))
            if data is None:
                self.logger.error("Got None from cache after fetching. This should not happen")  # type: ignore
            existing_citations = data["citations"]["data"]
        retval = existing_citations[offset:offset+limit]
        return [x["citingPaper"] for x in retval]

    # TODO: Although this fetches the appropriate data based on citations on disk
    #       the offset and limit handling is tricky and is not correct right now.
    # TODO: What if the num_citations change between the time we fetched earlier and now?
    def next_citations(self, ID: str, limit: int) -> Optional[dict]:
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
            return {"error": f"Data for {ID} not in cache"}
        elif data is not None and "next" not in data["citations"]:
            return None
        else:
            offset = data["citations"]["offset"]
            cite_count = data["details"]["citationCount"]
            if offset+limit > 10000 and self._corpus_cache is not None:
                corpus_id = get_corpus_id(data["details"])
                if corpus_id:
                    citations = self._build_citations_from_stored_data(
                        corpus_id=corpus_id,
                        existing_ids=citations_corpus_ids(data),
                        cite_count=cite_count,
                        offset=offset,
                        limit=limit)
            else:
                paper_id = data["details"]["paperId"]
                _next = data["citations"]["next"]
                if offset:
                    limit += offset - _next
                offset = _next
                url = self.citations_url(paper_id, limit, offset)
                citations = self._get(url)
            self._update_citations(citations, data["citations"])
            self._dump_paper_data(paper_id, data)
            return citations

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
        and after that, using the data available from :attr:`_corpus_cache`


        """
        data = self._check_cache(ID)
        if data is not None:
            cite_count = data["details"]["citationCount"]
            existing_cite_count = len(data["citations"]["data"])
            # NOTE: We should always instead get from the head of the stream
            #       and then merge
            if cite_count > 10000:
                self.logger.warning("More than 10000 citations cannot be fetched "
                                    "with this function. Use next_citations for that. "
                                    "Will only get first 10000")
            fields = ",".join(self._config.citations.fields)
            url_prefix = f"{self._root_url}/paper/{ID}/citations?fields={fields}"
            urls = self._batch_urls(cite_count - existing_cite_count, url_prefix)
            self.logger.debug(f"Will fetch {len(urls)} requests for citations")
            self.logger.debug(f"All urls {urls}")
            with timer:
                results = asyncio.run(self._get_some_urls(urls))
            self.logger.debug(f"Got {len(results)} results")
            result: Citations = {"next": 0, "offset": 0, "data": []}
            cite_list = []
            errors = 0
            for x in results:
                if "error" not in x:
                    cite_list.extend(x["data"])
                else:
                    errors += 1
            result["data"] = cite_list
            self.logger.debug(f"Have {len(cite_list)} citations without errors")
            if errors:
                self.logger.debug(f"{errors} errors occured while fetching all citations for {ID}")
            if all("next" in x for x in results):
                result["next"] = max([*[x["next"] for x in results if "error" not in x], 10000])
            else:
                result.pop("next")
            return result
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
    def _build_citations_from_stored_data(self, *,
                                          corpus_id: int | str,
                                          existing_ids: list[int],
                                          cite_count: int,
                                          offset: int = 0,
                                          limit: int = 0) -> Optional[Citations]:
        """Build the citations data for a paper entry from cached data

        Args:
            corpus_id: Semantic Scholar CorpusId
            existing_ids: Existing ids present if any
            cite_count: Total citationCount as given by S2 API
            limit: Total number of citations to fetch

        """
        if self._corpus_cache is not None:
            refs_ids = self._corpus_cache.get_citations(int(corpus_id))
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
            fields = ",".join(self._config.citations.fields).replace(",contexts", "")
            urls = [f"{self._root_url}/paper/CorpusID:{ID}?fields={fields}"
                    for ID in fetchable_ids]
            citations = {"offset": 0, "data": []}
            result = self._get_some_urls_in_batches(urls)
            citations["data"] = [{"citingPaper": x, "contexts": []} for x in result]
            return citations        # type: ignore
        else:
            self.logger.error("References Cache not present")
            return None

    def _maybe_fetch_citations_greater_than_10000(self, existing_data):
        """Fetch citations when their number is > 10000.

        SemanticScholar doesn't allow above 10000, so we have to build that
        from the dumped citation data. See :meth:`_build_citations_from_stored_data`

        Args:
            existing_data: Existing paper details data

        """
        if self._corpus_cache is None:
            return None
        cite_count = len(existing_data["citations"]["data"])
        existing_corpus_ids = [get_corpus_id(x) for x in existing_data["citations"]["data"]]
        if -1 in existing_corpus_ids:
            existing_corpus_ids.remove(-1)
        corpus_id = get_corpus_id(existing_data["details"])  # type: ignore
        if not corpus_id:
            raise AttributeError("Did not expect corpus_id to be 0")
        if corpus_id not in self._dont_build_citations:
            more_data = self._build_citations_from_stored_data(corpus_id,
                                                               existing_corpus_ids,
                                                               cite_count)
            new_ids = set([x["citingPaper"]["paperId"]
                           for x in more_data["data"]
                           if "citingPaper" in x and "error" not in x["citingPaper"]
                           and "paperId" in x["citingPaper"]])
            # NOTE: Some debug vars commented out
            # new_data_dict = {x["citingPaper"]["paperId"]: x["citingPaper"]
            #                  for x in more_data["data"]
            #                  if "citingPaper" in x and "error" not in x["citingPaper"]
            #                  and "paperId" in x["citingPaper"]}
            # existing_citation_dict = {x["citingPaper"]["paperId"]: x["citingPaper"]
            #                           for x in existing_data["citations"]["data"]}
            existing_ids = set(x["citingPaper"]["paperId"]
                               for x in existing_data["citations"]["data"])
            something_new = new_ids - existing_ids
            if more_data["data"] and something_new:
                self.logger.debug(f"Fetched {len(more_data['data'])} in {timer.time} seconds")
                self._update_citations(more_data, existing_data["citations"])
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
        existing_data = self._check_cache(ID)
        if existing_data is None:
            msg = f"data should not be None for ID {ID}"
            return msg          # type: ignore
        else:
            update = False
            cite_count = existing_data["details"]["citationCount"]
            existing_cite_count = len(existing_data["citations"]["data"])
            if abs(cite_count - existing_cite_count) > self.tolerance:
                with timer:
                    data = self._ensure_all_citations(ID)
                self.logger.debug(f"Fetched {len(data['data'])} in {timer.time} seconds")
                self._update_citations(data, existing_data["citations"])
                if len(existing_data["citations"]["data"]) > existing_cite_count:
                    self.logger.debug(f"Fetched {len(existing_data['citations']['data']) - existing_cite_count}"
                          " new citations")
                    update = True
                if cite_count > 10000:
                    _update = self._maybe_fetch_citations_greater_than_10000(existing_data)
                    update = update or _update
                if update:
                    self._dump_paper_data(ID, existing_data)
            return self.filter_subr("citingPaper", existing_data["citations"]["data"], filters, num)

    def filter_references(self, ID: str, filters: dict[str, Any], num: int = 0):
        """Like :meth:`filter_citations` but for references

        Args:
            ID: Paper ID
            filters: filter names and arguments
            num: Number of results to retrieve

        """
        data = self._check_cache(ID)
        if data is not None:
            references = data["references"]["data"]
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
            response = requests.post(root_url,
                                     json={"positivePaperIds": pos_ids,
                                           "negativePaperIds": neg_ids})
        else:
            response = requests.get(f"{root_url}/forpaper/{pos_ids[0]}")
        if response.status_code == 200:
            recommendations = json.loads(response.content)["recommendedPapers"]
            urls = [self.details_url(x["paperId"])
                    for x in recommendations]
            if count:
                urls = urls[:count]
            results = asyncio.run(self._get_some_urls(urls))
            return json.dumps(results)
        else:
            return json.dumps({"error": json.loads(response.content)})

    def author_url(self, ID: str) -> str:
        """Return the author url for a given :code:`ID`

        Args:
            ID: author identifier

        """
        fields = ",".join(self._config.author.fields)
        limit = self._config.author.limit
        return f"{self._root_url}/author/{ID}?fields={fields}&limit={limit}"

    def author_papers_url(self, ID: str) -> str:
        """Return the author papers url for a given :code:`ID`

        Args:
            ID: author identifier

        """
        fields = ",".join(self._config.author_papers.fields)
        limit = self._config.author_papers.limit
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
        fields = ",".join(self._config.search.fields)
        limit = self._config.search.limit
        url = f"{self._root_url}/paper/search?query={terms}&fields={fields}&limit={limit}"
        return self._get(url)
