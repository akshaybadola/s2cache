from typing import Optional, cast, Iterable
import sys
import glob
import gzip
import json
from collections import defaultdict
import os
import pickle
from pathlib import Path
import dataclasses

import mysql.connector
import requests
from common_pyutil.monitor import Timer

from .models import PaperDetails, CitationData, Pathlike


__doc__ = """Module to process Semantic Scholar Citation Data dump."""
timer = Timer()


def get_latest_download_links(dataset_name: str, api_key: str):
    """Get all the links for the latest release of the S2 data of your choice

    Args:
        dataset_name: Name of the dataset
        api_key: API key

    """
    api_url = "https://api.semanticscholar.org/datasets/v1"
    resp = requests.get(f"{api_url}/release")
    releases = resp.json()
    releases.sort()
    latest = releases[-1]
    resp = requests.get(f"{api_url}/release/{latest}/dataset/{dataset_name}",
                        headers={"x-api-key": api_key})
    links = resp.json()
    return links


def process_all_citation_data(root_dir: Pathlike):
    """Process all the Semantic Scholar Citation data in :code:`root_dir`

    Args:
        root_dir: Directory where the gzipped files of citations are stored


    """
    root_dir = Path(root_dir)
    if not root_dir.exists():
        raise ValueError(f"No such directory {root_dir}")
    parse_and_dump_citation_data(root_dir)
    split_and_dump(root_dir)


def parse_and_dump_citation_data(root_dir: Path):
    """Parse the downloaded gzipped citation data, consolidate, sort and
    dump again.

    Args:
        root_dir: Directory where all the gzipped data files are kept

    This may require A LOT of RAM as the entire citations data is consolidated
    in an adjacency list. This large pickle is split later.

    """
    citations: dict[int, set[int]] = defaultdict(set)
    # references: dict[int, set[int]] = defaultdict(set)
    filenames = glob.glob(str(root_dir.joinpath("*gz")))
    for f_num, filename in enumerate(filenames):
        with gzip.open(filename, "rt") as s2_file:
            for i, line in enumerate(s2_file):
                data = json.loads(line)
                if data["citedcorpusid"] and data["citingcorpusid"]:
                    a, b = int(data["citedcorpusid"]), int(data["citingcorpusid"])
                    citations[a].add(b)
                    # references[b].add(a)
                if not (i+1) % 1000000:
                    print(f"{i+1} done for file {filename}")
        print(f"Done file {f_num+1} out of {len(filenames)}")
    citations_file = root_dir.joinpath("citations.pkl")
    # references_file = root_dir.joinpath("references.pkl")
    print(f"Writing file {citations_file}")
    with open(citations_file, "wb") as f:
        pickle.dump(citations, f)
    # with open(references_file, "wb") as f:
    #     pickle.dump(citations, f)


def save_temp(output_dir: Path, data: dict, i: int):
    """Dump a temp pickle file of adjacency list

    Args:
        output_dir: Output directory to save the file
        temp: The temporary output file
        i: The numeric suffix for the file

    """
    with timer:
        with open(output_dir.joinpath(f"temp_{i:010}.pkl"), "wb") as f:
            pickle.dump(data, f)
    print(f"Dumped for {i} in {timer.time} seconds")


def split_and_dump_subr(input_dir: Path, output_dir: Path,
                        citations: dict, max_key: int):
    """Split, bin and dump the citations

    Args:
        input_dir: Input Directory
        output_dir: Output Directory
        citations: Citations loaded from pickle file
        max_key: Max value of all keys

    The adjacency list of citations :code:`citations` is sorted by :code:`corpusId` and
    stored in smaller :meth:`pickle` files which contain binned splits of the data.

    The files are numerically named :code:`xxxxxxxxxx.pkl`, so that each
    file contains the citations with :code:`corpus id < fname`
    and :code:`corpus id > previous_fname`

    """
    j = 0
    while True:
        temp = {}
        a = j * 1000000
        b = (j+1) * 1000000
        if os.path.exists(input_dir.joinpath(f"temp_{b:010}.pkl")):
            print(f"skipping for {b:010}")
            continue
        with timer:
            for i in range(a, b):
                if i in citations:
                    temp[i] = citations[i].copy()
                if i > max_key:
                    save_temp(output_dir, temp, b)
                    return
        print(f"Done for {b} in {timer.time} seconds")
        save_temp(output_dir, temp, b)
        j += 1


def split_and_dump(root_dir: Path):
    """Read the citations.pkl file and split them based on corpus_id

    Args:
        root_dir: Root directory where citations reside


    """
    for fname in ["citations.pkl", "references.pkl"]:
        with timer:
            with open(root_dir.joinpath(fname), "rb") as f:
                citations = pickle.load(f)
        print(f"Loaded citations in {timer.time} seconds")
        keys = [*citations.keys()]
        max_key = max(keys)
        split_and_dump_subr(root_dir, root_dir, citations, max_key)


def convert_keys_from_numpy(cache):
    """Convert cache keys from :class:`numpy.int64` to :class:`int`

    Used once when keys were taken from numpy

    Args:
        cache: :class:`CitationsCache`

    """
    for i, cf in enumerate(cache.files.values()):
        print(f"Opening {i+1} file")
        with open(cf, "rb") as fp:
            data = pickle.load(fp)
        out_data = defaultdict(set)
        for k, v in data.items():
            out_data[int(k)] = v
        with open(cf, "wb") as fp:
            pickle.dump(out_data, fp)
        print(f"Done {i+1} file")


class ReferencesCache:
    """A Semantic Scholar Papers local cache.

    Consists of adjacency list of
    papers and their references stored in pickle format.

    """
    def __init__(self, refs_file: Pathlike):
        with open(refs_file, "rb") as f:
            self._references: dict[int, set[int]] = pickle.load(f)

    def get_references(self, corpusid: int) -> Optional[set[int]]:
        return self._references.get(corpusid, None)


class PapersCache:
    """A Semantic Scholar Papers local cache.

    Consists of gzipped text files containing paper data along with abstracts.
    OR maybe store it all in mariadb with gzip compression.

    The pickle files are stored such that :code:`corpusId` of a :code:`citingPaper`
    is smaller than :code:`temp_{suffix}` where :code:`suffix` is an integer

    Args:
        user_name: mariadb username (MAYBE)
        password: mariadb username (MAYBE)
        db_name: mariadb DB name (MAYBE)
        table_name: mariadb TABLE name (MAYBE)

    """
    def __init__(self, host, user, password, database, table_name):
        self._fields_map = {x.name.lower(): x.name
                            for x in dataclasses.fields(PaperDetails)}
        self._table_name = table_name
        self.conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )

    def execute_sql(self, cursor, query, params=None):
        try:
            if params is not None:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            if cursor.description:
                column_names = [description[0] for description in cursor.description]
            else:
                column_names = None
            result = cursor.fetchall()
            self.conn.commit()
        except Exception as e:
            print(f"Error {e} for query {query}")
            return None, None
        return result, column_names

    def insert_data(self, data: dict):
        if self.conn.is_closed():
            self.conn.reconnect()
        cursor = self.conn.cursor()
        keys = ",".join(k.upper() for k in data.keys())
        vals = ",".join(["%s"] * len(data))
        query = f"INSERT INTO {self._table_name} ({keys}) VALUES ({vals});"
        result, column_names = self.execute_sql(cursor, query, [*data.values()])
        return result

    def select_data(self, condition: str = ""):
        if self.conn.is_closed():
            self.conn.reconnect()
        cursor = self.conn.cursor()
        if condition:
            query = f"SELECT * FROM {self._table_name} WHERE {condition};"
        else:
            query = f"SELECT * FROM {self._table_name};"
        result, column_names = self.execute_sql(cursor, query)
        return result, column_names

    def get_paper(self, corpusid: int) -> Optional[PaperDetails]:
        result, column_names = self.select_data(f"CORPUSID={corpusid}")
        if result and (len(result[0])):
            paper = dict(zip(column_names, result[0]))["PAPER"]
            return PaperDetails(**json.loads(paper))
        return None

    def get_some_papers(self, corpusids: Iterable) -> list[PaperDetails]:
        corpusids_str = ",".join(map(str, corpusids))
        result, column_names = self.select_data(f"CORPUSID in ({corpusids_str})")
        if result:
            retval = []
            for r in result:
                paper = dict(zip(column_names, r))["PAPER"]
                retval.append(PaperDetails(**json.loads(paper)))
            return retval
        return []


class CitationsCache:
    """A Semantic Scholar Citations local cache of CorpusId's.

    Consists of pickle files of :class:`dict` entries with keys as :code:`citedPaper`
    and values of :code:`citingPaper`

    The pickle files are stored such that :code:`corpusId` of a :code:`citingPaper`
    is smaller than :code:`temp_{suffix}` where :code:`suffix` is an integer

    Args:
        root_dir: Root directory where cache resides


    """
    def __init__(self, root_dir: Pathlike):
        self._root_dir = Path(root_dir)
        files = glob.glob(str(self._root_dir.joinpath("*.pkl")))
        if str(self._root_dir.joinpath("citations.pkl")) in files:
            files.remove(str(self._root_dir.joinpath("citations.pkl")))
        files.sort()
        _files: dict[int, str] = {int(Path(f).name.
                                      replace("temp_", "").
                                      replace(".pkl", "")): f
                                  for f in files}
        self.files = _files
        self._cache: CitationData = {}

    @property
    def cache(self) -> CitationData:
        """Cache to avoid reading files multiple times

        It's dictionary of type dict[corpusId, set(corpusId)]

        """
        return self._cache

    def maybe_get_data_from_file(self, ID: int) -> Optional[CitationData]:
        """Get the file corresponding to a corpusId

        Args:
            ID: corpusId of a paper


        """
        for i, f in enumerate(self.files):
            if ID < f:
                print(f"Looking in file {f}")
                with timer:
                    with open(self.files[f], "rb") as fp:
                        data = cast(CitationData, pickle.load(fp))
                print(f"Loaded file {self.files[f]} in {timer.time} seconds")
                return data
        return None

    def get_citations(self, ID: int) -> Optional[set]:
        """Get all the citing papers for a corpusId

        Args:
            ID: corpusId of a paper


        """
        print(f"Searching for {ID}")
        if ID in self.cache:
            print(f"Have data for {ID} in cache")
            return self.cache[ID]
        else:
            data = self.maybe_get_data_from_file(ID)
            if data and ID in data:
                self.cache[ID] = data[ID].copy()
                return data[ID]
            else:
                print(f"Could not find citation data for {ID}")
                return None
