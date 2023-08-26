from typing import Optional, cast
import sys
import glob
import gzip
import json
from collections import defaultdict
import os
import pickle
from pathlib import Path

from common_pyutil.monitor import Timer
import requests

from .models import CitationData, Pathlike


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


def parse_and_dump_citation_data(root_dir: Path):
    """Parse the downloaded gzipped citation data, consolidate, sort and
    dump again.

    Args:
        root_dir: Directory where all the gzipped data files are kept

    This may require A LOT of RAM as the entire citations data is consolidated
    in an adjacency list. This large pickle is split later.

    """
    citations = defaultdict(set)
    filenames = glob.glob(str(root_dir.joinpath("*gz")))
    for f_num, filename in enumerate(filenames):
        with gzip.open(filename, "rt") as s2_file:
            for i, line in enumerate(s2_file):
                data = json.loads(line)
                if data["citedcorpusid"] and data["citingcorpusid"]:
                    a, b = int(data["citedcorpusid"]), int(data["citingcorpusid"])
                    citations[a].add(b)
                if not (i+1) % 1000000:
                    print(f"{i+1} done for file {filename}")
        print(f"Done file {f_num+1} out of {len(filenames)}")
    out_file = root_dir.joinpath("citations.pkl")
    print(f"Writing file {out_file}")
    with open(out_file, "wb") as f:
        pickle.dump(citations, f)


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


def split_and_dump_citations_subr(input_dir: Path, output_dir: Path,
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


def split_and_dump_citations(root_dir: Path):
    """Read the citations.pkl file and split them based on corpus_id

    Args:
        root_dir: Root directory where citations reside


    """
    with timer:
        with open(root_dir.joinpath("citations.pkl"), "rb") as f:
            citations = pickle.load(f)
    print(f"Loaded citations in {timer.time} seconds")
    keys = [*citations.keys()]
    max_key = max(keys)
    split_and_dump_citations_subr(root_dir, root_dir, citations, max_key)


def convert_keys_from_numpy(cache):
    """Convert cache keys from :class:`numpy.int64` to :class:`int`

    Used once when keys were taken from numpy

    Args:
        cache: :class:`RefsCache`

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


class CorpusCache:
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
            if data:
                self.cache[ID] = data[ID].copy()
                if data and ID in data:
                    return data[ID]
                else:
                    print(f"Could not find reference data for {ID}")
                    return None
            return None


if __name__ == '__main__':
    root_dir = Path(sys.argv[1])
    if not root_dir.exists():
        raise ValueError(f"No such directory {root_dir}")
    parse_and_dump_citation_data(root_dir)
    split_and_dump_citations(root_dir)
