from typing import Optional
import os
from pathlib import Path
import json
import logging

from common_pyutil.monitor import Timer

from .models import Pathlike, Metadata, PaperData, IdKeys
from .util import dump_json, dumps_json, id_to_name


_timer = Timer()


class JSONLBackend:
    def __init__(self, root_dir: Pathlike, logger_name: str):
        self._root_dir = Path(root_dir)
        self._files: list[str] = [*filter(
            lambda x: not x.endswith("~") and "metadata" not in x and x != "cache"
            and "duplicates" not in x and "." not in x,
            os.listdir(self._root_dir))]
        self._logger = logging.getLogger(logger_name)

    @property
    def logger(self):
        return self._logger

    @property
    def duplicates_file(self):
        return self._root_dir.joinpath("duplicates.csv")

    @property
    def metadata_file(self):
        return self._root_dir.joinpath("metadata.jsonl")

    def load_metadata(self):
        return self.load_jsonl_metadata()

    def rebuild_metadata(self):
        return self.rebuild_jsonl_metadata()

    def update_metadata(self, paper_id: str, data: dict):
        self.update_jsonl_metadata_on_disk(paper_id, data)

    def update_duplicates_metadata(self, paper_id: str, duplicate: str):
        self.update_duplicates_metadata_on_disk(paper_id, duplicate)

    def load_jsonl_metadata(self):
        """Load JSON lines metadata from disk

        This will be default from version :code:`0.2.0`

        """
        metadata: Metadata = {}
        if self.metadata_file.exists():
            with open(self.metadata_file) as f:
                lines = f.read().split("\n")
            for line in lines:
                if line:
                    metadata.update(json.loads(line))
            self.logger.debug(f"Loaded metadata from {self.metadata_file}")
        else:
            self.logger.warning("Metadata file not found. Intialzing empty metadata")
        return metadata

    def load_duplicates_metadata(self):
        duplicates = {}
        if self.duplicates_file.exists():
            with open(self.duplicates_file) as f:
                lines = f.read().split("\n")
            for line in lines:
                if line:
                    a, b = line.split(":")
                    duplicates[a] = b
            self.logger.debug(f"Loaded duplicates from {self.duplicates_file}")
        else:
            self.logger.warning("Duplicates file not found. Intialzing empty metadata")
        return duplicates

    def dump_duplicates_metadata_on_disk(self, duplicates):
        """Dump JSON lines metadata to disk.

        This will be default from version :code:`0.2.0`

        """
        with open(self.duplicates_file, "w") as f:
            for k, v in duplicates.items():
                f.write(f"{k}:{v}\n")
        self.logger.debug("Dumped duplicates")

    def update_duplicates_metadata_on_disk(self, paper_id: str, duplicate: str):
        with open(self.duplicates_file, "a") as f:
            f.write(f"\n{paper_id}:{duplicate}")
        self.logger.debug(f"Updated duplicate in JSONL backend for {paper_id}")

    def dump_jsonl_metadata(self, metadata):
        """Dump JSON lines metadata to disk.

        This will be default from version :code:`0.2.0`

        """
        with open(self.metadata_file, "w") as f:
            for k, v in metadata.items():
                f.write(dumps_json({k: v}))
                f.write("\n")
        self.logger.debug("Dumped metadata")

    def update_jsonl_metadata_on_disk(self, paper_id: str, data: dict):
        """Update the existing data on disk with a single :code:`paper_id`

        Args:
            paper_id: The paper id to update


        """
        with open(self.metadata_file, "a") as f:
            f.write("\n")
            f.write(dumps_json({paper_id: data}))
        self.logger.debug(f"Updated metadata for {paper_id}")

    def dump_paper_data(self, ID: str, data: PaperData, force: bool = False):
        """Dump the paper :code:`data` for paperID :code:`ID` to the JSON file

        Args:
            ID: The paper ID
            data: The paper data

        The data is stored as JSON. This function handles it as raw :class:`dict`

        """
        fpath = self._root_dir.joinpath(str(ID))
        if len(data.citations.data) > data.details.citationCount:
            data.details.citationCount = len(data.citations.data)
        with _timer:
            with open(fpath, "w") as f:
                dump_json(data, f)
        self.logger.debug(f"Wrote file {fpath} in {_timer.time} seconds")

    def get_paper_data(self, ID: str, quiet: bool = False) -> Optional[dict]:
        """Fetch S2 details from disk with SSID=ID

        Args:
            ID: SSID of the paper

        """
        data_file = self._root_dir.joinpath(ID)
        if data_file.exists():
            if not quiet:
                self.logger.debug(f"Data for {ID} is on disk")
            try:
                with open(data_file, "rb") as f:
                    data = f.read()
                return json.loads(data)  # type: ignore
            except json.JSONDecodeError:
                self.logger.debug("Error decoding file. Corrupt file")
                return None
        else:
            return None

    def rebuild_jsonl_metadata(self):
        """Rebuild the JSON lines metadata file in case it's corrupted
        """
        id_names_map = {"arxivid": "ARXIV",
                        "arxiv": "ARXIV",
                        "doi": "DOI",
                        "url": "URL",
                        "pubmedid": "PUBMED",
                        "pubmed": "PUBMED",
                        "aclid": "ACL",
                        "dblp": "DBLP",
                        "acl": "ACL"}
        with open(self.metadata_file, "w") as wf:
            for fname in self._files:
                with open(self._root_dir.joinpath(fname)) as f:
                    paper_data = json.load(f)
                details = paper_data["details"] if "details" in paper_data else paper_data
                if "externalIds" in details:
                    ext_ids = {id_to_name(k): v
                               for k, v in details["externalIds"].items()}
                else:
                    ext_ids = {id_names_map[k.lower()]: details[k] for k in details
                               if k.lower() in id_names_map}
                    ext_ids = {k: ext_ids[k] if k in ext_ids else ""
                               for k in IdKeys}
                wf.write(dumps_json({fname: ext_ids}))
                wf.write("\n")
