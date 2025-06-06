from typing import Optional, Any
from pathlib import Path
import logging
import dataclasses
import json
from collections import defaultdict

from common_pyutil.sqlite import SQLite
from common_pyutil.monitor import Timer
from common_pyutil.functional import flatten

from .models import (Pathlike, Metadata, Citation, Reference, PaperDetails,
                     AuthorDetails, PaperData, Citations, References, IdKeys, NameToIds)
from .util import dumps_json


_timer = Timer()


class SQLiteBackend(SQLite):
    def __init__(self, root_dir: Pathlike, logger_name: str):
        super().__init__(root_dir, logger_name)
        self._dbs = {"papers": ("papers.db", "papers"),
                     "authors": ("authors.db", "authors"),
                     "citations": ("citations.db", "citations"),
                     "metadata": ("metadata.db", "metadata"),
                     "duplicates": ("metadata.db", "duplicates"),
                     "corpus": ("metadata.db", "corpus")}
        self._metadata_keys = [*NameToIds.keys()]
        self._metadata_keys.remove("SS")
        self._metadata_keys.remove("corpusId")
        self._metadata_keys.insert(0, "CORPUSID")
        self._paper_keys = self._define_some_keys(PaperDetails)
        self._paper_keys_db: dict[str, tuple[str, str]] = {k.upper(): (k, "primitive")
                                                           for k in self._paper_keys["primitive"]}
        self._paper_keys_db.update({k.upper(): (k, "json") for k in self._paper_keys["json"]})
        self._author_keys = self._define_some_keys(AuthorDetails)
        self._author_keys_db = {k.upper(): (k, "primitive") for k in self._author_keys["primitive"]}
        self._author_keys_db.update({k.upper(): (k, "json") for k in self._author_keys["json"]})
        self._citation_pks: set[tuple[str, str]] = set()
        self._refs_keys: list[str] = ["citingPaper", "citedPaper", "contexts", "intents"]
        self._init_pks()

    def _init_pks(self):
        all_papers, columns = self.select_data("papers")
        if all_papers:
            self._all_papers: dict[str, dict] = {x[1]: self._sql_to_paper(x, columns)
                                                 for x in all_papers if x is not None}
        else:
            self._all_papers = {}
        del all_papers
        citation_data = self.select_data("citations")[0]
        if citation_data:
            self._citation_pks = set((x[0], x[1]) for x in citation_data)
        else:
            self._citation_pks = set()
        corpus_data = self.select_data("corpus")[0]
        if corpus_data:
            self._corpus_ids = set([x[1] for x in corpus_data])
            self._rev_corpus_dict = dict(corpus_data)
        else:
            self._corpus_ids = set()

    # convenience functinos
    def create_table(self, table_name, *args, **kwargs):
        db_name, table_name = self._dbs[table_name]
        return super().create_table(db_name, table_name, *args, **kwargs)

    def select_data(self, table_name, *args, **kwargs):
        """Select """
        db_name, table_name = self._dbs[table_name]
        return super().select_data(db_name, table_name, *args, **kwargs)

    def select_column(self, table_name, *args, **kwargs):
        db_name, table_name = self._dbs[table_name]
        return super().select_column(db_name, table_name, *args, **kwargs)

    def insert_data(self, table_name, *args, **kwargs):
        db_name, table_name = self._dbs[table_name]
        return super().insert_data(db_name, table_name, *args, **kwargs)

    def insert_many(self, table_name, *args, **kwargs):
        db_name, table_name = self._dbs[table_name]
        return super().insert_many(db_name, table_name, *args, **kwargs)

    def insert_or_ignore_data(self, table_name, *args, **kwargs):
        db_name, table_name = self._dbs[table_name]
        return super().insert_or_ignore_data(db_name, table_name, *args, **kwargs)

    def insert_or_ignore_many(self, table_name, *args, **kwargs):
        db_name, table_name = self._dbs[table_name]
        return super().insert_or_ignore_many(db_name, table_name, *args, **kwargs)

    def update_data(self, table_name, *args, **kwargs):
        db_name, table_name = self._dbs[table_name]
        return super().update_data(db_name, table_name, *args, **kwargs)

    def delete_rows(self, table_name, *args, **kwargs):
        db_name, table_name = self._dbs[table_name]
        return super().delete_rows(db_name, table_name, *args, **kwargs)

    def _define_some_keys(self, datacls):
        fields = dataclasses.fields(datacls)
        primitive_keys = [f.name for f in fields
                          if f.type in {str, int, bool, Optional[str], Optional[int]}]
        return {"primitive": primitive_keys,
                "json": [x.name for x in fields if x.name not in primitive_keys
                         and x.name.lower() != "references" and x.name.lower() != "citations"]}

    def _create_dbs(self):
        self.create_table("metadata", self._metadata_keys, "corpusId")
        self.create_table("corpus", ["paperId", "corpusId"], "paperId")
        self.create_table("citations",
                          ["citingPaper", "citedPaper", "contexts", "intents"],
                          ("citingPaper", "citedPaper"))
        self.create_table("duplicates", ["paperId", "duplicate"], "paperId")
        self.create_table("authors", flatten([*self._author_keys.values()]), "authorId")
        self.create_table("papers", flatten([*self._paper_keys.values()]), "paperId")

    def _backup_dbs(self):
        for db_name, _ in self._dbs.values():
            self.backup(db_name)

    def _load_corpus(self):
        data, column_names = self.select_data("corpus")
        return dict(data)

    def _dump_corpus(self, corpus_db, request_ids):
        dups = []
        corpus_db = corpus_db[::-1]
        request_ids = request_ids[::-1]
        for x, y in zip(corpus_db, request_ids):
            if x["paperId"] != y:
                dups.append((y, x["paperId"]))
        self.insert_many("corpus", corpus_db)
        dups_data = [dict(zip(("paperid", "duplicate"), x)) for x in dups]
        self.insert_many("duplicates", dups_data)

    def _dump_metadata(self, metadata):
        data = []
        corpus_db = self._load_corpus()
        duplicates = self.load_duplicates_metadata()
        for key, datapoint in metadata.items():
            if key in duplicates:
                key = duplicates[key]
            corpus_id = corpus_db.get(key, None)
            if corpus_id:
                data.append({k: None for k in self._metadata_keys})
                data[-1].update({k.upper(): v for k, v in datapoint.items()})
                data[-1]["PAPERID"] = key
                data[-1]["CORPUSID"] = corpus_id
        self.insert_many("metadata", data)

    def _load_all_paper_data(self) -> dict[str, dict]:
        result, column_names = self.select_data("papers")
        data = {}
        for val in result:
            paper_data = self._sql_to_paper(val, column_names)
            data[paper_data["paperId"]] = paper_data
        return data

    def _select_from_citations(self, citingPaper, citedPaper):
        if not citingPaper and not citedPaper:
            raise ValueError("Can't both be Empty")
        elif citingPaper and citedPaper:
            return self.select_data("citations",
                                    f"CITINGPAPER = '{citingPaper}' and CITEDPAPER = '{citedPaper}'")[0]
        elif citingPaper and not citedPaper:
            return self.select_data("citations", f"CITINGPAPER = '{citingPaper}'")[0]
        else:
            return self.select_data("citations", f"CITEDPAPER = '{citedPaper}'")[0]

    def _update_citations_subroutine(self, citations_data, citingPaper,
                                     citedPaper, contexts, intents):
        # FIXME, this should merge with db data
        if citingPaper and citedPaper:
            if (citingPaper, citedPaper) in citations_data and\
               (citingPaper, citedPaper) not in self._citation_pks:
                citations_data[(citingPaper, citedPaper)]["contexts"] = \
                    dumps_json(json.loads(citations_data[(citingPaper, citedPaper)]["contexts"]) +
                               contexts)
                citations_data[(citingPaper, citedPaper)]["intents"] = \
                    dumps_json(json.loads(citations_data[(citingPaper, citedPaper)]["intents"]) +
                               contexts)
            else:
                citations_data[(citingPaper, citedPaper)] = {"citingPaper": citingPaper,
                                                             "citedPaper": citedPaper,
                                                             "contexts": dumps_json(contexts),
                                                             "intents": dumps_json(intents)}
            self._citation_pks.add((citingPaper, citedPaper))

    def _update_citations_validate_paper_data_point(self, data_point):
        return data_point and data_point["paperId"] and\
            data_point["paperId"] not in self._all_papers and\
            data_point["externalIds"] and\
            data_point["externalIds"]["CorpusId"]

    def _update_citations_maybe_update_paper_data(self, paper_data):
        if paper_data:
            formatted_data = []
            for data_point in paper_data.values():
                if self._update_citations_validate_paper_data_point(data_point):
                    self._all_papers[data_point["paperId"]] = data_point
                    formatted_data.append(self._paper_to_sql(data_point))
            if formatted_data:
                self.insert_many("papers", formatted_data)

    def _update_paper_references(self, paper_id: str, references: References):
        references_data: dict[str, str] = {}
        paper_data = {}
        for ref in references.data:
            if isinstance(ref, (Citation, Reference)):
                ref_dict = dataclasses.asdict(ref)
            else:
                ref_dict = ref
            contexts = ref_dict.get("contexts", [])
            intents = ref_dict.get("intents", [])
            citingPaper = paper_id
            citedPaper = ref_dict.get("citedPaper", {}).get("paperId", "")
            if citingPaper and citedPaper:
                self._update_citations_subroutine(references_data, citingPaper,
                                                  citedPaper, contexts, intents)
                cited_paper_id = ref_dict["citedPaper"]["paperId"]
                if cited_paper_id not in paper_data and cited_paper_id not in self._all_papers:
                    paper_data[ref_dict["citedPaper"]["paperId"]] = ref_dict["citedPaper"]
            else:
                print(f"Not citingPaper {citingPaper} or not citedPaper {citedPaper}")
        if references_data:
            self.insert_or_ignore_many("citations", [*references_data.values()])
        self._update_citations_maybe_update_paper_data(paper_data)

    def _update_paper_citations(self, paper_id: str, citations: Citations):
        citations_data: dict[str, str] = {}
        paper_data = {}
        for ref in citations.data:
            if isinstance(ref, Citation):
                ref_dict = dataclasses.asdict(ref)
            else:
                ref_dict = ref
            contexts = ref_dict.get("contexts", [])
            intents = ref_dict.get("intents", [])
            citedPaper = paper_id
            citingPaper = ref_dict.get("citingPaper", {}).get("paperId", "")
            if citingPaper and citedPaper:
                self._update_citations_subroutine(citations_data, citingPaper,
                                                  citedPaper, contexts, intents)
                citing_paper_id = ref_dict["citingPaper"]["paperId"]
                if citing_paper_id not in paper_data and citing_paper_id not in self._all_papers:
                    paper_data[ref_dict["citingPaper"]["paperId"]] = ref_dict["citingPaper"]
        if citations_data:
            self.insert_or_ignore_many("citations", [*citations_data.values()])
        self._update_citations_maybe_update_paper_data(paper_data)

    def _paper_to_sql(self, paper_data: dict) -> dict[str, str]:
        _data = {}
        for k, v in paper_data.items():
            if k in self._paper_keys["primitive"]:
                _data[k] = v
            elif k in self._paper_keys["json"]:
                _data[k] = dumps_json(v)
        _data["CorpusId"] = paper_data["externalIds"]["CorpusId"]
        return _data

    def _sql_to_paper(self, sql_data: dict, column_names: list[str]) -> dict:
        return {self._paper_keys_db[k][0]: v
                if not v or self._paper_keys_db[k][1] == "primitive" else json.loads(v)
                for k, v in zip(column_names, sql_data)}

    def rebuild_metadata(self):
        metadata: dict[str, dict] = {}
        corpus_data = []
        with _timer:
            all_papers = self._load_all_paper_data()
        self.logger.info(f"Loaded all papers in {_timer.time} seconds")
        with _timer:
            for paper in all_papers.values():
                cid = paper["corpusId"]
                if cid in metadata:
                    metadata[cid].update({k.upper(): v for k, v in paper["externalIds"].items()})
                else:
                    metadata[cid] = {k: None for k in self._metadata_keys}
                    metadata[cid].update({k.upper(): v for k, v in paper["externalIds"].items()})
                corpus_data.append({"corpusId": cid, "paperId": paper["paperId"]})
                self._corpus_ids.add(cid)
        self.logger.info(f"Gathered metadata in {_timer.time} seconds")
        with _timer:
            self.insert_many("metadata", [*metadata.values()])
            self.insert_many("corpus", corpus_data)
        self.logger.info(f"Updated metadata in {_timer.time} seconds")

    def get_references_and_citations(self, paper_id: str) -> dict[str, list]:
        """Get references (citedPapers) and citations (citingPapers) for a :code:`paper_id`

        Args:
            paper_id: unique hash id of the paper


        """
        references = self.select_data("citations", f"CITINGPAPER = \"{paper_id}\"")[0]
        references = [dict(zip(self._refs_keys, x)) for x in references]
        citations = self.select_data("citations", f"CITEDPAPER = \"{paper_id}\"")[0]
        citations = [dict(zip(self._refs_keys, x)) for x in citations]
        return {"references": references, "citations": citations}

    def load_metadata(self) -> tuple[dict, dict, dict, dict]:
        known_duplicates: dict[str, str] = self.load_duplicates_metadata()
        data, column_names = self.select_data("metadata")
        corpus = self._load_corpus()
        inferred_duplicates: dict[int, list[str]] = defaultdict(list)
        for pid, cid in corpus.items():
            inferred_duplicates[cid].append(pid)
        keys = list(inferred_duplicates.keys())
        for k in keys:
            if len(inferred_duplicates[k]) == 1:
                inferred_duplicates.pop(k)
        metadata = {}
        for d in data:
            d = dict(zip(column_names, d))
            try:
                cid = int(d["CORPUSID"])
            except Exception:
                pass
            metadata[d["CORPUSID"]] = d
        return metadata, known_duplicates, inferred_duplicates, corpus

    def load_duplicates_metadata(self):
        data, column_names = self.select_data("duplicates")
        return dict(data)

    def update_duplicates_metadata(self, paper_id: str, duplicate: str):
        data = {"paperid": paper_id, "duplicate": duplicate}
        self.insert_data("duplicates", data)

    def update_metadata(self, paper_id, external_ids):
        self.insert_or_update_metadata(paper_id, external_ids)

    def insert_or_update_metadata(self, paper_id: str, data: dict):
        cid = data.get("corpusId", None) or data.get("CorpusId", None) or data.get("CORPUSID", None)
        cid = cid and int(cid)
        if cid:
            if cid in self._corpus_ids:
                existing_data, column_names = self.select_data("metadata", f"corpusid = {cid}")
                existing_data = dict(zip(column_names, existing_data[0]))
                needs_update = any(data.get(k, None) and not existing_data[k] for k in existing_data)
                if needs_update:
                    existing_data.update(data)
                    self.update_data("metadata", existing_data, "corpusid", cid)
                    self.logger.debug("Nothing to update")
            else:
                self.insert_data("metadata", data)
        else:
            self.logger.error("Corpus ID not given")

    def _dump_paper_metadata(self, paper_id: str, corpus_id: Optional[int], external_ids: dict):
        corpus_id = corpus_id or external_ids["CorpusId"]
        if corpus_id in self._corpus_ids:
            result, columns = self.select_data("corpus", f"corpusid = {corpus_id}")
            if result and paper_id != result[0][0]:
                self.insert_data("corpus", {"corpusId": corpus_id, "paperId": paper_id})
        else:
            self.insert_data("corpus", {"corpusId": corpus_id, "paperId": paper_id})
        self.insert_or_update_metadata("metadata", {k.upper(): v for k, v in external_ids.items()})

    def dump_paper_data(self, ID: str, data: PaperData | PaperDetails, force: bool = False):
        """Dump paper data into the sqlite dbs

        Args:
            ID: PaperId of the paper
            data: The data as a dataclass, either :class:`models.PaperData`
                  or :class:`models.PaperDetails`
            force: dummy argument for compatibility

        """
        if (hasattr(data, "details") and hasattr(data, "references"))\
           or isinstance(data, PaperData):
            paper_details = dataclasses.asdict(data.details)
        else:
            paper_details = dataclasses.asdict(data)
        formatted_data = self._paper_to_sql(paper_details)
        if ID is None:
            raise ValueError("ID should not be None")
        if ID not in self._all_papers:
            self._all_papers[ID] = paper_details
            self.insert_data("papers", formatted_data)
        else:
            self.update_data("papers", formatted_data, "paperId", ID)
        if (hasattr(data, "details") and hasattr(data, "references"))\
           or isinstance(data, PaperData):
            self._update_paper_references(ID, data.references)  # type: ignore
            self._update_paper_citations(ID, data.citations)    # type: ignore
        self._dump_paper_metadata(paper_details["paperId"],
                                  paper_details["corpusId"],
                                  paper_details["externalIds"])

    def update_paper_data(self, ID: str, data: dict[str, Any]):
        """Update only certain fields for paper with ID

        Args:
            ID: Paper ID
            data: paper data as a dictionary

        """
        if ID not in self._all_papers:
            raise AttributeError("Cannot update data if row doesn't exist already")
        formatted_data = self._paper_to_sql(data)
        self.update_data("papers", formatted_data, "paperId", ID)

    def get_paper_data(self, ID: str, quiet: bool = False) -> Optional[dict]:
        try:
            return self._all_papers.get(ID)
        except json.JSONDecodeError:
            self.logger.debug("Error decoding file. Corrupt data")
        return None

    def select_some_papers(self, criteria: str) -> list[dict]:
        paper_data, column_names = self.select_data("papers", criteria)
        result = []
        for data_point in paper_data:
            result.append(self._sql_to_paper(data_point, column_names))
        return result

    def delete_metadata_with_id(self, corpusid: int):
        self.delete_rows("metadata", f"CORPUSID = {corpusid}")

    def delete_paper_with_id(self, ID: str):
        corpusid = self.select_data("corpus", f"PAPERID = '{ID}'")
        self.delete_rows("papers", f"PAPERID = '{ID}'")
        self.delete_rows("corpus", f"PAPERID = '{ID}'")
        self.delete_rows("metadata", f"CORPUSID = {corpusid}")
        self._all_papers.pop(ID)
        corpusid = self._rev_corpus_dict[ID]
        self._corpus_ids.remove(corpusid)
