from typing import Optional
from pathlib import Path
import logging
import dataclasses
import json
import sqlite3

from common_pyutil.functional import flatten

from .models import (Pathlike, Metadata, Citation, PaperDetails,
                     AuthorDetails, PaperData, Citations, IdKeys, IdNames)
from .util import dumps_json


class SQLite:
    def __init__(self, root_dir: Pathlike, logger_name: str):
        self._root_dir = Path(root_dir)
        self.logger = logging.getLogger(logger_name)

    def database_name(self, database_name: str) -> str:
        return str(self._root_dir.joinpath(database_name))

    def backup(self, db_name: str):
        db_file = self.database_name(db_name)
        backup_file = self.database_name(db_name) + ".bak"
        conn = sqlite3.connect(db_file)
        backup = sqlite3.connect(backup_file)
        conn.backup(backup)
        backup.close()
        conn.close()

    def execute_sql(self, conn, cursor, query, params=None):
        try:
            if params is not None:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            column_names = [description[0] for description in cursor.description]
            result = cursor.fetchall()
            conn.commit()
            conn.close()
        except Exception as e:
            self.logger.error(f"Error {e} for query {query}")
            conn.close()
            return None, None
        return result, column_names

    def describe_table(self, database_name, table_name):
        conn = sqlite3.connect(self.database_name(database_name))
        cursor = conn.cursor()
        query = f"PRAGMA table_info({table_name})"
        # query = f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table_name}';"
        result, column_names = self.execute_sql(conn, cursor, query)
        return result

    def create_table(self, database_name, table_name, columns, pk):
        conn = sqlite3.connect(self.database_name(database_name))
        cursor = conn.cursor()
        if isinstance(pk, (tuple, list)):
            pk = ", ".join(pk)
            pk = f"({pk})".upper()
            columns = ",".join([*[c.upper() for c in columns], f"primary key {pk}"])
        else:
            columns = ",".join([c.upper() + " primary key" if c == pk.upper() else c.upper()
                                for c in columns])
        query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns});"
        self.logger.debug(query)
        result, column_names = self.execute_sql(conn, cursor, query)
        return result

    def insert_many(self, database_name: str, table_name: str, data: list[dict]):
        conn = sqlite3.connect(self.database_name(database_name))
        cursor = conn.cursor()
        columns = data[0]
        keys = ",".join(k.upper() for k in columns.keys())
        vals = ','.join(['?'] * len(columns))
        query = f"INSERT INTO {table_name} ({keys}) VALUES ({vals});"
        values = [[*d.values()] for d in data]
        try:
            result = cursor.executemany(query, values).fetchall()
            conn.commit()
            conn.close()
        except Exception as e:
            self.logger.error(f"Error {e} for query {query}")
            conn.close()
            return None
        return result

    def insert_or_ignore_many(self, database_name: str, table_name: str, data: list[dict]):
        conn = sqlite3.connect(self.database_name(database_name))
        cursor = conn.cursor()
        columns = data[0]
        keys = ",".join(k.upper() for k in columns.keys())
        vals = ','.join(['?'] * len(columns))
        query = f"INSERT OR IGNORE INTO {table_name} ({keys}) VALUES ({vals});"
        values = [[*d.values()] for d in data]
        try:
            result = cursor.executemany(query, values).fetchall()
            conn.commit()
            conn.close()
        except Exception as e:
            self.logger.error(f"Error {e} for query {query}")
            conn.close()
            return None
        return result

    def insert_data(self, database_name: str, table_name: str, data):
        conn = sqlite3.connect(self.database_name(database_name))
        cursor = conn.cursor()
        keys = ",".join(k.upper() for k in data.keys())
        vals = ','.join(['?'] * len(data))
        query = f"INSERT INTO {table_name} ({keys}) VALUES ({vals});"
        result, column_names = self.execute_sql(conn, cursor, query)
        return result

    def insert_or_ignore_data(self, database_name: str, table_name: str, data):
        conn = sqlite3.connect(self.database_name(database_name))
        cursor = conn.cursor()
        keys = ",".join(k.upper() for k in data.keys())
        vals = ','.join(['?'] * len(data))
        query = f"INSERT OR IGNORE INTO {table_name} ({keys}) VALUES ({vals});"
        result, column_names = self.execute_sql(conn, cursor, query)
        return result

    def select_data(self, database_name, table_name, condition=None):
        conn = sqlite3.connect(self.database_name(database_name))
        cursor = conn.cursor()
        if condition:
            query = f"SELECT * FROM {table_name} WHERE {condition};"
        else:
            query = f"SELECT * FROM {table_name};"
        result, column_names = self.execute_sql(conn, cursor, query)
        return result, column_names

    def select_column(self, database_name, table_name, column, condition=None):
        conn = sqlite3.connect(self.database_name(database_name))
        cursor = conn.cursor()
        if condition:
            query = f"SELECT {column} FROM {table_name} WHERE {condition};"
        else:
            query = f"SELECT {column} FROM {table_name};"
        result, column_names = self.execute_sql(conn, cursor, query)
        return result


class SQLiteBackend(SQLite):
    def __init__(self, root_dir: Pathlike, logger_name: str):
        super().__init__(root_dir, logger_name)
        self._dbs = {"papers": ("papers.db", "papers"),
                     "authors": ("authors.db", "authors"),
                     "citations": ("citations.db", "citations"),
                     "metadata": ("metadata.db", "metadata"),
                     "duplicates": ("metadata.db", "duplicates"),
                     "corpus": ("metadata.db", "corpus")}
        self._metadata_keys = [*IdNames.keys()]
        self._metadata_keys.remove("SS")
        self._metadata_keys.remove("CorpusId")
        self._metadata_keys.insert(0, "PAPERID")
        self._metadata_keys.insert(0, "CORPUSID")
        self._paper_keys = self._define_some_keys(PaperDetails)
        self._paper_keys["primitive"].insert(0, "CorpusId")
        self._paper_keys_db = {k.upper(): (k, "primitive") for k in self._paper_keys["primitive"]}
        self._paper_keys_db.update({k.upper(): (k, "json") for k in self._paper_keys["json"]})
        self._author_keys = self._define_some_keys(AuthorDetails)
        self._author_keys_db = {k.upper(): (k, "primitive") for k in self._author_keys["primitive"]}
        self._author_keys_db.update({k.upper(): (k, "json") for k in self._author_keys["json"]})
        self._paper_pks: set[str] = set()
        self._citation_pks: set[tuple[str, str]] = set()

    def _define_some_keys(self, datacls):
        fields = dataclasses.fields(datacls)
        primitive_keys = [f.name for f in fields
                          if f.type in {str, int, bool, Optional[str], Optional[int]}]
        return {"primitive": primitive_keys,
                "json": [x.name for x in fields if x.name not in primitive_keys
                         and x.name.lower() != "references" and x.name.lower() != "citations"]}

    def _create_dbs(self):
        self.create_table(*self._dbs["metadata"], self._metadata_keys, "CorpusId")
        self.create_table(*self._dbs["corpus"], ["paperId", "CorpusId"], "paperId")
        self.create_table(*self._dbs["citations"],
                          ["citingPaper", "citedPaper", "contexts", "intents"],
                          ("citingPaper", "citedPaper"))
        self.create_table(*self._dbs["duplicates"], ["paperId", "duplicate"], "paperId")
        self.create_table(*self._dbs["authors"], flatten([*self._author_keys.values()]), "authorId")
        self.create_table(*self._dbs["papers"], flatten([*self._paper_keys.values()]), "paperId")

    def _backup_dbs(self):
        for db_name, _ in self._dbs.values():
            self.backup(db_name)

    def _load_corpus(self):
        return dict(self.select_data(*self._dbs["corpus"]))

    def _dump_corpus(self, corpus_db, request_ids):
        dups = []
        corpus_db = corpus_db[::-1]
        request_ids = request_ids[::-1]
        for x, y in zip(corpus_db, request_ids):
            if x["paperId"] != y:
                dups.append((y, x["paperId"]))
        self.insert_many(*self._dbs["corpus"], corpus_db)
        dups_data = [dict(zip(("paperid", "duplicate"), x)) for x in dups]
        self.insert_many(*self._dbs["duplicates"], dups_data)

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
        self.insert_many(*self._dbs["metadata"], data)

    def _dump_some_papers(self, paper_data: list[PaperDetails]):
        data = []
        for paper in paper_data:
            data.append({"paperid": paper.paperId, "data": dumps_json(paper)})
        self.insert_many(*self._dbs["papers"], data)

    def _load_all_paper_data(self):
        data = self.select_data(*self._dbs["papers"])
        return {x: json.loads(y) for x, y in data}

    def _select_from_citations(self, citingPaper, citedPaper):
        if not citingPaper and not citedPaper:
            raise ValueError("Can't both be Empty")
        elif citingPaper and citedPaper:
            return self.select_data(*self._dbs["citations"],
                                    f"CITINGPAPER = '{citingPaper}' and CITEDPAPER = '{citedPaper}'")
        elif citingPaper and not citedPaper:
            return self.select_data(*self._dbs["citations"], f"CITINGPAPER = '{citingPaper}'")
        else:
            return self.select_data(*self._dbs["citations"], f"CITEDPAPER = '{citedPaper}'")

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
            data_point["paperId"] not in self._paper_pks and\
            data_point["externalIds"] and\
            data_point["externalIds"]["CorpusId"]

    def _update_citations_maybe_update_paper_data(self, paper_data):
        if paper_data:
            formatted_data = []
            for data_point in paper_data.values():
                if self._update_citations_validate_paper_data_point(data_point):
                    self._paper_pks.add(data_point["paperId"])
                    formatted_data.append(self._format_paper_details(data_point))
            if formatted_data:
                self.insert_many(*self._dbs["papers"], formatted_data)

    def _update_paper_references(self, paper_id: str, references: Citations):
        references_data: dict[str, str] = {}
        paper_data = {}
        for ref in references.data:
            if isinstance(ref, Citation):
                ref = dataclasses.asdict(ref)  # type: ignore
            contexts = ref.get("contexts", [])  # type: ignore
            intents = ref.get("intents", [])    # type: ignore
            citingPaper = paper_id
            citedPaper = ref.get("citedPaper", {}).get("paperId", "")  # type: ignore
            self._update_citations_subroutine(references_data, citingPaper,
                                              citedPaper, contexts, intents)
            if ref["citedPaper"]["paperId"] not in paper_data:
                paper_data[ref["citedPaper"]["paperId"]] = ref["citedPaper"]
        if references_data:
            self.insert_or_ignore_many(*self._dbs["citations"], [*references_data.values()])
        self._update_citations_maybe_update_paper_data(paper_data)

    def _update_paper_citations(self, paper_id: str, citations: Citations):
        citations_data: dict[str, str] = {}
        paper_data = {}
        for ref in citations.data:
            if isinstance(ref, Citation):
                ref = dataclasses.asdict(ref)  # type: ignore
            contexts = ref.get("contexts", [])  # type: ignore
            intents = ref.get("intents", [])    # type: ignore
            citedPaper = paper_id
            citingPaper = ref.get("citingPaper", {}).get("paperId", "")  # type: ignore
            if citingPaper and citedPaper:
                self._update_citations_subroutine(citations_data, citingPaper,
                                                  citedPaper, contexts, intents)
            if ref["citingPaper"]["paperId"] not in paper_data:
                paper_data[ref["citingPaper"]["paperId"]] = ref["citingPaper"]
        if citations_data:
            self.insert_or_ignore_many(*self._dbs["citations"], [*citations_data.values()])
        self._update_citations_maybe_update_paper_data(paper_data)

    def _format_paper_details(self, paper_data: dict):
        _data = {}
        for k, v in paper_data.items():
            if k in self._paper_keys["primitive"]:
                _data[k] = v
            elif k in self._paper_keys["json"]:
                _data[k] = dumps_json(v)
        _data["CorpusId"] = paper_data["externalIds"]["CorpusId"]
        return _data

    def load_references_and_citations(self, paper_id):
        return {"references":
                self.select_data(*self._dbs["citations"], f"CITINGPAPER = \"{paper_id}\""),
                "citations":
                self.select_data(*self._dbs["citations"], f"CITEDPAPER = \"{paper_id}\"")}

    def load_metadata(self):
        data = self.select_data(*self._dbs["metadata"])
        metadata = {}
        pid_ind = self._metadata_keys.index("PAPERID")
        other_keys = self._metadata_keys.copy()
        other_keys.remove("PAPERID")
        for d in data:
            pid = d[pid_ind]
            d = [x or "" for x in d]
            d.pop(pid_ind)
            metadata[pid] = dict(zip(other_keys, d))
        return metadata

    def load_duplicates_metadata(self):
        return dict(self.select_data(*self._dbs["duplicates"]))

    def update_duplicates_metadata(self, paper_id: str, duplicate: str):
        data = {"paperid": paper_id, "duplicate": duplicate}
        self.insert_data(*self._dbs["duplicates"], data)

    def update_metadata(self, paper_id: str, data: dict):
        pass

    def dump_paper_data(self, ID: str, data: PaperData):
        paper_data = dataclasses.asdict(data.details)
        formatted_data = self._format_paper_details(paper_data)
        if data.details.paperId not in self._paper_pks:
            self._paper_pks.add(data.details.paperId)
            self.insert_data(*self._dbs["papers"], formatted_data)
        self._update_paper_references(ID, data.references)
        self._update_paper_citations(ID, data.citations)

    def get_paper_data(self, ID: str):
        try:
            data = self.select_data(*self._dbs["papers"], f"PAPERID = \"{ID}\"")
            if data:
                return json.loads(data[0][1])  # type: ignore
        except json.JSONDecodeError:
            self.logger.debug("Error decoding file. Corrupt data")
        return None

    def select_some_papers(self, criteria: str):
        paper_data, column_names = self.select_data(*self._dbs["papers"], criteria)
        result = []
        for data_point in paper_data:
            result.append({self._paper_keys_db[k][0]: v
                           if not v or self._paper_keys_db[k][1] == "primitive" else json.loads(v)
                           for k, v in zip(column_names, data_point)})
        return result
