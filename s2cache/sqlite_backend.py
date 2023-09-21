from typing import Optional, Any
from pathlib import Path
import logging
import dataclasses
import json
import sqlite3

from common_pyutil.monitor import Timer
from common_pyutil.functional import flatten

from .models import (Pathlike, Metadata, Citation, PaperDetails,
                     AuthorDetails, PaperData, Citations, IdKeys, NameToIds)
from .util import dumps_json


_timer = Timer()


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
            if cursor.description:
                column_names = [description[0] for description in cursor.description]
            else:
                column_names = None
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

    def insert_data(self, database_name: str, table_name: str, data: dict):
        conn = sqlite3.connect(self.database_name(database_name))
        cursor = conn.cursor()
        keys = ",".join(k.upper() for k in data.keys())
        vals = ','.join(['?'] * len(data))
        query = f"INSERT INTO {table_name} ({keys}) VALUES ({vals});"
        result, column_names = self.execute_sql(conn, cursor, query, [*data.values()])
        return result

    def update_data(self, database_name: str, table_name: str, data: dict, pk_name: str, pk):
        conn = sqlite3.connect(self.database_name(database_name))
        cursor = conn.cursor()
        keys = ", ".join(f"{k.upper()}=?" for k, v in data.items() if k.upper() != pk_name.upper())
        vals = [v for k, v in data.items() if k.upper() != pk_name.upper()]
        query = f"UPDATE {table_name} set {keys} where {pk_name.upper()} = ?;"
        params = tuple([*vals, pk])
        result, column_names = self.execute_sql(conn, cursor, query, params)
        return result

    def insert_or_ignore_data(self, database_name: str, table_name: str, data):
        conn = sqlite3.connect(self.database_name(database_name))
        cursor = conn.cursor()
        keys = ",".join(k.upper() for k in data.keys())
        vals = ','.join(['?'] * len(data))
        query = f"INSERT OR IGNORE INTO {table_name} ({keys}) VALUES ({vals});"
        result, column_names = self.execute_sql(conn, cursor, query, [*data.values()])
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

    def delete_rows(self, database_name, table_name, condition):
        conn = sqlite3.connect(self.database_name(database_name))
        cursor = conn.cursor()
        query = f"DELETE FROM {table_name} WHERE {condition}"
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
        self._metadata_keys = [*NameToIds.keys()]
        self._metadata_keys.remove("SS")
        self._metadata_keys.remove("corpusId")
        self._metadata_keys.insert(0, "CORPUSID")
        self._paper_keys = self._define_some_keys(PaperDetails)
        self._paper_keys_db = {k.upper(): (k, "primitive") for k in self._paper_keys["primitive"]}
        self._paper_keys_db.update({k.upper(): (k, "json") for k in self._paper_keys["json"]})
        self._author_keys = self._define_some_keys(AuthorDetails)
        self._author_keys_db = {k.upper(): (k, "primitive") for k in self._author_keys["primitive"]}
        self._author_keys_db.update({k.upper(): (k, "json") for k in self._author_keys["json"]})
        self._citation_pks: set[tuple[str, str]] = set()
        self._refs_keys: list[str] = ["citingPaper", "citedPaper", "contexts", "intents"]
        paper_ids = self.select_column("papers", "paperid")
        self._paper_pks: set[str] = set(x[0] for x in paper_ids if x is not None)
        citation_data = self.select_data("citations")[0]
        self._citation_pks = set((x[0], x[1]) for x in citation_data)
        corpus_data = self.select_data("corpus")[0]
        self._corpus_ids = set([x[1] for x in corpus_data])

    # convinience functinos
    def create_table(self, table_name, *args, **kwargs):
        db_name, table_name = self._dbs[table_name]
        return super().create_table(db_name, table_name, *args, **kwargs)

    def select_data(self, table_name, *args, **kwargs):
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
            data_point["paperId"] not in self._paper_pks and\
            data_point["externalIds"] and\
            data_point["externalIds"]["CorpusId"]

    def _update_citations_maybe_update_paper_data(self, paper_data):
        if paper_data:
            formatted_data = []
            for data_point in paper_data.values():
                if self._update_citations_validate_paper_data_point(data_point):
                    self._paper_pks.add(data_point["paperId"])
                    formatted_data.append(self._paper_to_sql(data_point))
            if formatted_data:
                self.insert_many("papers", formatted_data)

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
            if citingPaper and citedPaper:
                self._update_citations_subroutine(references_data, citingPaper,
                                                  citedPaper, contexts, intents)
            cited_paper_id = ref["citedPaper"]["paperId"]
            if cited_paper_id not in paper_data and cited_paper_id not in self._paper_pks:
                paper_data[ref["citedPaper"]["paperId"]] = ref["citedPaper"]
        if references_data:
            self.insert_or_ignore_many("citations", [*references_data.values()])
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
            citing_paper_id = ref["citingPaper"]["paperId"]
            if citing_paper_id not in paper_data and citing_paper_id not in self._paper_pks:
                paper_data[ref["citingPaper"]["paperId"]] = ref["citingPaper"]
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

    def load_metadata(self):
        data, column_names = self.select_data("metadata")
        metadata = {}
        pid_ind = self._metadata_keys.index("PAPERID")
        other_keys = self._metadata_keys.copy()
        other_keys.remove("PAPERID")
        for d in data:
            pid = d[pid_ind]
            # FIXME: This filters out Null keys but that should be a constraint baked
            #        into sqlite
            if pid:
                d = [x or "" for x in d]
                d.pop(pid_ind)
                metadata[pid] = dict(zip(other_keys, d))
        return metadata

    def load_duplicates_metadata(self):
        data, column_names = self.select_data("duplicates")
        return dict(data)

    def update_duplicates_metadata(self, paper_id: str, duplicate: str):
        data = {"paperid": paper_id, "duplicate": duplicate}
        self.insert_data("duplicates", data)

    def update_metadata(self, paper_id: str, data: dict):
        self.insert_data("metadata", data)

    def dump_paper_data(self, ID: str, data: PaperData | PaperDetails, force: bool = False):
        if isinstance(data, PaperData):
            paper_details = dataclasses.asdict(data.details)
        else:
            paper_details = dataclasses.asdict(data)
        formatted_data = self._paper_to_sql(paper_details)
        if ID is None:
            raise ValueError("ID should not be None")
        if ID not in self._paper_pks:
            self._paper_pks.add(ID)
            self.insert_data("papers", formatted_data)
        elif force:
            self.update_data("papers", formatted_data, "paperId", ID)
        if isinstance(data, PaperData):
            self._update_paper_references(ID, data.references)
            self._update_paper_citations(ID, data.citations)

    def update_paper_data(self, ID: str, data: dict[str, Any]):
        """Update only certain fields for paper with ID

        Args:
            ID: Paper ID
            data: paper data as a dictionary

        """
        if ID not in self._paper_pks:
            raise AttributeError("Cannot update data if row doesn't exist already")
        formatted_data = self._paper_to_sql(data)
        self.update_data("papers", formatted_data, "paperId", ID)

    def get_paper_data(self, ID: str, quiet: bool = False) -> Optional[dict]:
        try:
            data, column_names = self.select_data("papers", f"PAPERID = \"{ID}\"")
            if data:
                return self._sql_to_paper(data[0], column_names)
        except json.JSONDecodeError:
            self.logger.debug("Error decoding file. Corrupt data")
        return None

    def select_some_papers(self, criteria: str) -> list[dict]:
        paper_data, column_names = self.select_data("papers", criteria)
        result = []
        for data_point in paper_data:
            result.append(self._sql_to_paper(data_point, column_names))
        return result

    def delete_metadata_with_id(self, ID: str):
        self.delete_rows("metadata", f"PAPERID = '{ID}'")

    def delete_paper_with_id(self, ID: str):
        self.delete_rows("papers", f"PAPERID = '{ID}'")
        self._paper_pks.remove(ID)
