from typing import Optional
from pathlib import Path
import asyncio
import dataclasses
from dataclasses import dataclass
import json
import requests

from starlette.requests import Request
from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Route, Mount
import uvicorn


from s2cache.sqlite_backend import SQLiteBackend
from s2cache.util import dumps_json


@dataclass
class Error:
    msg: str


class Service:
    def __init__(self, sql, host, port):
        self.sql = sql
        self.host = host
        self.port = port
        self._paper_fields = ['paperId',
                              'title',
                              'authors',
                              'abstract',
                              'venue',
                              'year',
                              'url',
                              'corpusId',
                              'referenceCount',
                              'citationCount',
                              'influentialCitationCount',
                              'journal',
                              'publicationVenue',
                              'publicationTypes',
                              'publicationDate',
                              'isOpenAccess',
                              'openAccessPdf',
                              'fieldsOfStudy',
                              's2FieldsOfStudy',
                              'citationStyles',
                              'tldr',
                              'externalIds',
                              'citations',
                              'references']
        self._citation_fields = ['paperId',
                                 'title',
                                 'authors',
                                 'abstract',
                                 'venue',
                                 'year',
                                 'url',
                                 'corpusId',
                                 'referenceCount',
                                 'citationCount',
                                 'influentialCitationCount',
                                 'journal',
                                 'publicationVenue',
                                 'publicationTypes',
                                 'publicationDate',
                                 'isOpenAccess',
                                 'openAccessPdf',
                                 'fieldsOfStudy',
                                 's2FieldsOfStudy',
                                 'citationStyles',
                                 'externalIds',
                                 'contexts',
                                 'intents']
        self.all_papers = self.sql._load_all_paper_data()
        self.duplicates = self.sql.load_duplicates_metadata()
        self.corpus = self.sql._load_corpus()
        self.corpus = {v: k for k, v in self.corpus.items()}
        routes = [
            Route("/graph/v1/paper/{ID}", self.paper, methods=["GET"]),
            Route("/graph/v1/paper/{ID}/citations", self.citations, methods=["GET"]),
            Route("/graph/v1/paper/{ID}/references", self.references, methods=["GET"]),
        ]
        self.app = Starlette(debug=True,
                             routes=routes)
        self.config = uvicorn.Config(self.app, host=self.host, port=self.port)
        self.server = uvicorn.Server(config=self.config)
        self._corpus_dups = {49536547: 64935422}
        self._err_corpus_ids = {18403175}

    def id_subr(self, ID):
        if ":" in ID:
            prefix, ID = ID.split(":")
        else:
            prefix = None
        if prefix:
            try:
                ID = self._corpus_dups.get(int(ID), ID)
            except Exception:
                pass
            condition = f"{prefix.upper()} = {ID}" if prefix.upper() == "CORPUSID"\
                else f"{prefix.upper()} = '{ID}'"
            try:
                if int(ID) in self._err_corpus_ids:
                    return JSONResponse({"error": "Not found"})
            except Exception:
                pass
            data, column_names = self.sql.select_data("metadata", condition)
            cid = dict(zip(column_names, data[0]))["CORPUSID"]
            ID = self.corpus[cid]
        return self.duplicates.get(ID, ID)

    async def paper(self, request: Request) -> JSONResponse:
        ID = request.path_params.get("ID", "")
        if ID == "search":
            query = request.query_params.get("query", "")
            if query:
                resp = requests.get(f"https://api.semanticscholar.org/graph/v1/paper/search?query={query}")
                return JSONResponse(json.loads(resp.content))
            else:
                return JSONResponse({"error": "No query given for search"})
        ID = self.id_subr(ID)
        if isinstance(ID, JSONResponse):
            return ID
        fields = request.query_params.get("fields", "")
        if not ID:
            return JSONResponse(dataclasses.asdict(
                Error(msg="Invalid ID")))
        if not fields:
            fields = ["paperId", "title"]
        else:
            fields = fields.split(",")
        invalid_fields = [f for f in fields if f not in self._paper_fields]
        if invalid_fields:
            return JSONResponse(dataclasses.asdict(
                Error(msg=f"Invalid fields {','.join(invalid_fields)}")))
        paper_data = self.sql.get_paper_data(ID)
        return JSONResponse({k: v for k, v in paper_data.items() if k in fields})

    def _citation_subr(self, request: Request) -> dict | tuple[str | list[str]]:
        ID = request.path_params.get("ID", "")
        ID = self.id_subr(ID)
        fields = request.query_params.get("fields", "")
        if not ID:
            return dataclasses.asdict(Error(msg="Invalid ID"))
        if not fields:
            fields = ["paperId", "title"]
        else:
            fields = fields.split(",")
        invalid_fields = [f for f in fields if f not in self._citation_fields]
        if invalid_fields:
            return dataclasses.asdict(Error(msg=f"Invalid fields {','.join(invalid_fields)}"))
        return ID, fields

    async def references(self, request: Request) -> JSONResponse:
        maybe_error = self._citation_subr(request)
        if isinstance(maybe_error, dict):
            return JSONResponse(maybe_error)
        else:
            ID, fields = maybe_error
        offset = int(request.query_params.get("offset", "0"))
        limit = int(request.query_params.get("limit", "100"))
        references = self.sql.get_references_and_citations(ID)["references"]  # type: ignore
        data = {"data": [], "offset": 0, "next": None}
        for ref in references[offset:offset+limit]:
            paper_id = ref.pop("citedPaper")
            paper_id = self.duplicates.get(paper_id, paper_id)
            ref["citedPaper"] = self.sql.get_paper_data(paper_id)
            ref.pop("citingPaper")
            data["data"].append(ref)
        if len(references) > offset+limit:
            data["next"] = limit
        return JSONResponse(data)

    async def citations(self, request: Request) -> JSONResponse:
        maybe_error = self._citation_subr(request)
        if isinstance(maybe_error, dict):
            return JSONResponse(maybe_error)
        else:
            ID, fields = maybe_error
        offset = int(request.query_params.get("offset", "0"))
        limit = int(request.query_params.get("limit", "100"))
        citations = self.sql.get_references_and_citations(ID)["citations"]  # type: ignore
        data = {"data": [], "offset": 0, "next": None}
        for ref in citations[offset:offset+limit]:
            paper_id = ref.pop("citingPaper")
            paper_id = self.duplicates.get(paper_id, paper_id)
            ref["citingPaper"] = self.sql.get_paper_data(paper_id)
            ref.pop("citedPaper")
            data["data"].append(ref)
        if len(citations) > offset+limit:
            data["next"] = limit
        return JSONResponse(data)
