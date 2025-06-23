from pathlib import Path

from common_pyutil.functional import lens

from .sqlite_backend import SQLiteBackend
from .semantic_scholar import SemanticScholar




def ensure_corpus_ids(s2: SemanticScholar, metadata: dict):
    keys = [*metadata.keys()]
    need_ids = []
    result: list[dict] = []
    duplicates = s2._known_duplicates
    for k in keys:
        cid = None
        if k in metadata and metadata[k]["CORPUSID"]:
            cid = metadata[k]["CORPUSID"]
        elif k in duplicates:
            k = duplicates[k]
            if k in metadata and metadata[k]["CORPUSID"]:
                cid = metadata[k]["CORPUSID"]
        if not cid:
            temp = s2.get_paper_data(k)
            cid = lens(temp, "details", "corpusId")
        if cid:
            result.append({"paperid": k, "corpusId": cid})
        else:
            need_ids.append(k)
    batch_size = s2._batch_size
    j = 0
    ids = need_ids[batch_size*j:batch_size*(j+1)]
    while ids:
        result.extend(s2._paper_batch(ids, ["corpusId"]))
        j += 1
        ids = need_ids[batch_size*j:batch_size*(j+1)]
    return need_ids, result


def dump_all_paper_data_from_json_to_sqlite(s2: SemanticScholar, sql: SQLiteBackend,
                                            papers_dir: Path):
    metadata = s2._metadata
    for ID in metadata:
        ID = s2._known_duplicates.get(ID, ID)
        _ = s2.paper_data(ID)
    paper_ids = [*s2._in_memory.keys()]
    batch_size = s2._batch_size
    j = 0
    result: list[dict] = []
    ids = paper_ids[batch_size*j:batch_size*(j+1)]
    details_fields = [*s2.details_fields, "references", "citations"]
    while ids:
        result.extend(s2._paper_batch(ids, details_fields))
        j += 1
        ids = paper_ids[batch_size*j:batch_size*(j+1)]
    sql._dump_all_paper_data([PaperDetails(**x) for x in result if x])
