import json
import dataclasses


def json_serialize(obj):
    if dataclasses.is_dataclass(obj):
        return dataclasses.asdict(obj)
    else:
        return obj


def dumps_json(obj) -> str:
    return json.dumps(obj, default=json_serialize)


def dump_json(obj, file) -> None:
    json.dump(obj, file, default=json_serialize)


def id_to_name(ID: str):
    """Change the ExternalId returned by the S2 API to the name

    Args:
        ID: External ID

    """
    return "CorpusId" if ID.lower() == "corpusid" else ID.upper()
