import json
import dataclasses

from common_pyutil.monitor import Timer

from .models import Error


_timer = Timer()


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
    return "corpusId" if ID.lower() == "corpusid" else ID.upper()


def field_names(datacls) -> list[str]:
    return [x.name for x in dataclasses.fields(datacls)]



def _maybe_fix_citation_data(citation_data, citation_type, key):
    if isinstance(citation_data.data[0], dict):
        data = []
        for x in citation_data.data:
            try:
                if "contexts" not in x:
                    x["contexts"] = []
                if "intents" not in x:
                    x["intents"] = []
                data.append(citation_type(**x))
            except Exception:
                pass
        citation_data.data = data
    citation_data.data = list(filter(lambda x: getattr(x, key), citation_data.data))
