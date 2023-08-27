import json
import dataclasses


def json_serialize(obj):
    if dataclasses.is_dataclass(obj):
        return dataclasses.asdict(obj)
    else:
        return obj


def json_dumps(obj):
    return json.dumps(obj, default=json_serialize)


def json_dump(obj, file):
    json.dump(obj, file, default=json_serialize)


