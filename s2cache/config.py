from pathlib import Path
import yaml

from .models import Config, Pathlike


def load_config(config: Config, config_or_file: dict | Pathlike):
    """Load a given :code:`config_file` from disk and update a :code:`config`
    object in place.

    Args:
        config: The config object to populate
        config_file: The config file to load. The config file is in json

    """
    if isinstance(config_or_file, dict):
        _config = config_or_file
    else:
        with open(config_or_file) as f:
            _config = yaml.load(f, Loader=yaml.SafeLoader)
    for k in config:
        if k in _config:
            v = _config[k]
            if isinstance(v, dict):
                for key, val in v.items():
                    config[k][key] = val
            else:
                config[k] = v


def default_config() -> Config:
    """Generate a default config in case config file is not on disk.
    Or if some fields are missing from config.

    This will generate the following config:

    .. code-block:: yaml

        api_key: null
        cache_dir: $HOME/.config/s2cache
        client_timeout: 10
        corpus_cache_dir: null
        data:
            author:
              limit: 100
            author_papers:
              limit: 100
            citations:
              limit: 100
            details:
              limit: 100
            references:
              limit: 100
            search:
              limit: 10

    .. admonition:: Note

        Any arguments given to :class:`s2cache.semantic_scholar.SemanticScholar` will override those in
        default config.

    """
    _config = {"cache_dir": str(Path.home().joinpath(".config", "s2cache")),
               "api_key": None,
               "citations_cache_dir": None,
               "client_timeout": 10,
               "cache_backend": "sqlite",
               "data": {
                   "search": {"limit": 10},
                   "details": {"limit": 100},
                   "citations": {"limit": 100},
                   "references": {"limit": 100},
                   "author": {"limit": 100},
                   "author_papers": {"limit": 100}}}
    return Config(**_config)    # type: ignore
