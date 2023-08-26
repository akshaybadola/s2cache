import yaml

from .models import Config, Pathlike


def load_config(config: Config, config_file: Pathlike):
    """Load a given :code:`config_file` from disk and update a :code:`config`
    object in place.

    Args:
        config_file: The config file to load. The config file is in json

    """
    with open(config_file) as f:
        _config = yaml.load(f, Loader=yaml.SafeLoader)
    for k in config:
        if k in _config:
            config[k] = _config[k]


def default_config() -> Config:
    """Generate a default config in case config file is not on disk.

    """
    _config = {"cache_dir": None,
               "api_key": None,
               "search": {"limit": 10,
                          "fields": ['authors', 'abstract', 'title',
                                     'venue', 'paperId', 'year',
                                     'url', 'citationCount',
                                     'influentialCitationCount',
                                     'externalIds']},
               # NOTE: narrowing to category isn't supported on the current API
               # "default_category": ""},
               "details": {"limit": 100,
                           "fields": ['authors', 'abstract', 'title',
                                      'venue', 'paperId', 'year',
                                      'url', 'citationCount',
                                      'influentialCitationCount',
                                      'externalIds']},
               "citations": {"limit": 100,
                             "fields": ['authors', 'abstract', 'title',
                                        'venue', 'paperId', 'year',
                                        'contexts',
                                        'url', 'citationCount',
                                        'influentialCitationCount',
                                        'externalIds']},
               "references": {"limit": 100,
                              "fields": ['authors', 'abstract', 'title',
                                         'venue', 'paperId', 'year',
                                         'contexts',
                                         'url', 'citationCount',
                                         'influentialCitationCount',
                                         'externalIds']},
               "author": {"limit": 100,
                          "fields": ["authorId", "name"]},
               "author_papers": {"limit": 100,
                                 "fields": ['authors', 'abstract', 'title',
                                            'venue', 'paperId', 'year',
                                            'url', 'citationCount',
                                            'influentialCitationCount',
                                            'externalIds']}}
    return Config(**_config)    # type: ignore
