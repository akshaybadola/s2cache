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
        cache_dir: null
        client_timeout: 10
        corpus_cache_dir: null
        author:
          fields:
          - authorId
          - name
          limit: 100
        author_papers:
          fields:
          - paperId
          - authors
          - abstract
          - title
          - venue
          - publicationVenue
          - year
          - url
          - citationCount
          - influentialCitationCount
          - externalIds
          limit: 100
        citations:
          fields:
          - paperId
          - authors
          - abstract
          - title
          - venue
          - publicationVenue
          - year
          - url
          - citationCount
          - influentialCitationCount
          - externalIds
          - contexts
          limit: 100
        details:
          fields:
          - paperId
          - authors
          - abstract
          - title
          - venue
          - publicationVenue
          - year
          - url
          - citationCount
          - influentialCitationCount
          - externalIds
          limit: 100
        references:
          fields:
          - paperId
          - authors
          - abstract
          - title
          - venue
          - publicationVenue
          - year
          - url
          - citationCount
          - influentialCitationCount
          - externalIds
          - contexts
          limit: 100
        search:
          fields:
          - paperId
          - authors
          - abstract
          - title
          - venue
          - publicationVenue
          - year
          - url
          - citationCount
          - influentialCitationCount
          - externalIds
          limit: 10

    .. admonition:: Note

        Any arguments given to :class:`s2cache.semantic_scholar.SemanticScholar` will override those in
        default config.

    """
    _config = {"cache_dir": None,
               "api_key": None,
               "corpus_cache_dir": None,
               "client_timeout": 10,
               "api": {
                   "search": {"limit": 10,
                              "fields": ['authors', 'abstract', 'title',
                                         'venue', 'publicationVenue', 'paperId', 'year',
                                         'url', 'citationCount',
                                         'influentialCitationCount',
                                         'externalIds']},
                   # NOTE: narrowing to category isn't supported on the current API
                   # "default_category": ""},
                   "details": {"limit": 100,
                               "fields": ['authors', 'abstract', 'title',
                                          'venue', 'publicationVenue', 'paperId', 'year',
                                          'url', 'citationCount',
                                          'influentialCitationCount',
                                          'externalIds']},
                   "citations": {"limit": 100,
                                 "fields": ['authors', 'abstract', 'title',
                                            'venue', 'publicationVenue', 'paperId', 'year',
                                            'contexts', 'url', 'citationCount',
                                            'influentialCitationCount',
                                            'externalIds']},
                   "references": {"limit": 100,
                                  "fields": ['authors', 'abstract', 'title',
                                             'venue', 'publicationVenue', 'paperId', 'year',
                                             'contexts', 'url', 'citationCount',
                                             'influentialCitationCount',
                                             'externalIds']},
                   "author": {"limit": 100,
                              "fields": ["authorId", "name"]},
                   "author_papers": {"limit": 100,
                                     "fields": ['authors', 'abstract', 'title',
                                                'venue', 'publicationVenue', 'paperId', 'year',
                                                'url', 'citationCount',
                                                'influentialCitationCount',
                                                'externalIds']}}}
    return Config(**_config)    # type: ignore
