# Quickstart

You can use the Semantic Scholar API with or without an API Key (See
[api-key](https://www.semanticscholar.org/product/api#api-key)). You can specify the
`api_key` in a YAML `config_file` or while initializing the client.


## Configuration

The only imperatively required field for creating the cache and initializing the client
is `cache_dir`. The `cache_dir` can be provided as argument or will be
read from the given `config_file` argument to SemanticScholar

The configuration format is YAML.

A default configuration is automatically generated if nothing is provided.


### An Example of Absolute Minimal Config

```{code-block} yaml

api_key: YOUR_API_KEY
cache_dir: CACHE_DIRECTORY
citations:
  limit: 100

```

## Example Usage

```{code-block} python

from s2cache import SemanticScholar

s2 = SemanticScholar(config_file=config_file)

# Or Obtain API key from environment

import os
from s2cache import SemanticScholar

api_key = os.environ.get("S2_API_KEY")
s2 = SemanticScholar(cache_dir=cache_dir, api_key=api_key)

# Or just specify everything on __init__

from s2cache import SemanticScholar

s2 = SemanticScholar(cache_dir="YOUR_CACHE_DIR", api_key="YOUR_API_KEY",
                     corpus_cache_dir="CORPUS_CACHE_DIR", logger_name="YOUR-APP-LOGGER")

```

```{admonition} Note
If an option is specified both in config file and the `SemanticScholar` parameters, the option
given to `SemanticScholar` will take precendence.
```


## Get details for a Paper


```{code-block} python

details = s2.paper_details(paper_ssid)

```

See [PaperDetails](s2cache.models.PaperDetails) for the attributes.


This will fetch:
- The paper data (paperId, authors, number of citations etc.) according to the fields
  given in config. Please consult the [Semantic Scholar Graph API](https://api.semanticscholar.org/api-docs/graph)
  for a list of fields which can be fetched.
  + If fields are not given in the given, some (sensible) fields are generated according [default_config](config.default_config).
  + Please see [Config](models.Config) for configuration details.
- First `n` (in the API call and `config` the variable is named `limit`) references of the paper as configured
- First `n` citations as configured

For the fields and options see  [PaperData](PaperData) and [PaperDetails](PaperDetails)


