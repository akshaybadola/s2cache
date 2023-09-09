# S2 Cache documentation!

S2 Cache is an **unofficial** Python Semantic Scholar <https://www.semanticscholar.org/product/api>
client and library with a local cache. The cache helps to speed up the frequent reads of Paper metadata
and helps keep the requests to the API to a minimum.

Its intended use is for applications which use Semantic Scholar data for
generating bibliography management, visualizing citation graphs etc.
It can fetch citations and references from Semantic Scholar API
<https://www.semanticscholar.org/product/api> and store in a local cache
to avoid redundant requests to the service as the citation data fetches
can be network intensive. It stores data in a consistent manner which
can be formatted as required by user facing applications.


## Features

- Async requests
- Local `JSON` files or `sqlite` based storage
- Can fetch citations \> 10000 (Semantic Scholar API limit) when the
  full parsed citation graph is on disk.
  + Also contains a small library to parse the full `citations` data
    and to split it for easy indexing.
- Filter the data based on certain predicates like
  -   Year
  -   Author
  -   Title
  -   Venue
  -   CitationCount
- Local cache of dumped Semantic Scholar Citation Data, for fetching
  citations with papers \> 10000.

## Installation

`pip install s2cache`

Or if you'd like to tinker with the source code:

Clone from <https://github.com/akshaybadola/s2cache> and install with `pip install -e .`

## Usage

```{code-block} python
from s2cache import Semanticscholar

s2 = SemanticScholar(cache_dir=cache_dir)

# Use s2 in your application
```

## Quickstart

```{toctree}
:maxdepth: 2

quickstart.md

```

## S2Cache

```{toctree}
:maxdepth: 2

s2cache.md

```


## API Reference

```{toctree}
:maxdepth: 3

API <api/index>
```
