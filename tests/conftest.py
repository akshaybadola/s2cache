import shutil
import pytest

import sys
sys.path.append(".")

shutil.copy("tests/cache_data/metadata.jsonl.bak", "tests/cache_data/metadata.jsonl")

from fixtures import *
