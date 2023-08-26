import shutil
import pytest

import sys
sys.path.append(".")

shutil.copy("tests/cache_data/metadata.bak", "tests/cache_data/metadata")

from fixtures import *
