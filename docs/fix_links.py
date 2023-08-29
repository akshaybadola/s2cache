import re
import glob
from sphinx.application import Sphinx


def fix_links(*args):
    files = [f for f in glob.glob("_build/html/**", recursive=True) if f.endswith(".html")]
    for fname in files:
        with open(fname) as f:
            content = f.read()
        content = re.sub(r'(html)?#.+?"', lambda x: x and x.group().replace(".", "_"), content)
        with open(fname, "w") as f:
            f.write(content)


def setup(app: Sphinx):
    app.connect('build-finished', fix_links)
