# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# http://www.sphinx-doc.org/en/master/config

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import os
import sys
import pydata_sphinx_theme
from sphinx.ext.napoleon.docstring import GoogleDocstring
from functools import partial
sys.path.append("..")

sys.path.insert(0, os.path.abspath('.'))
import fix_links

# -- Project information -----------------------------------------------------

project = "s2cache"
copyright = "2023 Akshay Badola"
author = 'Akshay Badola'


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.

# import sphinx

extensions = [
    "myst_parser",
    "sphinx.ext.napoleon",
    # "sphinx.ext.autodoc",
    # "autodoc2",
    "sphinx.ext.intersphinx",
    "sphinx.ext.coverage",
    "sphinx.ext.autosectionlabel",
    "sphinx.ext.viewcode",
    # "sphinxcontrib.fulltoc",
    "autoapi.extension",
    "fix_links",
]

autodoc_typehints = 'description'
autoapi_dirs = ["../s2cache"]
autoapi_keep_files = True
# NOTE: this doesn't work
# autoapi_options = [
#     "members",
#     # "inherited-members",
#     # "undoc-members",
#     # "private-members",
#     # "show-inheritance",
#     # "show-module-summary",
#     # "special-members",
#     # "imported-members",
# ]



# autodoc2_packages = ["../s2cache"]
# autodoc2_output_dir = "api"
# # autodoc2_render_plugin = "myst"
# autodoc2_hidden_objects = ["private", "dunder"]
# autodoc2_skip_module_regexes = [".*flycheck_.*"]
# autodoc_mock_imports = ["models"]

source_suffix = [".rst", ".md"]


# first, we define new methods for any new sections and add them to the class
def parse_codelike_section(self, header, section):
    header = [f':{header}:', '']
    block = ['.. code-block:: python', '']
    lines = self._consume_usage_section()
    lines = self._indent(lines, 3)
    return header + block + lines + ['']


def parse_fieldlike_section(self, header, section):
    return self._format_fields(header, self._consume_fields())


# we now patch the parse method to guarantee that the the above methods are
# assigned to the _section dict
def patched_parse(self):
    self._sections['schemas'] = partial(parse_codelike_section, self, "Schemas")
    self._sections['examples'] = partial(parse_codelike_section, self, "Examples")
    self._sections['example'] = partial(parse_codelike_section, self, "Example")
    self._sections['map'] = partial(parse_fieldlike_section, self, "Map")
    self._sections['tags'] = partial(parse_codelike_section, self, "Tags")
    self._sections['request'] = partial(parse_codelike_section, self, "Request")
    self._sections['requests'] = partial(parse_codelike_section, self, "Requests")
    self._sections['response'] = partial(parse_fieldlike_section, self, "Response")
    self._sections['responses'] = partial(parse_fieldlike_section, self, "Responses")
    self._sections['keys'] = partial(parse_fieldlike_section, self, "Keys")
    self._sections['attributes'] = partial(parse_fieldlike_section, self, "Attributes")
    self._sections['class attributes'] = partial(parse_fieldlike_section, self, "Class Attributes")
    self._unpatched_parse()


GoogleDocstring._unpatched_parse = GoogleDocstring._parse
GoogleDocstring._parse = patched_parse


# Napoleon settings
napoleon_google_docstring = True
napoleon_numpy_docstring = False
napoleon_include_init_with_doc = False
napoleon_include_private_with_doc = False
napoleon_include_special_with_doc = True
napoleon_use_admonition_for_examples = False
napoleon_use_admonition_for_notes = False
napoleon_use_admonition_for_references = False
napoleon_use_ivar = False
napoleon_use_param = True
napoleon_use_keyword = False
napoleon_use_rtype = True
napoleon_type_aliases = None


# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']

autosectionlabel_prefix_document = True

# intersphinx_mapping
intersphinx_mapping = {
    'python': ('https://docs.python.org/3', None),
    # 'torch': ('https://pytorch.org/docs/master', None),
    # 'flask': ('https://click.palletsprojects.com/', None)
}

# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
# html_theme = 'divio_docs_theme'
html_theme = 'pydata_sphinx_theme'

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ["_static"]
html_css_files = ["css/custom.css"]


html_theme_options = {
  "show_nav_level": 4
}


def skip_submodules(app, what, name, obj, skip, options):
    if "._" in name:
        skip = True
    return skip


def setup(sphinx):
    sphinx.connect("autoapi-skip-member", skip_submodules)

# NOTE: This is the default
# html_sidebars = ['localtoc.html', 'relations.html', 'sourcelink.html', 'searchbox.html']
# html_sidebars = {'**': ['globaltoc.html', 'relations.html', 'sourcelink.html', 'searchbox.html']}
