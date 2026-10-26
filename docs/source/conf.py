import os
import sys
from pathlib import Path

# https://www.sphinx-doc.org/en/master/usage/configuration.html

# add source root to PYTHONPATH, for sphinx-argparse
root_path = Path(__file__).parents[2]
sys.path.insert(0, str(root_path.resolve()))

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'DryPipe'
copyright = '2022, Maxime Lévesque'
author = 'Maxime Lévesque'
release = '0.5.1'

root_doc = "index"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "myst_parser",
    "sphinxcontrib.mermaid",
    "sphinxarg.ext"
]

templates_path = ['_templates']
exclude_patterns = [p.name for p in Path(__file__).parent.iterdir() if p.name not in {"ng", "index.rst"}]

source_suffix = {
    '.rst': 'restructuredtext',
    '.txt': 'markdown',
    '.md': 'markdown',
}


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'furo'
#html_theme = 'sphinx_rtd_theme'

html_static_path = ['_static']

___html_theme_options = {
    'analytics_id': 'G-XXXXXXXXXX',
    'analytics_anonymize_ip': False,
    'logo_only': False,
    'display_version': True,
    'prev_next_buttons_location': 'bottom',
    'style_external_links': False,
    'vcs_pageview_mode': '',
    #'style_nav_header_background': 'white',
    # Toc options
    'collapse_navigation': False,
    'sticky_navigation': False,
    'navigation_depth': 4,
    'includehidden': True,
    'titles_only': False
}

def setup(app):
    from bs4 import BeautifulSoup
    from docutils import nodes
    from furo.navigation import get_navigation_tree

    # sphinx nests sections by order, a ### right under a # would otherwise show as a 2nd level entry
    def anchors_of_level_3_and_deeper_markdown_headings(doctree):
        if doctree is None or not doctree["source"].endswith(".md"):
            return set()
        lines = Path(doctree["source"]).read_text().splitlines()

        def markdown_level(section):
            heading = lines[section.line - 1]
            return len(heading) - len(heading.lstrip("#"))

        return {
            f"#{id}"
            for section in doctree.findall(nodes.section) if markdown_level(section) > 2
            for id in section["ids"]
        }

    def without_entries(toctree_html, anchors):
        soup = BeautifulSoup(toctree_html, "html.parser")
        for link in soup.select("li.toctree-l2 > a"):
            if link["href"] in anchors:
                link.parent.decompose()
        for empty_list in soup.select("ul:not(:has(li))"):
            empty_list.decompose()
        return str(soup)

    def show_current_page_sections_in_sidebar(app, pagename, templatename, context, doctree):
        if "toctree" in context:
            toctree_html = context["toctree"](collapse=True, titles_only=False, maxdepth=2, includehidden=True)
            toctree_html = without_entries(toctree_html, anchors_of_level_3_and_deeper_markdown_headings(doctree))
            context["furo_navigation_tree"] = get_navigation_tree(toctree_html)

    # runs after furo's own handler, which computes the default tree
    app.connect("html-page-context", show_current_page_sections_in_sidebar, priority=600)
