#!/usr/bin/env python
from setuptools import setup, find_packages

install_requires = []

tests_require = []

setup(
    name="UpdateSearch",
    version='1.17.0',
    description="Process article to Solr",
    author="SciELO",
    author_email="scielo-dev@googlegroups.com",
    license="BSD",
    url="https://github.com/scieloorg/search-journals-proc",
    keywords='search scielo',
    maintainer_email='atta.jamil@gmail.com',
    packages=find_packages(),
    classifiers=[
        "Topic :: System",
        "Topic :: Utilities",
        "Programming Language :: Python",
        "Operating System :: POSIX :: Linux",
    ],
    test_suite='tests',
    tests_require=tests_require,
    install_requires=install_requires,
    entry_points="""
    [console_scripts]
    update_search=updatesearch.metadata:main
    update_search_preprint=updatepreprint.updatepreprint:main
    update_search_accesses=updatesearch.accesses:main
    update_search_citations=updatesearch.citations:main
    """
)
