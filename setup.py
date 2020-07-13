#!/usr/bin/env python
from setuptools import setup, find_packages

install_requires = [
    'requests>=2.18.1',
    'lxml==4.5.0',
    'raven==6.1.0',
    'articlemetaapi==1.23.0',
    'accessstatsapi==1.2.0',
    'citedbyapi==1.8.0',
    'picles.plumber==0.10',
    'Sickle==0.6.5',
    'langcodes==1.4.1',
    'solrapi>=1.2.0',
    'certifi',
    'mongomock',
    'pymongo'
]

tests_require = []

setup(
    name="UpdateSearch",
    version='1.16.0',
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
    dependency_links=[
        "git+https://github.com/picleslivre/solrapi@1.0.0#egg=solrapi-1.0.0"
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
    merge_search=mergesearch.merge_solr:main
    gen_dedup_keys=mergesearch.generate_dedup_keys:main
    """
)
