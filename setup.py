#!/usr/bin/env python
import os
from setuptools import setup, find_packages

install_requires = [
    'articlemetaapi==1.6.15',
    'citedbyapi==1.3.10',
    'requests==2.11.1',
    'lxml==3.7.2',
    'picles.plumber==0.10',
    'solrapi'
]

tests_require = []

setup(
    name="UpdateSearch",
    version='1.2.3',
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
        "git+https://github.com/picleslivre/solrapi@1.0.0#egg=solrapi"
    ],
    test_suite='tests',
    tests_require=tests_require,
    install_requires=install_requires,
    entry_points="""
    [console_scripts]
    update_search=updatesearch.metadata:main
    """
)
