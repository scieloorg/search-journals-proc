import argparse
import logging
import os
import SolrAPI
import textwrap

from datetime import datetime
from pymongo import MongoClient, uri_parser

SOLR_URL = os.environ.get('SOLR_URL', 'http://localhost:8983/solr/articles')
SOLR_ROWS_LIMIT = os.environ.get('SOLR_ROWS_LIMIT', 10000)


class MergeSolr(object):

    def __init__(self,
                 cit_hash_base,
                 solr,
                 mongo,
                 persist_on_solr=False):

        self.cit_hash_base = cit_hash_base
        self.solr = solr
        self.mongo = mongo
        self.persist_on_solr = persist_on_solr

