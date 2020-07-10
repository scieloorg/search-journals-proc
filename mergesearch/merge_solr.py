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

    def dump_data(self, data, filename):
        """
        Persiste dados no disco, em formato JSON

        :param data: Dados a serem persistidos
        :param filename: Nome do arquivo JSON
        """
        str_time = datetime.utcnow().isoformat(sep='_', timespec='milliseconds')
        filepath = '-'.join([self.cit_hash_base, filename, str_time]) + '.json'

        with open(os.path.join('merges', filepath), 'w') as f:
            f.write(data)

    def persist(self, data, data_name):
        """
        Persiste data no disco e no Solr, se indicado.

        :param data: Dados a serem persistidos
        :param data_name: Nome do conjunto de dados a ser persistido
        """
        self.dump_data(str(data), data_name)

        if self.persist_on_solr:
            self.solr.update(str(data).encode('utf-8'), headers={'content-type': 'application/json'})

