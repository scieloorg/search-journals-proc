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

    def get_ids_for_merging(self, mongo_filter: dict):
        """
        Coleta identificadores da base de de-duplicação.

        :param mongo_filter: Filtro Mongo para selecionar parte dos identificadores
        :return: Identificadores hash das citações a serem mescladas
        """
        logging.info('Getting cited references identifiers... [filter=%s]' % mongo_filter)

        ids_for_merging = []

        for j in self.mongo.find(mongo_filter):

            item = {
                '_id': j['_id'],
                'cit_full_ids': j['cit_full_ids'],
                'citing_docs': j['citing_docs']
            }

            # Caso base de de-duplicação seja issue, mantem na citação principal o melhor valor de issue
            if self.cit_hash_base == 'article_issue':
                item.update({'cit_issue': j['cit_keys']['cleaned_issue']})

            # Caso base de de-duplicação seja start_page, mantem na citação principal o melhor valor de start_page
            if self.cit_hash_base == 'article_start_page':
                item.update({'cit_start_page': j['cit_keys']['cleaned_start_page']})

            # Caso base de de-duplicação seja volume, mantem na citação principal o melhor valor de volume
            if self.cit_hash_base == 'article_volume':
                item.update({'cit_volume': j['cit_keys']['cleaned_volume']})

            ids_for_merging.append(item)

        logging.info('There are %d cited references identifiers to be merged.' % len(ids_for_merging))

        return ids_for_merging
    def persist(self, data, data_name):
        """
        Persiste data no disco e no Solr, se indicado.

        :param data: Dados a serem persistidos
        :param data_name: Nome do conjunto de dados a ser persistido
        """
        self.dump_data(str(data), data_name)

        if self.persist_on_solr:
            self.solr.update(str(data).encode('utf-8'), headers={'content-type': 'application/json'})

