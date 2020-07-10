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

    def merge_citation(self, primary_citation, others):
        """
        Mescla dados de uma citação principal com dados de outras citações.

        :param primary_citation: Citação principal
        :param others: Outras citações similares à principal
        :return: Identificadores Solr a serem removidos
        """

        ids_for_removing = set()

        for cit in others[1:]:
            raw_cit = cit.copy()

            # Mescla informação de documentos citantes
            primary_citation['document_fk'].extend(raw_cit['document_fk'])
            primary_citation['document_fk'] = list(set(primary_citation['document_fk']))

            # Mescla informação de coleções citantes
            primary_citation['in'].extend(raw_cit['in'])
            primary_citation['in'] = list(set(primary_citation['in']))

            # Mescla informação de autores citantes
            if 'document_fk_au' in raw_cit:
                if 'document_fk_au' not in primary_citation:
                    primary_citation['document_fk_au'] = []
                primary_citation['document_fk_au'].extend(raw_cit['document_fk_au'])
                primary_citation['document_fk_au'] = list(set(primary_citation['document_fk_au']))

            # Mescla informação de periódicos citantes
            if 'document_fk_ta' in raw_cit:
                if 'document_fk_ta' not in primary_citation:
                    primary_citation['document_fk_ta'] = []
                primary_citation['document_fk_ta'].extend(raw_cit['document_fk_ta'])
                primary_citation['document_fk_ta'] = list(set(primary_citation['document_fk_ta']))

            # Obtém ids das citações que devem ser removidas dos documentos citantes e do Solr
            ids_for_removing.add(raw_cit['id'])

        # Calcula número de citações recebidas
        primary_citation['total_received'] = str(len(primary_citation['document_fk']))

        return ids_for_removing

    def request_docs(self, ids):
        """
        Obtém documentos Solr.

        :param ids: Lista de ids de documentos Solr a serem obtidos
        :return: Dicionário contendo documentos Solr
        """

        response = {}

        # O limite de cláusulas booleandas no Solr é 1024 (na configuração corrente)
        # Por isso, é preciso fazer mais de uma query, caso o número de ids seja > 1023
        if len(ids) < 1000:
            query = 'id:(%s)' % ' OR '.join(ids)
            response.update(eval(self.solr.select({'q': query, 'rows': SOLR_ROWS_LIMIT})))
        else:
            response = {'response': {'docs': []}}

            for start_pos in range(0, len(ids), 1000):
                last_pos = start_pos + 1000
                if last_pos > len(ids):
                    last_pos = len(ids)

                i_query = 'id:(%s)' % ' OR '.join(ids[start_pos: last_pos])
                i_response = eval(self.solr.select({'q': i_query, 'rows': SOLR_ROWS_LIMIT}))

                if len(i_response.get('response', {}).get('docs')) > 0:
                    response['response']['docs'].extend(i_response['response']['docs'])

        return response

    def mount_removing_commands(self, cits_for_removing):
        """
        Cria comandos para remoção de documentos Solr.

        :param cits_for_removing: Identificadores de documentos a serem removidos
        :return: Códigos Solr para remoção de documentos
        """
        rm_commands = []
        ids = list(cits_for_removing)

        for start_pos in range(0, len(ids), 1000):
            last_pos = start_pos + 1000
            if last_pos > len(ids):
                last_pos = len(ids)

            command = {'delete': {'query': 'id:(' + ' OR '.join([ids[k] for k in range(start_pos, last_pos)]) + ')'}}
            rm_commands.append(command)
        return rm_commands

    def persist(self, data, data_name):
        """
        Persiste data no disco e no Solr, se indicado.

        :param data: Dados a serem persistidos
        :param data_name: Nome do conjunto de dados a ser persistido
        """
        self.dump_data(str(data), data_name)

        if self.persist_on_solr:
            self.solr.update(str(data).encode('utf-8'), headers={'content-type': 'application/json'})

    def merge_citations(self, deduplicated_citations):
        """
        Mescla documentos Solr. Persiste no próprio Solr ou em disco (para posterior persistência).

        :param deduplicated_citations: Códigos hashes contendo ids de citações a serem mescladas
        """
        logging.info('Merging cited references (Solr documents)...')

        cits_for_merging = []
        docs_for_updating = []
        cits_for_removing = set()

        total_citations = len(deduplicated_citations)
        for counter, dc in enumerate(deduplicated_citations):
            logging.info('%d of %d' % (counter, total_citations))

            cit_full_ids = dc['cit_full_ids']
            citing_docs = dc['citing_docs']

            response_citations = self.request_docs(cit_full_ids)

            # Mescla citações
            if len(response_citations.get('response', {}).get('docs')) > 1:
                citations = response_citations['response']['docs']

                merged_citation = {}
                merged_citation.update(citations[0])

                if self.cit_hash_base == 'articles_start_page':
                    merged_citation['start_page'] = dc['cit_start_page']
                elif self.cit_hash_base == 'articles_volume':
                    merged_citation['volume'] = dc['cit_volume']
                elif self.cit_hash_base == 'articles_issue':
                    merged_citation['issue'] = dc['cit_issue']

                ids_for_removing = self.merge_citation(merged_citation, citations[1:])
                cits_for_merging.append(merged_citation)

                response_documents = self.request_docs(citing_docs)

                # Atualiza documentos citantes
                for doc in response_documents.get('response', {}).get('docs', []):
                    updated_doc = {}
                    updated_doc['entity'] = 'document'
                    updated_doc['id'] = doc['id']
                    updated_doc['citation_fk'] = {'remove': list(ids_for_removing), 'add': merged_citation['id']}

                    docs_for_updating.append(updated_doc)

                # Monta instruções de remoção de citações mescladas
                cits_for_removing = cits_for_removing.union(ids_for_removing)

            # Persiste a cada 50000 comandos de mesclagem
            if len(cits_for_merging) >= 50000:
                self.persist(cits_for_merging, '0_cits_for_merging')
                cits_for_merging = []

                self.persist(docs_for_updating, '1_docs_for_updating')
                docs_for_updating = []

                rm_commands = self.mount_removing_commands(cits_for_removing)
                for counter, rm in enumerate(rm_commands):
                    self.persist(rm, '2_cits_for_removing' + '_' + str(counter))
                cits_for_removing = set()

        if len(cits_for_merging) > 0 or len(docs_for_updating) or len(cits_for_removing) > 0:
            self.persist(cits_for_merging, '0_cits_for_merging')
            self.persist(docs_for_updating, '1_docs_for_updating')

            rm_commands = self.mount_removing_commands(cits_for_removing)
            for counter, rm in enumerate(rm_commands):
                self.persist(rm, '2_cits_for_removing' + '_' + str(counter))

        if self.persist_on_solr:
            self.solr.commit()


def main():
    usage = """\
        Mescla documentos Solr do tipo citation (entity=citation).
        """

    parser = argparse.ArgumentParser(textwrap.dedent(usage))

    parser.add_argument(
        '--mongo_uri',
        default=None,
        required=True,
        dest='mongo_uri',
        help='String de conexão a base Mongo que contem códigos identificadores de citações de-duplicadas. '
             'Usar o formato: mongodb://[username]:[password]@[host1]:[port1]/[database].[collection].'
    )

    parser.add_argument(
        '-b', '--cit_hash_base',
        required=True,
        choices=['article_issue', 'article_start_page', 'article_volume', 'book', 'chapter'],
        help='Nome da base de chaves de de-duplicação de citações'
    )

    parser.add_argument(
        '-f', '--from_date',
        help='Obtém apenas as chaves cuja data de atualização é a partir da data especificada (use o formato YYYY-MM-DD)'
    )

    parser.add_argument(
        '--logging_level',
        '-l',
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help='Logggin level'
    )

    parser.add_argument(
        '-s', '--persist_on_solr',
        action='store_true',
        default=False,
        help='Persiste resultados diretamente no Solr'
    )

    args = parser.parse_args()

    logging.basicConfig(level=args.logging_level)

    if args.from_date:
        mongo_filter = {'update_date': {'$gte': args.from_date}}
    else:
        mongo_filter = {}

    mongo_collection_name = uri_parser.parse_uri(args.mongo_uri).get('collection')
    if not mongo_collection_name:
        mongo_collection_name = args.cit_hash_base
    try:
        mongo = MongoClient(args.mongo_uri).get_database().get_collection(mongo_collection_name)
    except ConnectionError as ce:
        logging.error(ce)
        exit(1)

    os.makedirs('merges', exist_ok=True)

    solr = SolrAPI.Solr(SOLR_URL, timeout=100)

    merger = MergeSolr(cit_hash_base=args.cit_hash_base,
                       solr=solr,
                       mongo=mongo,
                       persist_on_solr=args.persist_on_solr)

    deduplicated_citations = merger.get_ids_for_merging(mongo_filter)
    if deduplicated_citations:
        merger.merge_citations(deduplicated_citations)
