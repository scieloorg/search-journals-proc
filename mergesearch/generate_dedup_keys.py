import argparse
import logging
import os
import textwrap
import time

from datetime import datetime
from hashlib import sha3_224
from multiprocessing import Pool
from pymongo import MongoClient, UpdateOne, uri_parser
from mergesearch.utils.field_cleaner import get_cleaned_default, get_cleaned_publication_date, get_cleaned_first_author_name, get_cleaned_journal_title
from xylose.scielodocument import Article, Citation


DEDUPLICATED_CITATIONS_PREFIX = os.environ.get('DEDUPLICATED_CITATIONS_PREFIX', 'dedup_')

COLLECTION_STANDARDIZED = os.environ.get('COLLECTION_STANDARDIZED', 'standardized')

ARTICLE_KEYS = ['cleaned_publication_date',
                'cleaned_first_author',
                'cleaned_title',
                'cleaned_journal_title']

BOOK_KEYS = ['cleaned_publication_date',
             'cleaned_first_author',
             'cleaned_source',
             'cleaned_publisher',
             'cleaned_publisher_address']

chunk_size = 2000

citation_types = set()

mongo_uri_scielo_search = ''

mongo_uri_article_meta = ''


def get_mongo_connection(mongo_uri, collection=None):
    """
    Obtém uma conexão com o MongoDB.

    :param mongo_uri: String de conexão MongoDB
    :param collection: Nome da coleção MongoDB
    :return: Conexão com coleção MongoDB
    """
    try:
        if collection:
            return MongoClient(mongo_uri, maxPoolSize=None).get_database().get_collection(collection)
        else:
            mongo_collection_name = uri_parser.parse_uri(mongo_uri).get('collection')
            if mongo_collection_name:
                return MongoClient(mongo_uri, maxPoolSize=None).get_database().get_collection(mongo_collection_name)
            else:
                return MongoClient(mongo_uri, maxPoolSize=None).get_database()
    except ConnectionError as ce:
        logging.error(ce)
        exit(1)


def _extract_citation_fields_by_list(citation: Citation, fields):
    """
    Extrai de uma citação os campos indicados na variável fields.

    :param citation: Citação da qual serão extraídos os campos
    :param fields: Campos a serem extraídos
    :return: Dicionário composto pelos pares campo: valor do campo
    """
    data = {}

    for f in fields:
        cleaned_v = get_cleaned_default(getattr(citation, f))
        if cleaned_v:
            data['cleaned_' + f] = cleaned_v

    return data


def _extract_citation_authors(citation: Citation):
    """
    Extrai o primeiro autor de uma citação.
    Caso citação seja capitulo de livro, extrai o primeiro autor do livro e o primeiro autor do capitulo.
    Caso citação seja livro ou artigo, extrai o primeiro autor.

    :param citation: Citação da qual o primeiro autor sera extraido
    :return: Dicionário composto pelos pares cleaned_first_author: valor e cleaned_chapter_first_author: valor
    """
    data = {}

    if citation.publication_type == 'article' or not citation.chapter_title:
        cleaned_first_author = get_cleaned_first_author_name(citation.first_author)
        if cleaned_first_author:
            data['cleaned_first_author'] = cleaned_first_author
    else:
        if citation.analytic_authors:
            cleaned_chapter_first_author = get_cleaned_first_author_name(citation.analytic_authors[0])
            if cleaned_chapter_first_author:
                data['cleaned_chapter_first_author'] = cleaned_chapter_first_author

            if citation.monographic_authors:
                cleaned_first_author = get_cleaned_first_author_name(citation.monographic_authors[0])
                if cleaned_first_author:
                    data['cleaned_first_author'] = cleaned_first_author

    return data


def extract_citation_data(citation: Citation, cit_standardized_data=None):
    """
    Extrai os dados de uma citação.

    :param citation: Citação da qual os dados serao extraidos
    :param cit_standardized_data: Caso seja artigo, usa o padronizador de título de periódico
    :return: Dicionário composto pelos pares de nomes dos ampos limpos das citações e respectivos valores
    """
    data = {}

    data.update(_extract_citation_authors(citation))

    cleaned_publication_date = get_cleaned_publication_date(citation.publication_date)
    if cleaned_publication_date:
        data['cleaned_publication_date'] = cleaned_publication_date

    if citation.publication_type == 'article':
        data.update(_extract_citation_fields_by_list(citation, ['issue', 'start_page', 'volume']))

        cleaned_journal_title = ''
        if cit_standardized_data:
            cleaned_journal_title = cit_standardized_data['official-journal-title'][0].lower()
            if cleaned_journal_title:
                data['cleaned_journal_title'] = cleaned_journal_title

        if not cleaned_journal_title:
            cleaned_journal_title = get_cleaned_journal_title(citation.source)
            if cleaned_journal_title:
                data['cleaned_journal_title'] = cleaned_journal_title

        cleaned_title = get_cleaned_default(citation.title())
        if cleaned_title:
            data['cleaned_title'] = cleaned_title

    elif citation.publication_type == 'book':
        data.update(_extract_citation_fields_by_list(citation, ['source', 'publisher', 'publisher_address']))

        cleaned_chapter_title = get_cleaned_default(citation.chapter_title)
        if cleaned_chapter_title:
            data['cleaned_chapter_title'] = cleaned_chapter_title

    return data


def mount_citation_id(citation: Citation, collection_acronym):
    """
    Monta o id completo de uma citação.

    :param citation: Citação da qual o id completo sera montado
    :param collection_acronym: Acrônimo da coleção SciELO na qual a citação foi referida
    :return: ID completo da citação formada pelo PID do documento citante, numero da citação e coleção citante
    """
    cit_id = citation.data['v880'][0]['_']
    cit_full_id = '{0}-{1}'.format(cit_id, collection_acronym)
    return cit_full_id


def hash_keys(cit_data, keys):
    """
    Cria um codigo hash dos dados de uma citação, com base na lista de keys.

    :param cit_data: Dicionário de pares de nome de campo e valor de campo de citação
    :param keys: Nomes dos campos a serem considerados para formar o codigo hash
    :return: Codigo hash SHA3_256 para os dados da citação
    """
    data = []
    for k in keys:
        if k in cit_data:
            if cit_data[k]:
                data.append(k + cit_data[k])
            else:
                return
        else:
            return

    if data:
        return sha3_224(''.join(data).encode()).hexdigest()


def extract_citations_ids_keys(document: Article, standardizer):
    """
    Extrai as quadras (id de citação, pares de campos de citação, hash da citação, base) para todos as citações.
    São contemplados livros, capítulos de livros e artigos.

    :param document: Documento do qual a lista de citações será convertida para hash
    :param standardizer: Normalizador de título de periódico citado
    :return: Quadra composta por id de citação, dicionário de nomes de campos e valores, hash de citação e base
    """
    citations_ids_keys = []

    if document.citations:
        for cit in [c for c in document.citations if c.publication_type in citation_types]:
            cit_full_id = mount_citation_id(cit, document.collection_acronym)

            if cit.publication_type == 'article':
                cit_standardized_data = standardizer.find_one({'_id': cit_full_id, 'status': {'$gt': 0}})
                cit_data = extract_citation_data(cit, cit_standardized_data)

                for extra_key in ['volume', 'start_page', 'issue']:
                    keys_i = ARTICLE_KEYS + ['cleaned_' + extra_key]

                    article_hash_i = hash_keys(cit_data, keys_i)
                    if article_hash_i:
                        citations_ids_keys.append((cit_full_id,
                                                   {k: cit_data[k] for k in keys_i if k in cit_data},
                                                   article_hash_i,
                                                   'article_' + extra_key))

            else:
                cit_data = extract_citation_data(cit)

                book_hash = hash_keys(cit_data, BOOK_KEYS)
                if book_hash:
                    citations_ids_keys.append((cit_full_id,
                                               {k: cit_data[k] for k in BOOK_KEYS if k in cit_data},
                                               book_hash,
                                               'book'))

                    chapter_keys = BOOK_KEYS + ['cleaned_chapter_title', 'cleaned_chapter_first_author']

                    chapter_hash = hash_keys(cit_data, chapter_keys)
                    if chapter_hash:
                        citations_ids_keys.append((cit_full_id,
                                                   {k: cit_data[k] for k in chapter_keys if k in cit_data},
                                                   chapter_hash,
                                                   'chapter'))

    return citations_ids_keys


def convert_to_mongodoc(data):
    """
    Converte dados de citação para registro em formato Mongo.

    :param data: Dados a serem convertidos (lista de quadras no formato: id de citacao, dados de citação, hash, base)
    :return: Dados convertidos
    """
    mgdocs = {'article_issue': {}, 'article_start_page': {}, 'article_volume': {}, 'book': {}, 'chapter': {}}

    for doc_id, citations_data in [d for d in data if d]:
        for cit in citations_data:
            cit_full_id = cit[0]
            cit_keys = cit[1]
            cit_sha3_256 = cit[2]
            cit_hash_mode = cit[3]

            if cit_sha3_256 not in mgdocs[cit_hash_mode]:
                mgdocs[cit_hash_mode][cit_sha3_256] = {'cit_full_ids': [], 'citing_docs': [], 'cit_keys': cit_keys}

            mgdocs[cit_hash_mode][cit_sha3_256]['cit_full_ids'].append(cit_full_id)
            mgdocs[cit_hash_mode][cit_sha3_256]['citing_docs'].append(doc_id)
            mgdocs[cit_hash_mode][cit_sha3_256]['update_date'] = datetime.now().strftime('%Y-%m-%d')

    return mgdocs

