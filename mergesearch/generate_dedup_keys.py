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


