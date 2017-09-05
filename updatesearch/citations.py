#!/usr/bin/python
# coding: utf-8
import os
import sys
import time
import json
import argparse
import logging
import logging.config
import textwrap
import itertools
from datetime import datetime, timedelta

from lxml import etree as ET
from SolrAPI import Solr
import plumber
from articlemeta.client import ThriftClient as ArticleMetaThriftClient
from citedby.client import ThriftClient as CitedbyThriftClient

logger = logging.getLogger(__name__)

SOLR_URL = os.environ.get('SOLR_URL', 'http://127.0.0.1/solr')
SENTRY_HANDLER = os.environ.get('SENTRY_HANDLER', None)
LOGGING_LEVEL = os.environ.get('LOGGING_LEVEL', 'DEBUG')
LOGGING = {
    'version': 1,
    'disable_existing_loggers': True,

    'formatters': {
        'console': {
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            'datefmt': '%H:%M:%S',
            },
        },
    'handlers': {
        'console': {
            'level': LOGGING_LEVEL,
            'class': 'logging.StreamHandler',
            'formatter': 'console'
            }
        },
    'loggers': {
        '': {
            'handlers': ['console'],
            'level': LOGGING_LEVEL,
            'propagate': False,
            },
        'updatesearch.citations': {
            'level': LOGGING_LEVEL,
            'propagate': True,
        },
    }
}

if SENTRY_HANDLER:
    LOGGING['handlers']['sentry'] = {
        'level': 'ERROR',
        'class': 'raven.handlers.logging.SentryHandler',
        'dsn': SENTRY_HANDLER,
    }
    LOGGING['loggers']['']['handlers'].append('sentry')


class UpdateSearch(object):
    """
    Process to get article in article meta and index in Solr.
    """

    def __init__(self, collection=None, issn=None):
        self.collection = collection
        self.issn = issn
        self.solr = Solr(SOLR_URL, timeout=10)

    def set_citations(self, document_id, citations):

        xml = ET.Element('add')

        doc = ET.Element('doc')

        identifier = ET.Element('field')
        identifier.set('name', 'id')
        identifier.text = document_id

        total_citations = ET.Element('field')
        total_citations.set('name', 'total_received')
        total_citations.text = str(citations)
        total_citations.set('update', 'set')
        doc.append(identifier)
        doc.append(total_citations)

        xml.append(doc)

        return ET.tostring(xml, encoding="utf-8", method="xml")

    def run(self):
        """
        Run the process for update article in Solr.
        """

        art_meta = ArticleMetaThriftClient()
        art_citations = CitedbyThriftClient(domain="citedby.scielo.org:11610")

        logger.info("Loading Solr available document ids")
        itens_query = []

        if self.collection:
            itens_query.append('in:%s' % self.collection)

        if self.issn:
            itens_query.append('issn:%s' % self.issn)

        query = '*:*' if len(itens_query) == 0 else ' AND '.join(itens_query)

        available_ids = set([i['id'] for i in json.loads(self.solr.select(
            {'q': query, 'fl': 'id', 'rows': 1000000}))['response']['docs']])

        logger.info("Recording citations for documents in {0}".format(self.solr.url))

        for document in art_meta.documents(
            collection=self.collection,
            issn=self.issn
        ):

            solr_id = '-'.join([document.publisher_id, document.collection_acronym])

            if solr_id not in available_ids:
                continue

            logger.debug("Loading citations for document %s" % solr_id)

            result = art_citations.citedby_pid(document.publisher_id, metaonly=True)

            total_citations = result.get(
                'article', {'total_received': 0})['total_received']

            xml = self.set_citations(
                solr_id,
                total_citations
            )

            try:
                result = self.solr.update(xml, commit=False)
            except ValueError as e:
                logger.error("ValueError: {0}".format(e))
                logger.exception(e)
                continue
            except Exception as e:
                logger.error("Error: {0}".format(e))
                logger.exception(e)
                continue

        # optimize the index
        self.solr.commit()
        self.solr.optimize()


def main():

    usage = """\
    Process to load citations count to documents in SciELO Solr.

    This process collects articles citations and store it in SciELO Solr.
    """

    parser = argparse.ArgumentParser(textwrap.dedent(usage))

    parser.add_argument(
        '-c', '--collection',
        default=None,
        help='use the acronym of the collection eg.: spa, scl, col.'
    )

    parser.add_argument(
        '-i', '--issn',
        default=None,
        help='journal issn.'
    )

    parser.add_argument(
        '--logging_level',
        '-l',
        default=LOGGING_LEVEL,
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help='Logggin level'
    )

    args = parser.parse_args()
    LOGGING['handlers']['console']['level'] = args.logging_level
    for lg, content in LOGGING['loggers'].items():
        content['level'] = args.logging_level

    logging.config.dictConfig(LOGGING)

    start = time.time()

    try:
        us = UpdateSearch(collection=args.collection, issn=args.issn)
        us.run()
    except KeyboardInterrupt:
        logger.critical("Interrupt by user")
    finally:
        # End Time
        end = time.time()
        logger.info("Duration {0} seconds.".format(end-start))
