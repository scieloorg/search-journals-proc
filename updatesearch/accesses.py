#!/usr/bin/python
# coding: utf-8

from __future__ import print_function

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
import pipeline_xml
from articlemeta.client import ThriftClient as ArticleMetaThriftClient
from accessstats.client import ThriftClient as AccessThriftClient

logger = logging.getLogger('updatesearch')

LOGGING_LEVEL = os.environ.get('LOGGING_LEVEL', None)
SOLR_URL = os.environ.get('SOLR_URL', 'http://127.0.0.1/solr')


class UpdateSearch(object):
    """
    Process to get article in article meta and index in Solr.
    """

    def __init__(self, collection=None, issn=None):
        self.collection = collection
        self.issn = issn
        self.solr = Solr(SOLR_URL, timeout=10)

    def set_accesses(self, document_id, accesses):

        xml = ET.Element('add')

        doc = ET.Element('doc')

        identifier = ET.Element('field')
        identifier.set('name', 'id')
        identifier.text = document_id

        total_accesses = ET.Element('field')
        total_accesses.set('name', 'total_access')
        total_accesses.text = str(accesses)
        total_accesses.set('update', 'set')
        doc.append(identifier)
        doc.append(total_accesses)

        xml.append(doc)

        return ET.tostring(xml, encoding="utf-8", method="xml")

    def run(self):
        """
        Run the process for update article in Solr.
        """

        art_meta = ArticleMetaThriftClient()
        art_accesses = AccessThriftClient(domain="ratchet.scielo.org:11660")

        logger.info("Loading Solr available document ids")
        itens_query = []

        if self.collection:
            itens_query.append('in:%s' % self.collection)

        if self.issn:
            itens_query.append('issn:%s' % self.issn)

        query = '*:*' if len(itens_query) == 0 else ' AND '.join(itens_query)

        available_ids = set([i['id'] for i in json.loads(self.solr.select(
            {'q': query, 'fl': 'id', 'rows': 1000000}))['response']['docs']])

        logger.info("Recording accesses for documents in {0}".format(self.solr.url))

        for document in art_meta.documents(
            collection=self.collection,
            issn=self.issn
        ):

            solr_id = '-'.join([document.publisher_id, document.collection_acronym])

            if solr_id not in available_ids:
                continue

            logger.debug("Loading accesses for document %s" % solr_id)

            total_accesses = int(art_accesses.document(
                document.publisher_id,
                document.collection_acronym
            ).get('access_total', {'value': 0})['value'])

            xml = self.set_accesses(
                solr_id,
                total_accesses
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
    Process to load accesses count to documents in SciELO Solr.

    This process collects articles accesses and store it in SciELO Solr.
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
    logger.setLevel(args.logging_level)
    logging.basicConfig(
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    start = time.time()

    try:
        us = UpdateSearch(collection=args.collection, issn=args.issn)
        us.run()
    except KeyboardInterrupt:
        logger.critical("Interrupt by user")
    finally:
        # End Time
        end = time.time()
        print("Duration {0} seconds.".format(end-start))
