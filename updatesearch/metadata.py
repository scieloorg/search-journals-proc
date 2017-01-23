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
from articlemeta.client import ThriftClient


logger = logging.getLogger('updatesearch')

LOGGING_LEVEL = os.environ.get('LOGGING_LEVEL', None)
SOLR_URL = os.environ.get('SOLR_URL', 'http://127.0.0.1/solr')


class UpdateSearch(object):
    """
    Process to get article in article meta and index in Solr.
    """

    def __init__(self, period=None, from_date=None, until_date=None, collection=None, issn=None, delete=False, sanitization=False):
        self.delete = delete
        self.sanitization = sanitization
        self.collection = collection
        self.from_date = from_date
        self.until_date = until_date
        self.issn = issn
        self.solr = Solr(SOLR_URL, timeout=10)
        if period:
            self.from_date = datetime.now() - timedelta(days=period)

    def format_date(self, date):
        """
        Convert datetime.datetime to str return: ``2000-05-12``.

        :param datetime: bult-in datetime object

        :returns: str
        """
        if not date:
            return None

        return date.strftime('%Y-%m-%d')

    def pipeline_to_xml(self, article):
        """
        Pipeline to tranform a dictionary to XML format

        :param list_dict: List of dictionary content key tronsform in a XML.
        """

        ppl = plumber.Pipeline(
            pipeline_xml.SetupDocument(),
            pipeline_xml.DocumentID(),
            pipeline_xml.DOI(),
            pipeline_xml.Collection(),
            pipeline_xml.DocumentType(),
            pipeline_xml.URL(),
            pipeline_xml.Authors(),
            pipeline_xml.Titles(),
            pipeline_xml.OriginalTitle(),
            pipeline_xml.Pages(),
            pipeline_xml.WOKCI(),
            pipeline_xml.WOKSC(),
            pipeline_xml.JournalAbbrevTitle(),
            pipeline_xml.Languages(),
            pipeline_xml.AvailableLanguages(),
            pipeline_xml.Fulltexts(),
            pipeline_xml.PublicationDate(),
            pipeline_xml.SciELOPublicationDate(),
            pipeline_xml.SciELOProcessingDate(),
            pipeline_xml.Abstract(),
            pipeline_xml.AffiliationCountry(),
            pipeline_xml.AffiliationInstitution(),
            pipeline_xml.Sponsor(),
            pipeline_xml.Volume(),
            pipeline_xml.SupplementVolume(),
            pipeline_xml.Issue(),
            pipeline_xml.SupplementIssue(),
            pipeline_xml.ElocationPage(),
            pipeline_xml.StartPage(),
            pipeline_xml.EndPage(),
            pipeline_xml.JournalTitle(),
            pipeline_xml.IsCitable(),
            pipeline_xml.Permission(),
            pipeline_xml.Keywords(),
            pipeline_xml.JournalISSNs(),
            pipeline_xml.SubjectAreas(),
            pipeline_xml.ReceivedCitations(),
            pipeline_xml.TearDown()
        )

        xmls = ppl.run([article])

        # Add root document
        add = ET.Element('add')

        for xml in xmls:
            add.append(xml)

        return ET.tostring(add, encoding="utf-8", method="xml")

    def run(self):
        """
        Run the process for update article in Solr.
        """

        art_meta = ThriftClient()

        if self.delete:

            self.solr.delete(self.delete, commit=True)

        elif self.sanitization:

            # set of index ids
            ind_ids = set()

            # set of articlemeta ids
            art_ids = set()

            # all ids in index
            list_ids = json.loads(
                self.solr.select(
                    {'q': '*:*', 'fl': 'id', 'rows': 1000000}))['response']['docs']

            for id in list_ids:
                ind_ids.add(id['id'])

            # all ids in articlemeta
            for item in art_meta.documents(only_identifiers=True):
                art_ids.add('%s-%s' % (item.code, item.collection))

            # Ids to remove
            remove_ids = ind_ids - art_ids

            for id in remove_ids:
                self.solr.delete('id:%s' % id, commit=True)

            logger.info("List of removed ids: %s" % remove_ids)

        else:

            # Get article identifiers

            logger.info("Indexing in {0}".format(self.solr.url))

            for document in art_meta.documents(
                collection=self.collection,
                issn=self.issn,
                from_date=self.format_date(self.from_date),
                until_date=self.format_date(self.until_date)
            ):

                logger.debug("Loading document %s" % '_'.join([document.collection_acronym, document.publisher_id]))
                try:
                    xml = self.pipeline_to_xml(document)
                    self.solr.update(self.pipeline_to_xml(document), commit=True)
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
    Process to index article to SciELO Solr.

    This process collects articles in the Article meta using thrift and index
    in SciELO Solr.

    With this process it is possible to process all the article or some specific
    by collection, issn from date to until another date and a period like 7 days.
    """

    parser = argparse.ArgumentParser(textwrap.dedent(usage))

    parser.add_argument(
        '-p', '--period',
        type=int,
        help='index articles from specific period, use number of days.'
    )

    parser.add_argument(
        '-f', '--from_date',
        type=lambda x: datetime.strptime(x, '%Y-%m-%d'),
        nargs='?',
        help='index articles from specific date. YYYY-MM-DD.'
    )

    parser.add_argument(
        '-u', '--until_date',
        type=lambda x: datetime.strptime(x, '%Y-%m-%d'),
        nargs='?',
        help='index articles until this specific date. YYYY-MM-DD (default today).',
        default=datetime.now()
    )

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
        '-d', '--delete',
        default=None,
        help='delete query ex.: q=*:* (Lucene Syntax).'
    )

    parser.add_argument(
        '-s', '--sanitization',
        default=False,
        action='store_true',
        help='Remove objects from the index that are no longer present in the database.'
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
        us = UpdateSearch(
            period=args.period,
            from_date=args.from_date,
            until_date=args.until_date,
            collection=args.collection,
            issn=args.issn,
            delete=args.delete,
            sanitization=args.sanitization
        )
        us.run()
    except KeyboardInterrupt:
        logger.critical("Interrupt by user")
    finally:
        # End Time
        end = time.time()
        print("Duration {0} seconds.".format(end-start))
