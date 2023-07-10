#!/usr/bin/python
# coding: utf-8
import os
import time
import json
import argparse
import textwrap
from datetime import datetime, timedelta

from lxml import etree as ET
from SolrAPI import Solr
import plumber

DEBUG = os.environ.get("DEBUG", "True") == "True"

# Debug mode
if DEBUG:
    from articlemeta.client import RestfulClient as AMClient
else:
    from articlemeta.client import ThriftClient as AMClient

import pipeline_xml


SOLR_URL = os.environ.get('SOLR_URL', 'http://127.0.0.1/solr')

class UpdateSearch(object):
    """
    Process to get article in article meta and index in Solr.
    """

    def __init__(self, period=None, from_date=None, until_date=None,
                 collection=None, issn=None, delete=False, differential=False,
                 load_indicators=False):
        self.delete = delete
        self.collection = collection
        self.from_date = from_date
        self.until_date = until_date
        self.differential = differential
        self.load_indicators = load_indicators
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

        pipeline_itens = [
            pipeline_xml.SetupDocument(),
            pipeline_xml.DocumentID(),
            pipeline_xml.DOI(),
            pipeline_xml.Collection(),
            pipeline_xml.DocumentType(),
            pipeline_xml.URL(),
            pipeline_xml.Authors(),
            pipeline_xml.Orcid(),
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
            pipeline_xml.Networks(),

        ]

        if self.load_indicators is True:
            pipeline_itens.append(pipeline_xml.ReceivedCitations())

        pipeline_itens.append(pipeline_xml.TearDown())

        ppl = plumber.Pipeline(*pipeline_itens)

        xmls = ppl.run([article])

        # Add root document
        add = ET.Element('add')

        for xml in xmls:
            add.append(xml)

        return ET.tostring(add, encoding="utf-8", method="xml")

    def differential_mode(self):
        art_meta = AMClient()

        print("Running with differential mode")
        ind_ids = set()
        art_ids = set()

        # all ids in search index
        print("Loading Search Index ids.")
        itens_query = []
        if self.collection:
            itens_query.append('in:%s' % self.collection)

        if self.issn:
            itens_query.append('issn:%s' % self.issn)

        query = '*:*' if len(itens_query) == 0 else ' AND '.join(itens_query)
        list_ids = json.loads(self.solr.select(
            {'q': query, 'fl': 'id,scielo_processing_date', 'rows': 1000000}))['response']['docs']

        for id in list_ids:
            ind_ids.add('%s-%s' % (id['id'], id.get('scielo_processing_date', '1900-01-01')))

        # all ids in articlemeta
        print("Loading ArticleMeta ids.")
        for item in art_meta.documents(
            collection=self.collection,
            issn=self.issn,
            only_identifiers=True
        ):
            art_ids.add('%s-%s-%s' % (item.code, item.collection, item.processing_date))

        # Ids to remove
        if self.delete is True:
            print("Running remove records process.")
            remove_ids = set([i[:27] for i in ind_ids]) - set([i[:27] for i in art_ids])
            print("Removing (%d) documents from search index." % len(remove_ids))
            total_to_remove = len(remove_ids)
            if total_to_remove > 0:
                for ndx, to_remove_id in enumerate(remove_ids, 1):
                    print("Removing (%d/%d): %s" % (ndx, total_to_remove, to_remove_id))
                    self.solr.delete('id:%s' % to_remove_id, commit=False)

        # Ids to include
        print("Running include records process.")
        include_ids = art_ids - ind_ids
        print("Including (%d) documents to search index." % len(include_ids))
        total_to_include = len(include_ids)
        if total_to_include > 0:
            for ndx, to_include_id in enumerate(include_ids, 1):
                print("Including (%d/%d): %s" % (ndx, total_to_include, to_include_id))
                code = to_include_id[:23]
                collection = to_include_id[24: 27]
                processing_date = to_include_id[:-11]
                document = art_meta.document(code=code, collection=collection)
                try:
                    xml = self.pipeline_to_xml(document)
                    self.solr.update(xml, commit=False)
                except ValueError as e:
                    print("ValueError: {0}".format(e))
                    print(e)
                    continue
                except Exception as e:
                    print("Error: {0}".format(e))
                    print(e)
                    continue

    def common_mode(self):
        art_meta = AMClient()

        print("Running without differential mode")
        print("Indexing in {0}".format(self.solr.url))
        print("Collection: {0}".format(self.collection))
        for document in art_meta.documents(
            collection=self.collection,
            issn=self.issn,
            from_date=self.format_date(self.from_date),
            until_date=self.format_date(self.until_date)
        ):

            print("Loading document %s" % '_'.join([document.collection_acronym, document.publisher_id]))

            try:
                xml = self.pipeline_to_xml(document)
                self.solr.update(xml, commit=False)
            except ValueError as e:
                print("ValueError: {0}".format(e))
                print(e)
                continue
            except Exception as e:
                print("Error: {0}".format(e))
                print(e)
                continue

        if self.delete is True:
            print("Running remove records process.")
            ind_ids = set()
            art_ids = set()

            itens_query = []
            if self.collection:
                itens_query.append('in:%s' % self.collection)

            if self.issn:
                itens_query.append('issn:%s' % self.issn)

            query = '*:*' if len(itens_query) == 0 else ' AND '.join(itens_query)

            list_ids = json.loads(self.solr.select(
                {'q': query, 'fl': 'id', 'rows': 1000000}))['response']['docs']

            for id in list_ids:
                ind_ids.add(id['id'])

            # all ids in articlemeta
            for item in art_meta.documents(
                collection=self.collection,
                issn=self.issn,
                only_identifiers=True
            ):
                art_ids.add('%s-%s' % (item.code, item.collection))
            # Ids to remove
            total_to_remove = len(remove_ids)
            print("Removing (%d) documents from search index." % len(remove_ids))
            remove_ids = ind_ids - art_ids
            for ndx, to_remove_id in enumerate(remove_ids, 1):
                print("Removing (%d/%d): %s" % (ndx, total_to_remove, to_remove_id))
                self.solr.delete('id:%s' % to_remove_id, commit=False)

    def run(self):
        """
        Run the process for update article in Solr.
        """
        if self.differential is True:
            self.differential_mode()
        else:
            self.common_mode()

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
        '-x', '--differential',
        default=False,
        action='store_true',
        help='Update and Remove records according to a comparison between ArticleMeta ID\'s and the ID\' available in the search engine. It will consider the processing date as a compounded matching key collection+pid+processing_date. This option will run over the entire index, the parameters -p -f -u will not take effect when this option is selected.'
    )

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
        '-n', '--load_indicators',
        default=False,
        action='store_true',
        help='Load articles received citations and downloads while including or updating documents. It makes the processing extremelly slow.'
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
        default=False,
        action='store_true',
        help='delete query ex.: q=*:* (Lucene Syntax).'
    )

    args = parser.parse_args()

    start = time.time()

    try:
        us = UpdateSearch(
            period=args.period,
            from_date=args.from_date,
            until_date=args.until_date,
            collection=args.collection,
            issn=args.issn,
            delete=args.delete,
            differential=args.differential,
            load_indicators=args.load_indicators
        )
        us.run()
    except KeyboardInterrupt:
        print("Interrupt by user")
    finally:
        # End Time
        end = time.time()
        print("Duration {0} seconds.".format(end-start))

if __name__ == "__main__":
    main()