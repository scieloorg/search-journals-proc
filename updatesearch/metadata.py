#!/usr/bin/python
# coding: utf-8
import argparse
import json
import logging
import logging.config
import os
import textwrap
import time
import zipfile
from copy import deepcopy
from datetime import datetime, timedelta

from updatesearch import pipeline_xml, citation_pipeline_xml
import plumber
from SolrAPI import Solr
from lxml import etree as ET

from articlemeta.client import ThriftClient


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
        'updatesearch.metadata': {
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

    def __init__(self, period=None, from_date=None, until_date=None,
                 collection=None, issn=None, delete=False, differential=False,
                 load_indicators=False, include_cited_references=False, external_metadata=None):
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
        self.include_cited_references = include_cited_references
        if external_metadata:
            self.external_metadata = self.load_external_metadata(external_metadata)

    def format_date(self, date):
        """
        Convert datetime.datetime to str return: ``2000-05-12``.

        :param datetime: bult-in datetime object

        :returns: str
        """
        if not date:
            return None

        return date.strftime('%Y-%m-%d')

    def load_external_metadata(self, path_external_data):
        """
        Lê arquivo com dados extras e normalizados de citações.
        Chave de cada registro é o id da citação.
        Valor de cada registro é o conjunto de campos extras e normalizados da citação.
        Caso path_external_data refira-se a um arquivo compactado, o json normalizador deve estar na sua raiz.

        :param path_external_data: caminho para dados normalizados de citações

        :returns: dict
        """
        normalization_file = os.path.basename(path_external_data)
        if normalization_file.endswith('.zip'):
            try:
                with zipfile.ZipFile(path_external_data) as zf:
                    if zf.namelist():
                        first_json = zf.namelist()[0]
                        data = zf.read(first_json)
                        return json.loads(data).get('metadata', {})
                    else:
                        return {}
            except FileNotFoundError as e:
                logger.error('FileNotFoundError: {0}'.format(e))
                logger.exception(e)
                return {}
            except json.JSONDecodeError as e:
                logger.error('JSONDecodeError: {0}'.format(e))
                logger.exception(e)
                return {}
            except Exception as e:
                logger.error('Error: {0}'.format(e))
                logger.exception(e)
                return {}
        else:
            try:
                with open(path_external_data) as f:
                    return json.loads(f.read()).get('metadata', {})
            except FileNotFoundError as e:
                logger.error("FileNotFoundError: {0}".format(e))
                logger.exception(e)
                return {}
            except json.JSONDecodeError as e:
                logger.error("JSONDecodeError: {0}".format(e))
                logger.exception(e)
                return {}
            except Exception as e:
                logger.error('Error: {0}'.format(e))
                logger.exception(e)
                return {}

    def add_fields_to_doc(self, pipeline_results, doc):
        """
        Adiciona a um doc os campos de um xml empacotado em um lista de tupla (raw, xml).

        :param pipeline_results: lista com um elemento tupla (raw, xml)
        :param doc: um ET.Element raiz
        """
        for raw, xml in pipeline_results:
            doc.find('.').extend(xml)

    def pipeline_to_xml(self, article):
        """
        Pipeline to tranform a dictionary to XML format

        :param list_dict: List of dictionary content key tronsform in a XML.
        """

        pipeline_itens = [
            pipeline_xml.SetupDocument(),
            pipeline_xml.DocumentID(),
            pipeline_xml.Entity(name='document'),
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
            pipeline_xml.SubjectAreas()
        ]

        if self.load_indicators is True:
            pipeline_itens.append(pipeline_xml.ReceivedCitations())

        add = ET.Element('add')

        if self.include_cited_references:
            article_pipeline_results = plumber.Pipeline(*pipeline_itens).run([article])

            cit_xmls, citations_fk_doc = self.get_xmls_citations(article)

            add.extend(cit_xmls)

            # Completa pipeline de artigo com informações estrangeiras das referências citadas
            self.add_fields_to_doc(article_pipeline_results, citations_fk_doc)

            add.append(citations_fk_doc)

        else:
            pipeline_itens.append(pipeline_xml.TearDown())
            article_pipeline_results = plumber.Pipeline(*pipeline_itens).run([article])

            add.extend(article_pipeline_results)

        return ET.tostring(add, encoding="utf-8", method="xml")

    def get_xmls_citations(self, document):
        """
        Pipeline para transformar citações em documentos Solr.
        Gera um <doc> citação para cada citação. Povoa esses <doc>s com dados das citações e do artigo citante.
        Extrai campos estrangeiros das referências citadas para povoar <doc> artigo.

        :param document: Article

        :return citations_xmls: <doc>s das citações
        :return citations_fk: campos estrangeiros das citações
        """

        # Pipeline para adicionar no <doc> citation dados da referência citada
        ppl_citation_itens = [
            pipeline_xml.SetupDocument(),
            pipeline_xml.Entity(name='citation'),
            citation_pipeline_xml.DocumentID(document.collection_acronym),
            citation_pipeline_xml.IndexNumber(),
            pipeline_xml.DOI(),
            citation_pipeline_xml.PublicationType(),
            citation_pipeline_xml.Authors(),
            citation_pipeline_xml.AnalyticAuthors(),
            citation_pipeline_xml.MonographicAuthors(),
            citation_pipeline_xml.PublicationDate(),
            citation_pipeline_xml.Institutions(),
            citation_pipeline_xml.Publisher(),
            citation_pipeline_xml.PublisherAddress(),
            pipeline_xml.Pages(),
            pipeline_xml.StartPage(),
            pipeline_xml.EndPage(),
            citation_pipeline_xml.Title(),
            citation_pipeline_xml.Source(),
            citation_pipeline_xml.Serie(),
            citation_pipeline_xml.ChapterTitle(),
            citation_pipeline_xml.ISBN(),
            citation_pipeline_xml.ISSN(),
            citation_pipeline_xml.Issue(),
            citation_pipeline_xml.Volume(),
            citation_pipeline_xml.Edition(),

            # Pipe para adicionar no <doc> citation o id do artigo citante
            citation_pipeline_xml.DocumentFK(document.collection_acronym),

            # Pipe para adicionar no <doc> citation a coleção do artigo citante
            citation_pipeline_xml.Collection(document.collection_acronym)
        ]

        if hasattr(self, 'external_metadata'):
            # Pipe para adicionar no <doc> citation dados normalizados do dicionário external_metadata
            ppl_citation_itens.append(
                citation_pipeline_xml.ExternalMetaData(self.external_metadata, document.collection_acronym))

        ppl_citation = plumber.Pipeline(*ppl_citation_itens)

        # Pipeline para adicionar no <doc> citation os autores e periódico do documento citante
        ppl_doc_fk = plumber.Pipeline(
            pipeline_xml.SetupDocument(),

            # Pipe para adicionar autores do artigo citante
            pipeline_xml.Authors(field_name='document_fk_au'),

            # Pipe para adicionar títulos do periódico do artigo citante
            pipeline_xml.JournalTitle(field_name="document_fk_ta"),
            pipeline_xml.JournalAbbrevTitle(field_name="document_fk_ta")
        )

        # Pipeline para adicionar no <doc> do artigo dados das referências citadas
        ppl_citation_fk_itens = [pipeline_xml.SetupDocument()]

        if hasattr(self, 'external_metadata'):
            # Com metadados externos
            ppl_citation_fk_itens.append(pipeline_xml.CitationsFKData(self.external_metadata))
        else:
            # Sem metadados externos
            ppl_citation_fk_itens.append(pipeline_xml.CitationsFKData())

        ppl_citations_fk = plumber.Pipeline(*ppl_citation_fk_itens)

        citations_xmls = []

        # Cria raiz para armazenar dados básicos do artigo
        article_fk_doc = ET.Element('doc')

        # Obtém dados estrangeiros do artigo (a serem inseridos nos <doc>s citação)
        article_fk_pipeline_results = ppl_doc_fk.run([document])

        # Insere na raiz doc_basic_xml os dados estrangeiros do artigo
        self.add_fields_to_doc(article_fk_pipeline_results, article_fk_doc)

        # Cria documentos para as citações
        if document.citations:
            for cit in document.citations:
                citation_doc = ET.Element('doc')
                if cit.publication_type in pipeline_xml.CITATION_ALLOWED_TYPES:
                    citation_pipeline_results = ppl_citation.run([cit])
                    # Adiciona tags da citação no documento citação
                    self.add_fields_to_doc(citation_pipeline_results, citation_doc)

                    # Adiciona tags do documento citante no documento citação
                    citation_doc.extend(deepcopy(article_fk_doc))

                    citations_xmls.append(citation_doc)

        # Cria tags de dados estrangeiros das citações a serem inseridas no documento citante
        citations_fk_doc = ET.Element('doc')
        citations_fk_pipeline_results = ppl_citations_fk.run([document])
        self.add_fields_to_doc(citations_fk_pipeline_results, citations_fk_doc)

        return citations_xmls, citations_fk_doc

    def differential_mode(self):
        art_meta = ThriftClient()

        logger.info("Running with differential mode")
        ind_ids = set()
        art_ids = set()

        # all ids in search index
        logger.info("Loading Search Index ids.")
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
        logger.info("Loading ArticleMeta ids.")
        for item in art_meta.documents(
            collection=self.collection,
            issn=self.issn,
            only_identifiers=True
        ):
            art_ids.add('%s-%s-%s' % (item.code, item.collection, item.processing_date))

        # Ids to remove
        if self.delete is True:
            logger.info("Running remove records process.")
            remove_ids = set([i[:27] for i in ind_ids]) - set([i[:27] for i in art_ids])
            logger.info("Removing (%d) documents from search index." % len(remove_ids))
            total_to_remove = len(remove_ids)
            if total_to_remove > 0:
                for ndx, to_remove_id in enumerate(remove_ids, 1):
                    logger.debug("Removing (%d/%d): %s" % (ndx, total_to_remove, to_remove_id))
                    self.solr.delete('id:%s' % to_remove_id, commit=False)

        # Ids to include
        logger.info("Running include records process.")
        include_ids = art_ids - ind_ids
        logger.info("Including (%d) documents to search index." % len(include_ids))
        total_to_include = len(include_ids)
        if total_to_include > 0:
            for ndx, to_include_id in enumerate(include_ids, 1):
                logger.debug("Including (%d/%d): %s" % (ndx, total_to_include, to_include_id))
                code = to_include_id[:23]
                collection = to_include_id[24: 27]
                processing_date = to_include_id[:-11]
                document = art_meta.document(code=code, collection=collection)
                try:
                    xml = self.pipeline_to_xml(document)
                    self.solr.update(xml, commit=False)
                except ValueError as e:
                    logger.error("ValueError: {0}".format(e))
                    logger.exception(e)
                    continue
                except Exception as e:
                    logger.error("Error: {0}".format(e))
                    logger.exception(e)
                    continue

    def common_mode(self):
        art_meta = ThriftClient()

        logger.info("Running without differential mode")
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
                self.solr.update(xml, commit=False)
            except ValueError as e:
                logger.error("ValueError: {0}".format(e))
                logger.exception(e)
                continue
            except Exception as e:
                logger.error("Error: {0}".format(e))
                logger.exception(e)
                continue

        if self.delete is True:
            logger.info("Running remove records process.")
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
            logger.info("Removing (%d) documents from search index." % len(remove_ids))
            remove_ids = ind_ids - art_ids
            for ndx, to_remove_id in enumerate(remove_ids, 1):
                logger.debug("Removing (%d/%d): %s" % (ndx, total_to_remove, to_remove_id))
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

    parser.add_argument(
        '--logging_level',
        '-l',
        default=LOGGING_LEVEL,
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help='Logggin level'
    )

    parser.add_argument(
        '--include_cited_references',
        '-r',
        action='store_true',
        help='include cited references in the indexing process'
    )

    parser.add_argument(
        '-m', '--external_metadata',
        default=None,
        help='json composed of external citations metadata'
    )

    args = parser.parse_args()
    LOGGING['handlers']['console']['level'] = args.logging_level
    for lg, content in LOGGING['loggers'].items():
        content['level'] = args.logging_level

    logging.config.dictConfig(LOGGING)

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
            load_indicators=args.load_indicators,
            include_cited_references=args.include_cited_references,
            external_metadata=args.external_metadata
        )
        us.run()
    except KeyboardInterrupt:
        logger.critical("Interrupt by user")
    finally:
        # End Time
        end = time.time()
        logger.info("Duration {0} seconds.".format(end-start))
