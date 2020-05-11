# coding: utf-8
import unittest

from lxml import etree as ET
import json
import os

from xylose.scielodocument import Article, Citation

from updatesearch import citation_pipeline_xml

class ExportTests(unittest.TestCase):

    def setUp(self):
        self._raw_json = json.loads(open(os.path.dirname(__file__) + '/fixtures/article_meta.json').read())
        self._article_meta = Article(self._raw_json)

    def test_xml_citation_analytic_authors_pipe(self):

        pxml = ET.Element('doc')

        fake_xylose_citation = Citation({'v18': '', 'v10': [
            {'r': 'ND', 's': 'Bastos', '_': '', 'n': 'MG'}, {'r': 'ND', 's': 'Carmo', '_': '', 'n': 'WB'},
            {'r': 'ND', 's': 'Abrita', '_': '', 'n': 'RR'}, {'r': 'ND', 's': 'Almeida', '_': '', 'n': 'EC'},
            {'r': 'ND', 's': 'Mafra', '_': '', 'n': 'D'}, {'r': 'ND', 's': 'Costa', '_': '', 'n': 'DMN'}
        ]})

        data = [fake_xylose_citation, pxml]

        xml_citation = citation_pipeline_xml.AnalyticAuthors()
        raw, xml = xml_citation.transform(data)

        result = '; '.join([ac.text for ac in xml.findall('./field[@name="cit_ana_au"]')])

        self.assertEqual(u'Bastos, MG; Carmo, WB; Abrita, RR; Almeida, EC; Mafra, D; Costa, DMN', result)

    def test_xml_citation_authors_pipe(self):

        pxml = ET.Element('doc')

        fake_xylose_citation = Citation({'v18': '', 'v10': [
            {'r': 'ND', 's': 'Bastos', '_': '', 'n': 'MG'}, {'r': 'ND', 's': 'Carmo', '_': '', 'n': 'WB'},
            {'r': 'ND', 's': 'Abrita', '_': '', 'n': 'RR'}, {'r': 'ND', 's': 'Almeida', '_': '', 'n': 'EC'},
            {'r': 'ND', 's': 'Mafra', '_': '', 'n': 'D'}, {'r': 'ND', 's': 'Costa', '_': '', 'n': 'DMN'}
        ]})

        data = [fake_xylose_citation, pxml]

        xml_citation = citation_pipeline_xml.Authors()
        raw, xml = xml_citation.transform(data)

        result = '; '.join([ac.text for ac in xml.findall('./field[@name="au"]')])

        self.assertEqual(u'Bastos, MG; Carmo, WB; Abrita, RR; Almeida, EC; Mafra, D; Costa, DMN', result)

    def test_xml_citation_chapter_title(self):

        pxml = ET.Element('doc')

        fake_xylose_citation = Citation({'v18': '', 'v12': [
            {'l': 'pt', '_': u'Doença renal crônica: problemas e soluções'}]})

        data = [fake_xylose_citation, pxml]

        xml_chapter_citation = citation_pipeline_xml.ChapterTitle()
        raw, xml = xml_chapter_citation.transform(data)

        result = xml.find('./field[@name="cit_chapter_title"]').text

        self.assertEqual(u'Doença renal crônica: problemas e soluções', result)

    def test_xml_citation_chapter_title_without_data_pipe(self):

        pxml = ET.Element('doc')

        fake_xylose_citation = Citation({})
        data = [fake_xylose_citation, pxml]

        xml_book_citation = citation_pipeline_xml.ChapterTitle()
        raw, xml = xml_book_citation.transform(data)

        self.assertIsNone(xml.find('./field[@name="cit_chapter_title"]'))

    def test_xml_citation_collection_pipe(self):

        pxml = ET.Element('doc')

        fake_xylose_citation = Citation({})
        data = [fake_xylose_citation, pxml]

        xml_citation = citation_pipeline_xml.Collection(self._article_meta.collection_acronym)
        raw, xml = xml_citation.transform(data)

        result = xml.find('./field[@name="in"]').text

        self.assertEqual(u'scl', result)

    def test_xml_citation_document_fk_pipe(self):

        pxml = ET.Element('doc')

        fake_xylose_citation = Citation({'v880': [{'_': 'S0034-8910201000040000700002'}], 'v701': [{'_': '2'}]})
        data = [fake_xylose_citation, pxml]

        xml_citation = citation_pipeline_xml.DocumentFK(self._article_meta.collection_acronym)
        raw, xml = xml_citation.transform(data)

        result = xml.find('./field[@name="document_fk"]').text

        self.assertEqual(u'S0034-89102010000400007-scl', result)

    def test_xml_citation_document_id_pipe(self):

        pxml = ET.Element('doc')

        fake_xylose_citation = Citation({'v880': [{'_': 'S0034-8910201000040000700002'}], 'v701': [{'_': '2'}]})
        data = [fake_xylose_citation, pxml]

        xml_citation = citation_pipeline_xml.DocumentID(self._article_meta.collection_acronym)
        raw, xml = xml_citation.transform(data)

        result = xml.find('./field[@name="id"]').text

        self.assertEqual(u'S0034-8910201000040000700002-scl', result)

    def test_xml_citation_edition_pipe(self):

        pxml = ET.Element('doc')

        fake_xylose_citation = Citation({'v18': '', 'v63': [{'_': '3'}]})
        data = [fake_xylose_citation, pxml]

        xml_book_citation = citation_pipeline_xml.Edition()
        raw, xml = xml_book_citation.transform(data)

        result = xml.find('./field[@name="cit_edition"]').text

        self.assertEqual(u'3', result)

    def test_xml_citation_edition_without_data_pipe(self):

        pxml = ET.Element('doc')

        fake_xylose_citation = Citation({})
        data = [fake_xylose_citation, pxml]

        xml_citation = citation_pipeline_xml.Edition()
        raw, xml = xml_citation.transform(data)

        result = xml.find('./field[@name="cit_edition"]')

        self.assertIsNone(result)

    def test_xml_citation_external_data_pipe(self):

        pxml = ET.Element('doc')

        fake_xylose_citation = Citation({'v880': [{'_': 'S0034-8910201000040000700002'}], 'v701': [{'_': '2'}]})
        data = [fake_xylose_citation, pxml]

        fake_external_metadata = {
            u'S0034-8910201000040000700002-scl': {
                'type': u'journal-article',
                'container-title': [u'Brazilian Journal of Nephrology'],
                'ISSN': [u'0101-2800'],
                'BC1-ISSNS': [u'0101-2800', u'2175-8239'],
                'BC1-JOURNAL-TITLES': [
                    u'BRAZILIAN JOURNAL OF NEPHROLOGY',
                    u'J BRAS NEFROL',
                    u'JORNAL BRASILEIRO DE NEFROLOGIA',
                    u'BRAZ J NEPHROL ONLINE',
                    u'BRAZ J NEPHROL',
                    u'BRAZ J NEPHROL IMPR'
                ],
                'normalization-status': u'4',
            }
        }

        xml_citation = citation_pipeline_xml.ExternalMetaData(fake_external_metadata, 
                                                                      self._article_meta.collection_acronym)
        raw, xml = xml_citation.transform(data)

        obtained_title_canonical = xml.find('./field[@name="cit_journal_title_canonical"]').text
        self.assertEqual(obtained_title_canonical, 'Brazilian Journal of Nephrology')

        obtained_issn_canonical = \
            '; '.join([ac.text for ac in xml.findall('./field[@name="cit_journal_issn_canonical"]')])
        self.assertEqual(obtained_issn_canonical, '0101-2800')

        obtained_title_normalized = \
            '; '.join([ac.text for ac in xml.findall('./field[@name="cit_journal_title_normalized"]')])
        self.assertEqual(obtained_title_normalized, 'BRAZILIAN JOURNAL OF NEPHROLOGY; J BRAS NEFROL;'
                                                    ' JORNAL BRASILEIRO DE NEFROLOGIA; BRAZ J NEPHROL ONLINE;'
                                                    ' BRAZ J NEPHROL; BRAZ J NEPHROL IMPR')

        obtained_issn_normalized = \
            '; '.join([ac.text for ac in xml.findall('./field[@name="cit_journal_issn_normalized"]')])
        self.assertEqual(obtained_issn_normalized, '0101-2800; 2175-8239')

        self.assertEqual(xml.find('./field[@name="cit_normalization_status"]').text, '4')

    def test_xml_citation_index_number_pipe(self):

        pxml = ET.Element('doc')

        fake_xylose_citation = Citation({'v880': [{'_': 'S0034-8910201000040000700002'}], 'v701': [{'_': '2'}]})
        data = [fake_xylose_citation, pxml]

        xml_citation = citation_pipeline_xml.IndexNumber()
        raw, xml = xml_citation.transform(data)

        result = xml.find('./field[@name="cit_index_number"]').text

        self.assertEqual(u'2', result)

    def test_xml_citation_institutions_pipe(self):

        pxml = ET.Element('doc')
        fake_xylose_citation = Citation({'v17': [{'_': 'World Health Organization'}]})
        data = [fake_xylose_citation, pxml]

        xml_citation = citation_pipeline_xml.Institutions()
        raw, xml = xml_citation.transform(data)

        result = xml.find('./field[@name="cit_inst"]').text

        self.assertEqual(u'World Health Organization', result)

    def test_xml_citation_institutions_without_data_pipe(self):

        pxml = ET.Element('doc')

        fake_xylose_citation = Citation({})
        data = [fake_xylose_citation, pxml]

        xml_book_citation = citation_pipeline_xml.Institutions()
        raw, xml = xml_book_citation.transform(data)

        result = xml.find('./field[@name="cit_inst"]')

        self.assertIsNone(result)

    def test_xml_citation_isbn_pipe(self):

        pxml = ET.Element('doc')

        fake_xylose_citation = Citation({'v18': '', 'v69': [{'_': '978-8582423400'}]})
        data = [fake_xylose_citation, pxml]

        xml_book_citation = citation_pipeline_xml.ISBN()
        raw, xml = xml_book_citation.transform(data)

        result = xml.find('./field[@name="cit_isbn"]').text

        self.assertEqual(u'978-8582423400', result)

    def test_xml_citation_isbn_without_data_pipe(self):

        pxml = ET.Element('doc')

        fake_xylose_citation = Citation({})
        data = [fake_xylose_citation, pxml]

        xml_book_citation = citation_pipeline_xml.ISBN()
        raw, xml = xml_book_citation.transform(data)

        self.assertIsNone(xml.find('./field[@name="cit_isbn"]'))

    def test_xml_citation_issn_pipe(self):

        pxml = ET.Element('doc')

        fake_xylose_citation = Citation({'v30': '', 'v35': [{'_': '0101-2800'}]})
        data = [fake_xylose_citation, pxml]

        xml_citation = citation_pipeline_xml.ISSN()
        raw, xml = xml_citation.transform(data)

        result = xml.find('./field[@name="cit_issn"]').text

        self.assertEqual('0101-2800', result)

    def test_xml_citation_issn_without_data_pipe(self):

        pxml = ET.Element('doc')

        fake_xylose_citation = Citation({})
        data = [fake_xylose_citation, pxml]

        xml_citation = citation_pipeline_xml.ISSN()
        raw, xml = xml_citation.transform(data)

        self.assertIsNone(xml.find('./field[@name="cit_issn"]'))

    def test_xml_citation_issue_pipe(self):

        pxml = ET.Element('doc')

        fake_xylose_citation = Citation({'v30': '', 'v32': [{'_': '4'}]})
        data = [fake_xylose_citation, pxml]

        xml_citation = citation_pipeline_xml.Issue()
        raw, xml = xml_citation.transform(data)

        result = xml.find('./field[@name="issue"]').text

        self.assertEqual(u'4', result)

    def test_xml_citation_monographic_authors_pipe(self):

        pxml = ET.Element('doc')

        fake_xylose_citation = Citation({'v18': '', 'v16': [{'r': 'ND', 's': 'Dutton', '_': '', 'n': 'DG'}]})
        data = [fake_xylose_citation, pxml]

        xml_book_citation = citation_pipeline_xml.MonographicAuthors()
        raw, xml = xml_book_citation.transform(data)

        result = '; '.join([ac.text for ac in xml.findall('./field[@name="cit_mon_au"]')])

        self.assertEqual(u'Dutton, DG', result)

    def test_xml_citation_publication_date_pipe(self):

        pxml = ET.Element('doc')

        fake_xylose_citation = Citation({'v30': '', 'v65': [{'_': '20040000'}]})
        data = [fake_xylose_citation, pxml]

        xml_citation = citation_pipeline_xml.PublicationDate()
        raw, xml = xml_citation.transform(data)

        result = xml.find('./field[@name="da"]').text

        self.assertEqual(u'2004', result)

    def test_xml_citation_publication_type_article_pipe(self):

        cit_art_pxml = ET.Element('doc')

        fake_xylose_article_citation = Citation({'v30': ''})
        cit_art_data = [fake_xylose_article_citation, cit_art_pxml]

        xml_citation = citation_pipeline_xml.PublicationType()
        raw, art_xml = xml_citation.transform(cit_art_data)

        art_result = art_xml.find('./field[@name="cit_type"]').text

        self.assertEqual(u'article', art_result)

    def text_xml_citation_publication_type_book_pipe(self):

        cit_book_pxml = ET.Element('doc')

        fake_xylose_book_citation = Citation({'v18': ''})
        cit_book_data = [fake_xylose_book_citation, cit_book_pxml]

        xml_book_citation = citation_pipeline_xml.PublicationType()
        raw, book_xml = xml_book_citation.transform(cit_book_data)

        book_result = book_xml.find('./field[@name="cit_type"]').text

        self.assertEqual(u'book', book_result)

    def test_xml_citation_publisher_pipe(self):

        pxml = ET.Element('doc')

        fake_xylose_citation = Citation({'v18': '', 'v62': [{'_': 'Oxford University Press'}]})
        data = [fake_xylose_citation, pxml]

        xml_chapter_citation = citation_pipeline_xml.Publisher()
        raw, xml = xml_chapter_citation.transform(data)

        result = xml.find('./field[@name="cit_publisher"]').text

        self.assertEqual(u'Oxford University Press', result)

    def test_xml_citation_publisher_pipe_without_data_pipe(self):

        pxml = ET.Element('doc')

        fake_xylose_citation = Citation({})
        data = [fake_xylose_citation, pxml]

        xml_citation = citation_pipeline_xml.Publisher()
        raw, xml = xml_citation.transform(data)

        self.assertIsNone(xml.find('./field[@name="cit_publisher"]'))

    def test_xml_citation_publisher_address_pipe(self):

        pxml = ET.Element('doc')

        fake_xylose_citation = Citation({'v18': '', 'v66': [{'_': 'Nova York'}]})
        data = [fake_xylose_citation, pxml]

        xml_chapter_citation = citation_pipeline_xml.PublisherAddress()
        raw, xml = xml_chapter_citation.transform(data)

        result = xml.find('./field[@name="cit_publisher_address"]').text

        self.assertEqual(u'Nova York', result)

    def test_xml_citation_publisher_address_pipe_without_data_pipe(self):

        pxml = ET.Element('doc')

        fake_xylose_citation = Citation({})
        data = [fake_xylose_citation, pxml]

        xml_citation = citation_pipeline_xml.PublisherAddress()
        raw, xml = xml_citation.transform(data)

        self.assertIsNone(xml.find('./field[@name="cit_publisher_address"]'))

    def test_xml_citation_serie_pipe(self):

        pxml = ET.Element('doc')

        fake_xylose_citation = Citation({'v18': '', 'v25': [{'_': 'Geodynamics'}], 'v69': [{'_': '978-8582423400'}]})
        data = [fake_xylose_citation, pxml]

        xml_citation = citation_pipeline_xml.Serie()
        raw, xml = xml_citation.transform(data)

        result = xml.find('./field[@name="cit_serie"]').text

        self.assertEqual(u'Geodynamics', result)

    def test_xml_citation_serie_pipe_without_pipe(self):

        pxml = ET.Element('doc')

        fake_xylose_citation = Citation({})
        data = [fake_xylose_citation, pxml]

        xml_citation = citation_pipeline_xml.Serie()
        raw, xml = xml_citation.transform(data)

        self.assertIsNone(xml.find('./field[@name="cit_serie"]'))

    def test_xml_citation_source_article_pipe(self):

        art_pxml = ET.Element('doc')

        fake_xylose_art_citation = Citation({'v30': [{'_': 'J Bras Nefrol'}]})
        art_data = [fake_xylose_art_citation, art_pxml]

        xml_citation = citation_pipeline_xml.Source()
        raw, art_xml = xml_citation.transform(art_data)

        art_result = art_xml.find('./field[@name="cit_journal_title"]').text

        self.assertEqual(u'J Bras Nefrol', art_result)

    def test_xml_citation_source_book_pipe(self):

        book_pxml = ET.Element('doc')

        fake_xylose_book_citation = Citation({'v18': [{'_': 'Health measurement scales: A practical guide to their'
                                                            ' development and use'}]})
        book_data = [fake_xylose_book_citation, book_pxml]

        xml_book_citation = citation_pipeline_xml.Source()
        raw, art_xml = xml_book_citation.transform(book_data)

        book_result = art_xml.find('./field[@name="ti"]').text

        self.assertEqual(u'Health measurement scales: A practical guide to their development and use', book_result)

    def test_xml_citation_source_pipe_without_pipe(self):

        pxml = ET.Element('doc')

        fake_xylose_citation = Citation({})
        data = [fake_xylose_citation, pxml]

        xml_citation = citation_pipeline_xml.Source()
        raw, xml = xml_citation.transform(data)

        self.assertIsNone(xml.find('./field[@name="cit_journal_title"]'))

    def test_xml_citation_title_pipe(self):

        pxml = ET.Element('doc')

        fake_xylose_citation = Citation(
            {'v30': [{'_': u'J Bras Nefrol'}],
             'v12': [{'_': u'Doença renal crônica: problemas e soluções'}]})
        data = [fake_xylose_citation, pxml]

        xml_citation = citation_pipeline_xml.Title()
        raw, xml = xml_citation.transform(data)

        result = xml.find('./field[@name="ti"]').text

        self.assertEqual(u'Doença renal crônica: problemas e soluções', result)

    def test_xml_citation_volume_pipe(self):

        pxml = ET.Element('doc')

        fake_xylose_citation = Citation(
            {'v30': [{'_': 'J Bras Nefrol'}],
             'v31': [{'_': '26'}],
             'v12': [{'_': 'Doença renal crônica: problemas e soluções'}]})
        data = [fake_xylose_citation, pxml]

        xml_citation = citation_pipeline_xml.Volume()
        raw, xml = xml_citation.transform(data)

        result = xml.find('./field[@name="volume"]').text

        self.assertEqual(u'26', result)

    def test_xml_citation_volume_without_data_pipe(self):

        pxml = ET.Element('doc')

        fake_xylose_citation = Citation({})
        data = [fake_xylose_citation, pxml]

        xml_citation = citation_pipeline_xml.Volume()
        raw, xml = xml_citation.transform(data)

        self.assertIsNone(xml.find('./find[@name="volume"]'))
