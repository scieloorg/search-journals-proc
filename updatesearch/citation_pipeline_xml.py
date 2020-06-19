# coding: utf-8
from lxml import etree as ET
from pymongo import collection as MongoCollection
from updatesearch.field_sanitizer import get_author_name_quality, get_date_quality, remove_period

import plumber


class AnalyticAuthors(plumber.Pipe):

    def precond(data):

        raw, xml = data

        if not raw.analytic_authors:
            raise plumber.UnmetPrecondition()

    @plumber.precondition(precond)
    def transform(self, data):
        raw, xml = data

        for author in raw.analytic_authors:
            name = []

            if 'surname' in author:
                name.append(author['surname'])

            if 'given_names' in author:
                name.append(author['given_names'])

            fullname = remove_period(', '.join(name))
            if fullname:
                field = ET.Element('field')
                field.text = fullname
                field.set('name', 'cit_ana_au')
                xml.find('.').append(field)

        return data


class Authors(plumber.Pipe):

    def precond(data):

        raw, xml = data

        if not raw.authors:
            raise plumber.UnmetPrecondition()

    @plumber.precondition(precond)
    def transform(self, data):
        raw, xml = data

        for  author in raw.authors:
            name = []

            if 'surname' in author:
                name.append(author['surname'])

            if 'given_names' in author:
                name.append(author['given_names'])

            fullname = remove_period(', '.join(name))
            if fullname:
                field = ET.Element('field')
                field.text = fullname
                field.set('name', 'au')
                xml.find('.').append(field)

                au_quality_level = get_author_name_quality(fullname)
                if au_quality_level:
                    field = ET.Element('field')
                    field.text = str(au_quality_level)
                    field.set('name', 'cit_au_quality_level')

                    xml.find('.').append(field)

        return data


class ChapterTitle(plumber.Pipe):

    def precond(data):

        raw, xml = data

        if not raw.chapter_title:
            raise plumber.UnmetPrecondition()

    @plumber.precondition(precond)
    def transform(self, data):
        raw, xml = data

        chapter_title = remove_period(raw.chapter_title)
        if chapter_title:
            field = ET.Element('field')
            field.text = chapter_title
            field.set('name', 'ti')

            xml.find('.').append(field)

            field = ET.Element('field')
            field.text = chapter_title
            field.set('name', 'cit_chapter_title')

            xml.find('.').append(field)

        return data


class Collection(plumber.Pipe):

    def __init__(self, collection):
        self.collection = collection

    def transform(self, data):
        raw, xml = data

        field = ET.Element('field')
        field.text = self.collection
        field.set('name', 'in')

        xml.find('.').append(field)

        return data


class DocumentFK(plumber.Pipe):
    """
    Adiciona no XML da citação o id (pid) do documento citante.
    O campo document_fk é a chave estrangeira do documento citante.
    """

    def __init__(self, collection):
        self.collection = collection

    def transform(self, data):
        raw, xml = data

        field = ET.Element('field')

        # Ignores the last five numbers; these are for reference ids
        cit_id = raw.data['v880'][0]['_'][:-5]
        cit_full_id = '{0}-{1}'.format(cit_id, self.collection)

        field.text = cit_full_id
        field.set('name', 'document_fk')

        xml.find('.').append(field)

        return data


class DocumentID(plumber.Pipe):

    def __init__(self, collection):
        self.collection = collection

    def transform(self, data):
        raw, xml = data

        cit_id = raw.data['v880'][0]['_']
        cit_full_id = '{0}-{1}'.format(cit_id, self.collection)

        field = ET.Element('field')
        field.text = cit_full_id
        field.set('name', 'id')

        xml.find('.').append(field)

        return data


class Edition(plumber.Pipe):

    def precond(data):

        raw, xml = data

        if not raw.edition:
            raise plumber.UnmetPrecondition()

    @plumber.precondition(precond)
    def transform(self, data):
        raw, xml = data

        edition = remove_period(raw.edition)
        if edition:
            field = ET.Element('field')
            field.text = edition
            field.set('name', 'cit_edition')

            xml.find('.').append(field)

        return data


class ExternalMetaData(plumber.Pipe):
    """
    Adiciona no <doc> citation dados normalizados de citação.

    :param standardizer: base de normalização em formato de dicionário
    :param collection: coleção do documento citante
    """

    def __init__(self, standardizer: MongoCollection, collection):
        self.collection = collection
        self.standardizer = standardizer

    def transform(self, data):
        raw, xml = data

        cit_id = raw.data['v880'][0]['_']
        cit_full_id = '{0}-{1}'.format(cit_id, self.collection)
        cit_std_data = self.standardizer.find_one({'_id': cit_full_id})

        if cit_std_data:
            official_journal_title = cit_std_data.get('official-journal-title', [])
            for ojt in official_journal_title:
                field = ET.Element('field')
                field.text = ojt.lower()
                field.set('name', 'cit_official_journal_title')
                xml.find('.').append(field)

            official_abbreviated_journal_title = cit_std_data.get('official-abbreviated-journal-title', [])
            for oajt in official_abbreviated_journal_title:
                field = ET.Element('field')
                field.text = oajt.lower()
                field.set('name', 'cit_official_abbreviated_journal_title')
                xml.find('.').append(field)

            alternative_journal_title = cit_std_data.get('alternative-journal-titles', [])
            for ajt in alternative_journal_title:
                field = ET.Element('field')
                field.text = ajt.lower()
                field.set('name', 'cit_alternative_journal_title')
                xml.find('.').append(field)

            issn_l = cit_std_data.get('issn-l')
            if issn_l:
                field = ET.Element('field')
                field.text = issn_l
                field.set('name', 'cit_official_journal_issn_l')
                xml.find('.').append(field)

            issn = cit_std_data.get('issn', [])
            for i in issn:
                field = ET.Element('field')
                field.text = i
                field.set('name', 'cit_official_journal_issn')
                xml.find('.').append(field)

            status = cit_std_data.get('status')
            if status:
                field = ET.Element('field')
                field.text = str(status)
                field.set('name', 'cit_normalization_status')
                xml.find('.').append(field)

        return data


class Institutions(plumber.Pipe):

    def precond(data):

        raw, xml = data

        if not raw.institutions:
            raise plumber.UnmetPrecondition()

    @plumber.precondition(precond)
    def transform(self, data):
        raw, xml = data

        for institution in raw.institutions:
            institution_name = remove_period(institution)

            if institution_name:
                field = ET.Element('field')
                field.text = institution_name
                field.set('name', 'cit_inst')

                xml.find('.').append(field)

        return data


class ISBN(plumber.Pipe):

    def precond(data):

        raw, xml = data

        if not raw.isbn:
            raise plumber.UnmetPrecondition()

    @plumber.precondition(precond)
    def transform(self, data):
        raw, xml = data

        isbn = remove_period(raw.isbn)
        if isbn:
            field = ET.Element('field')
            field.text = isbn
            field.set('name', 'cit_isbn')

            xml.find('.').append(field)

        return data


class ISSN(plumber.Pipe):

    def precond(data):

        raw, xml = data

        if not raw.issn:
            raise plumber.UnmetPrecondition()

    @plumber.precondition(precond)
    def transform(self, data):
        raw, xml = data

        issn = remove_period(raw.issn)
        if issn:
            field = ET.Element('field')
            field.text = raw.issn
            field.set('name', 'cit_issn')

            xml.find('.').append(field)

        return data


class Issue(plumber.Pipe):

    def precond(data):

        raw, xml = data

        if not raw.issue:
            raise plumber.UnmetPrecondition()

    @plumber.precondition(precond)
    def transform(self, data):
        raw, xml = data

        issue = remove_period(raw.issue)
        if issue:
            field = ET.Element('field')
            field.text = raw.issue
            field.set('name', 'issue')

            xml.find('.').append(field)

        return data


class MonographicAuthors(plumber.Pipe):

    def precond(data):

        raw, xml = data

        if not raw.monographic_authors:
            raise plumber.UnmetPrecondition()

    @plumber.precondition(precond)
    def transform(self, data):
        raw, xml = data

        for author in raw.monographic_authors:
            name = []

            if 'surname' in author:
                name.append(author['surname'])

            if 'given_names' in author:
                name.append(author['given_names'])

            fullname = remove_period(', '.join(name))
            if fullname:
                field = ET.Element('field')
                field.text = fullname
                field.set('name', 'cit_mon_au')
                xml.find('.').append(field)

        return data


class PublicationDate(plumber.Pipe):

    def precond(data):
        raw, xml = data

        if not raw.publication_date:
            raise plumber.UnmetPrecondition()

    @plumber.precondition(precond)
    def transform(self, data):
        raw, xml = data

        field = ET.Element('field')
        field.text = raw.publication_date
        field.set('name', 'da')

        xml.find('.').append(field)

        # da_quality_level é utilizado para sabermos o nível de consistência (limpeza) do campo "raw.publication_date"
        da_quality_level = get_date_quality(raw.publication_date)

        if da_quality_level:
            field = ET.Element('field')
            field.text = str(da_quality_level)
            field.set('name', 'cit_da_quality_level')

            xml.find('.').append(field)

        return data


class PublicationType(plumber.Pipe):

    def transform(self, data):
        raw, xml = data

        field = ET.Element('field')
        field.text = raw.publication_type
        field.set('name', 'cit_type')

        xml.find('.').append(field)

        return data


class Publisher(plumber.Pipe):

    def precond(data):

        raw, xml = data

        if not raw.publisher:
            raise plumber.UnmetPrecondition()

    @plumber.precondition(precond)
    def transform(self, data):
        raw, xml = data

        publisher = remove_period(raw.publisher)
        if publisher:
            field = ET.Element('field')
            field.text = publisher
            field.set('name', 'cit_publisher')

            xml.find('.').append(field)

        return data


class PublisherAddress(plumber.Pipe):

    def precond(data):

        raw, xml = data

        if not raw.publisher_address:
            raise plumber.UnmetPrecondition()

    @plumber.precondition(precond)
    def transform(self, data):
        raw, xml = data

        publisher_address = remove_period(raw.publisher_address)
        if publisher_address:
            field = ET.Element('field')
            field.text = publisher_address
            field.set('name', 'cit_publisher_address')

            xml.find('.').append(field)

        return data


class Serie(plumber.Pipe):

    def precond(data):

        raw, xml = data

        if not raw.serie:
            raise plumber.UnmetPrecondition()

    @plumber.precondition(precond)
    def transform(self, data):
        raw, xml = data

        serie = remove_period(raw.serie)
        if serie:
            field = ET.Element('field')
            field.text = raw.serie
            field.set('name', 'cit_serie')

            xml.find('.').append(field)

        return data


class Source(plumber.Pipe):

    def precond(data):

        raw, xml = data

        if not raw.source:
            raise plumber.UnmetPrecondition()

    @plumber.precondition(precond)
    def transform(self, data):
        raw, xml = data

        source = remove_period(raw.source)
        if source:
            field = ET.Element('field')
            field.text = source

            if raw.publication_type == 'article':
                field.set('name', 'cit_journal_title')
            else:
                field.set('name', 'cit_source')

            xml.find('.').append(field)

            if raw.publication_type == 'book':
                field = ET.Element('field')
                field.text = source
                field.set('name', 'ti')

                xml.find('.').append(field)

        return data


class Title(plumber.Pipe):

    def transform(self, data):
        raw, xml = data

        title = remove_period(raw.title())
        if title:
            field = ET.Element('field')
            field.text = title
            field.set('name', 'ti')

            xml.find('.').append(field)

        return data


class Volume(plumber.Pipe):

    def precond(data):

        raw, xml = data

        if not raw.volume:
            raise plumber.UnmetPrecondition()

    @plumber.precondition(precond)
    def transform(self, data):
        raw, xml = data

        volume = remove_period(raw.volume)
        if volume:
            field = ET.Element('field')
            field.text = volume
            field.set('name', 'volume')

            xml.find('.').append(field)

        return data
