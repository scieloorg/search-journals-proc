# coding: utf-8
from lxml import etree as ET
from field_sanitizer import FieldSanitizer as fs

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
            field = ET.Element('field')
            name = []

            if 'surname' in author:
                name.append(author['surname'])

            if 'given_names' in author:
                name.append(author['given_names'])

            fullname = ', '.join(name)
            cleaned_fullname = fs.remove_endpoint(fullname)

            if cleaned_fullname:
                field.text = cleaned_fullname
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

        for author in raw.authors:
            field = ET.Element('field')
            name = []

            if 'surname' in author:
                name.append(author['surname'])

            if 'given_names' in author:
                name.append(author['given_names'])

            fullname = ', '.join(name)
            cleaned_fullname = fs.remove_endpoint(fullname)

            if cleaned_fullname:
                field.text = cleaned_fullname
                field.set('name', 'au')
                xml.find('.').append(field)

                au_quality_level = fs.get_author_name_quality(cleaned_fullname)

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

        chapter_title = raw.chapter_title
        cleaned_chapter_title = fs.remove_endpoint(chapter_title)

        if cleaned_chapter_title:
            field = ET.Element('field')
            field.text = cleaned_chapter_title
            field.set('name', 'ti')

            xml.find('.').append(field)

            field = ET.Element('field')
            field.text = cleaned_chapter_title
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
    Adiciona no <doc> citation id do documento citante.
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

        field = ET.Element('field')
        field.text = raw.edition
        field.set('name', 'cit_edition')

        xml.find('.').append(field)

        return data


class ExternalMetaData(plumber.Pipe):
    """
    Adiciona no <doc> citation dados extras e normalizados de citação.

    :param external_metadata: dicionário com dados extras e normalizados
    :param collection_acronym: coleção do documento citante
    """

    def __init__(self, external_metadata, collection):
        self.external_metadata = external_metadata
        self.collection = collection

    def transform(self, data):
        raw, xml = data

        cit_id = raw.data['v880'][0]['_']
        cit_full_id = '{0}-{1}'.format(cit_id, self.collection)
        cit_metadata = self.external_metadata.get(cit_full_id, {'type': None})

        if cit_metadata['type'] == 'journal-article':
            was_normalized = False
            journal_titles = cit_metadata.get('container-title', [])
            first_journal_title = journal_titles[0] if len(journal_titles) else None
            if first_journal_title:
                field = ET.Element('field')
                field.text = first_journal_title
                field.set('name', 'cit_journal_title_canonical')

                xml.find('.').append(field)

                was_normalized = True

            for i in cit_metadata.get('ISSN', []):
                field = ET.Element('field')
                field.text = i
                field.set('name', 'cit_journal_issn_canonical')

                xml.find('.').append(field)

                was_normalized = True

            for i in cit_metadata.get('BC1-ISSNS', []):
                field = ET.Element('field')
                field.text = i
                field.set('name', 'cit_journal_issn_normalized')

                xml.find('.').append(field)

                was_normalized = True

            for i in cit_metadata.get('BC1-JOURNAL-TITLES', []):
                field = ET.Element('field')
                field.text = i
                field.set('name', 'cit_journal_title_normalized')

                xml.find('.').append(field)

                was_normalized = True

            if was_normalized:
                normalization_status = cit_metadata['normalization-status']

                field = ET.Element('field')
                field.text = normalization_status
                field.set('name', 'cit_normalization_status')

                xml.find('.').append(field)

        return data


class IndexNumber(plumber.Pipe):

    def transform(self, data):
        raw, xml = data

        field = ET.Element('field')
        field.text = str(raw.index_number)
        field.set('name', 'cit_index_number')

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
            cleaned_institution_name = fs.remove_endpoint(institution)

            if cleaned_institution_name:
                field = ET.Element('field')
                field.text = cleaned_institution_name
                field.set('name', 'cit_inst')

                xml.find('.').append(field)

        return data


