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

