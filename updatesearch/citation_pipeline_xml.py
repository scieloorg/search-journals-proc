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

