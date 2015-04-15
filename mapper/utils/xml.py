from lxml import etree

from ..utils.base import BaseModelParser
from ..utils.base import BaseFieldParser
from ..utils.base import BaseMapperBackend
from ..utils.base import BaseFieldValidator
from ..utils.base import BaseManyToManyValidator
from ..utils.base import BaseManyToManyParseField


class XmlFieldValidator(BaseFieldValidator):

    @classmethod
    def get_relative_xpath(cls, query):
        return ".//{path}".format(path="/".join(query.split(cls.query_divider)))

    @classmethod
    def get_abs_xpath(cls, query):
        return '/{path}'.format(path='/'.join(query.split(cls.query_divider)))

    @classmethod
    def validate_query(cls, options):
        query = super(XmlFieldValidator, cls).validate_query(options)
        query = cls.get_relative_xpath(query)
        return query


class XmlManyToManyValidator(BaseManyToManyValidator):
    plain_validator_cls = XmlFieldValidator


class XmlFieldParser(BaseFieldParser):
    validator = XmlFieldValidator

    @classmethod
    def get_raw_value(cls, raw_data, query):
        return map(lambda x: x.text, raw_data.findall(query))


class XmlManyToManyFieldParser(BaseManyToManyParseField):

    validator = XmlManyToManyValidator
    field_parser_cls = XmlFieldParser

    @classmethod
    def get_raw_value(cls, raw_data, query):
        return map(lambda x: x.text, raw_data.findall(query))


class XmlModelParser(BaseModelParser):
    field_parser_cls = XmlFieldParser
    field_parser_m2m_cls = XmlManyToManyFieldParser

    def get_source_iterator(self, source, query):
        for raw_item in source.findall(query):
            yield raw_item


class XmlMapperBackend(BaseMapperBackend):
    parser_cls = XmlModelParser

    def load_source(self, file_name):
        return etree.parse(file_name)