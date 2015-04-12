from lxml import etree
from mapper.utils.base import BaseMapperBackend
from mapper.utils.base import BaseModelParser


class XmlModelParser(BaseModelParser):

    def get_item_source(self, source):
        pass



class XmlMapperBackend(BaseMapperBackend):

    def load_source(self, file_name):
        return etree.parse(file_name)
