import logging

from adsingestp.ingest_exceptions import (
    NoSchemaException,
    WrongSchemaException,
    XmlLoadException,
)
from adsingestp.parsers.base import IngestBase
from adsingestp.parsers.dubcore import DublinCoreParser

logger = logging.getLogger(__name__)


class MultiArxivParser(IngestBase):
    start_re = r"<record(?!-)[^>]*>"
    end_re = r"</record(?!-)[^>]*>"

    def parse(self, text, header=False):
        """
        Separate multi-record arXiv XML document into individual XML documents

        :param text: string, input XML text from a multi-record XML document
        :param header: boolean (default: False), set to True to preserve overall
            document header/footer for each separate record's document
        :return: list, each item is the XML of a separate arXiv document
        """
        output_chunks = []
        for chunk in self.get_chunks(text, self.start_re, self.end_re, head_foot=header):
            output_chunks.append(chunk.strip())

        return output_chunks


class ArxivParser(DublinCoreParser):
    # Dublin Core parser for arXiv

    author_collaborations_params = {
        "keywords": ["group", "team", "collaboration"],
        "remove_the": False,
        "fix_arXiv_mixed_collaboration_string": True,
    }

    def __init__(self):
        self.base_metadata = {}
        self.input_header = None
        self.input_metadata = None

    def parse(self, text):
        """
        Parse arXiv XML into standard JSON format
        :param text: string, contents of XML file
        :return: parsed file contents in JSON format
        """
        try:
            d = self.bsstrtodict(text, parser="lxml-xml")
        except Exception as err:
            raise XmlLoadException(err)

        if d.find("record"):
            self.input_header = d.find("record").find("header")
        if d.find("record") and d.find("record").find("metadata"):
            self.input_metadata = d.find("record").find("metadata").find("oai_dc:dc")

        schema_spec = self.input_metadata.get("xmlns:oai_dc", "")
        if not schema_spec:
            raise NoSchemaException("Unknown record schema.")
        elif schema_spec not in self.DUBCORE_SCHEMA:
            raise WrongSchemaException("Wrong schema.")

        self._parse_ids()
        self._parse_title()
        self._parse_author()
        self._parse_pubdate()
        self._parse_abstract()
        self._parse_keywords()

        self.base_metadata = self._entity_convert(self.base_metadata)

        output = self.format(self.base_metadata, format="OtherXML")

        return output
