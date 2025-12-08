import json
from abc import ABC, abstractmethod
import xml.etree.ElementTree as ET

class IOutputFormatter(ABC):
    """Astract classs for formatters"""
    @abstractmethod
    def format(self, data):
        pass

class JsonFormatter(IOutputFormatter):
    """Realization of JSON output formatter"""
    def format(self, data):
        return json.dumps(data, indent=4, default=str)

class XmlFormatter(IOutputFormatter):
    """Realization of XML output formatter"""
    def format(self, data):
        root = ET.Element("report")
        for query_name, records in data.items():
            query_elem = ET.SubElement(root, query_name)
            for record in records:
                item = ET.SubElement(query_elem, "item")
                for key, value in record.items():
                    child = ET.SubElement(item, key)
                    child.text = str(value)

        ET.indent(root, space="    ", level=0)
        
        return ET.tostring(root, encoding='unicode')
