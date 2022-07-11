""""This module used to convert the xml data into json file"""
import os
import xmltodict
import json


class XmlDataToJson:
    """This class is used to convert the xml data into json file"""

    def __init__(self) -> None:
        """This is the init method of the class XmlDataToJson"""
        pass

    def convert_xml_to_json(self,               xml_data):
        """This method will convert the the give xml data into json file"""
        dict_data = xmltodict.parse(xml_data)
        json_data = json.dumps(dict_data)
        with open("data.json", "w") as json_file:
            json_file.write(json_data)
            json_file.close()


def main():
    """This the main function for this module"""
    xml_json = XmlDataToJson()
    xml_data = """
    <EthicalHacker>
    <ThreatAnalyst>
        <ID> TA01 </ID>
        <Name> Karlos Ray </Name>
        <Rating> 4.6 </Rating>
        <Dept> Intelligence Analyst Dept </Dept>
        <Available> Yes </Available>
    </ThreatAnalyst>
    <ThreatAnalyst>
        <ID> TA102 </ID>
        <Name> Sue </Name>
        <Rating> 4.0 </Rating>
        <Dept>
             <D1> Network Security </D1>
             <D2> Cloud systems </D2>
             <D3> Malware analysis </D3>
        </Dept>
        <Available> False </Available>
    </ThreatAnalyst>
</EthicalHacker>
"""
    xml_json.convert_xml_to_json(xml_data)
    
if __name__ == '__main__':
    main()




# data = xmltodict.parse(xml_data)
# # using json.dumps to convert dictionary to JSON
# json_data = json.dumps(data, indent = 3)
# print(json_data)
