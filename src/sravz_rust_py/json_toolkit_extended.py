# JsonToolkitExtended
import itertools
import json
from typing import List

from langchain.tools import BaseTool, Tool
from langchain.tools.json.tool import JsonSpec
from langchain_community.agent_toolkits.json.toolkit import JsonToolkit
from langchain_core.tools import Tool

from utils import logger_setup

logger = logger_setup.get_logger(__name__)


class JsonToolkitExtended(JsonToolkit):
    """Toolkit for interacting with a JSON spec."""

    spec: JsonSpec

    def compare_json_lists(self, tool_input: str) -> str:
        tool_input_list = tool_input.split(",")
        left_list = self.spec.value(tool_input_list[0].strip())
        right_list = self.spec.value(tool_input_list[1].strip())
        print(
            f"\n\ntool_input_list: {tool_input_list}\n\nleft_list: {left_list}\n\nright_list: {right_list}")
        return list(set(left_list) - set(right_list))

    def flatten_list_of_list_of_dicts(self, tool_input: str) -> list[dict]:
        '''
            ['[{'Name': 'MICROSOFT CORP', 'Weight': '18.22%'}, 
            {'Name': 'NVIDIA CORP', 'Weight': '13.77%'}, 
            ...t
            {'Name': 'NETFLIX INC', 'Weight': '2.26%'}]']        
        '''
        logger.info(f"Input: {tool_input}")
        input_lists = json.loads(tool_input.replace("'", '"'))
        final_list = []
        # flattened_list = [item for sublist in tool_input for item in sublist)]
        # flattend_list = list(itertools.chain(*input_lists))
        # If there is a list within the list, flatten it else just return the origial list
        for item in input_lists:
            if type(item) == list:
                [final_list.append(item_) for item_ in item]
            else:
                final_list.append(item)
        logger.info(f"Output: {final_list}")
        return final_list

    def get_tools(self) -> List[BaseTool]:
        """Get the tools in the toolkit."""

        compare_lists_tool = Tool(
            name="compare_json_lists",
            func=self.compare_json_lists,
            description="""
                Compares two given JSON lists and returns the difference between the two JSON lists.
                Used this tool only if you need to find difference between two JSON list.
                Input needs to be a valid JSON.
            """
        )

        # flatten_list_of_list_of_dicts=Tool(
        #     name="flatten_list_of_list_of_dicts",
        #     func=self.flatten_list_of_list_of_dicts,
        #     description="""
        #         Purpose: Flattens list of list of dicts.
        #         Input Parameter: List of Lists of python dictionaries.
        #         Output: Returns the list of lists of dicts as a single list - flattens the list.
        #     """
        #     )

        # + [# flatten_list_of_list_of_dicts, compare_lists_tool]
        return super().get_tools()
