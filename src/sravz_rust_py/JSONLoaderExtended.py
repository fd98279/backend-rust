'''
    JSONLoaderExtended
'''
from pathlib import Path
from typing import Callable, Dict, Iterator, Optional, Union
import tempfile

from langchain_community.document_loaders import JSONLoader
from langchain_core.documents import Document


class JSONLoaderExtended(JSONLoader):
    '''
        Extended JSONLoader to accept JSON Data
    '''

    def __init__(
        self,
        json_data: str,
        jq_schema: str,
        file_path: Union[str, Path] = None,
        content_key: Optional[str] = None,
        is_content_key_jq_parsable: Optional[bool] = False,
        metadata_func: Optional[Callable[[Dict, Dict], Dict]] = None,
        text_content: bool = True,
        json_lines: bool = False,
    ):
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            super().__init__(temp_file.name,
                             jq_schema,
                             content_key,
                             is_content_key_jq_parsable,
                             metadata_func,
                             text_content,
                             json_lines)
            self.json_data = json_data

    def lazy_load(self) -> Iterator[Document]:
        """Load and return documents from the JSON file."""
        index = 0
        for doc in self._parse(self.json_data, index):
            yield doc
            index += 1
