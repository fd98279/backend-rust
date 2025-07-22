from dataclasses import dataclass


@dataclass
class QueryData:
    """
    A class representing query data structure for database operations.

    This class encapsulates the components needed for constructing and executing database queries.

    Attributes:
        keys (list): List of keys or columns to be queried.
        query (str): The SQL query string.
        asset_list (list): List of assets or values related to the query.
        header (str): Optional header text for the query, defaults to empty string.
        footer (str): Optional footer text for the query, defaults to empty string.
    """
    keys: list
    query: str
    asset_list: list
    header: str = ""
    footer: str = ""
