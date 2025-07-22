class PyMessage:
    '''
        PyMessage: message struct passed between python and rust
    '''

    def __init__(self, message_id: str,
                 key: str,
                 sravz_ids: str,
                 codes: str,
                 df_parquet_file_path: str,
                 json_keys: str = "",
                 llm_query: str = ""):
        self.message_id = message_id
        self.key = key
        self.sravz_ids = sravz_ids
        self.codes = codes
        self.df_parquet_file_path = df_parquet_file_path
        self.output = ""
        self.json_keys = json_keys
        self.llm_query = llm_query

    def __repr__(self):
        return f"PyMessage(message_id={self.message_id}, sravz_ids={self.sravz_ids})"
