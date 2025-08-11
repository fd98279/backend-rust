'''
    main.py - entry point of the rust-backend-py
'''
import logging
from pathlib import Path

import pandas as pd

from PyMessage import PyMessage

# Create logger
logger = logging.getLogger('MyLogger')
logger.setLevel(logging.INFO)

# Create file handler
file_handler = logging.FileHandler('/tmp/app.log')
file_handler.setLevel(logging.INFO)

# Create console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

# Define log format
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# Add handlers to the logger
logger.addHandler(file_handler)
logger.addHandler(console_handler)


def run(py_message: PyMessage) -> PyMessage:
    '''
        Run function for the given py_message.message_id
    '''
    logger.info("Message recevied: %s", py_message)
    message_id = float(py_message.message_id)
    try:
        if message_id == 1.0:
            logger.info("Processing message key %s", py_message.message_id)
            file_path = f"/tmp/data/{py_message.key}.parquet"
            path = Path(file_path)
            if path.is_file():
                logger.info("File %s found. Processing...", file_path)
                df = pd.read_parquet(file_path)
                import leveraged_funds
                py_message.output = leveraged_funds.process(
                    df, py_message.key)
            else:
                logger.info("The file %s does not exist.", file_path)
        elif message_id == 2.0:
            import agent_supervisor
            py_message.output = agent_supervisor.query(
                py_message.sravz_ids.split(","),
                py_message.json_keys.split(","),
                py_message.llm_query)
        elif message_id == 3.0:
            import earnings
            file_path = f"/tmp/data/{py_message.key}.png"
            logger.info("Processing file path and id: %s - file_path %s",
                        py_message, file_path)
            py_message.output = earnings.main(py_message.sravz_ids,
                                              py_message.df_parquet_file_path, file_path)
        else:
            logger.error("Message ID did not match any ID: %s",
                         py_message)
    except Exception as e:  # pylint: disable=broad-except
        logger.exception("Error occurred: %s", e)

    return py_message
