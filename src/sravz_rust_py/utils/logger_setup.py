import logging
import logging.config
import os

import settings


def get_logger(name):
    """
    Function to set up a logger with the specified name, log file, and log level.
    
    :param name: Name of the logger.
    :param log_file: Log file path.
    :param level: Logging level.
    :return: Configured logger.
    """
    
    # Define a dictionary-based logging configuration
    LOGGING_CONFIG: dict = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'standard': {
                'format': '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s - %(message)s'
            },
        },
        'handlers': {
            'console': {
                'level': settings.LOG_LEVEL,
                'class': 'logging.StreamHandler',
                'formatter': 'standard',
            },
        },
        'loggers': {
            name: {
                'handlers': ['console'],
                'level': settings.LOG_LEVEL,
                'propagate': False
            },
        }
    }

    # Ensure the log directory exists   
    if settings.LOG_FILE:
        os.makedirs(os.path.dirname(settings.LOG_FILE), exist_ok=True)
        LOGGING_CONFIG['handlers'].update({'file': {
                'level': settings.LOG_LEVEL,
                'class': 'logging.FileHandler',
                'formatter': 'standard',
                'filename': settings.LOG_FILE,
                'mode': 'a',
            }})
        LOGGING_CONFIG['loggers']['name']['handlers'].append('file')
        
    # Configure logging
    logging.config.dictConfig(LOGGING_CONFIG)

    # Get the logger
    logger = logging.getLogger(name)
    return logger

if __name__ == "__main__":
    logger = get_logger(__name__)
    
    def example_function():
        logger.info("This is an info message from example_function.")
        logger.debug("This is a debug message from example_function.")
        logger.warning("This is a warning message from example_function.")
    
    def another_function():
        logger.error("This is an error message from another_function.")
        logger.critical("This is a critical message from another_function.")
    
    example_function()
    another_function()