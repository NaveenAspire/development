""""This module will used to create a logging configuration
and make a download path for download the files in local"""
import logging
import os
from logging.handlers import TimedRotatingFileHandler

parent_dir = os.path.dirname(os.getcwd())


class LoggingDownloadpath:
    """This class will contain the methods for create logger and set dwonload path"""

    def __init__(self, config_obj) -> None:
        """This is the init method for the class LoggingDownloadpath"""
        self.config = config_obj

    def set_logger(self, name):
        """This method will create the logger in the name which you passed in params"""
        log_dir = os.path.join(parent_dir, self.config["local"]["local_log"], name)
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, name + ".log")
        handler = TimedRotatingFileHandler(
            log_file, when="h", interval=1, backupCount=3
        )
        logging.basicConfig(
            datefmt="%d-%b-%y %H:%M:%S",
            format="%(asctime)s - %(levelname)s - %(filename)s - %(lineno)d - %(message)s",
            level=logging.INFO,
            handlers=[handler],
        )
        logger = logging.getLogger(name)
        # logger.addHandler(handler)
        return logger

    def set_downloadpath(self, folder_name):
        """This method will used to set downloadpath for the local"""
        path = os.path.join(
            parent_dir, self.config["local"]["local_file_path"], folder_name
        )
        os.makedirs(path, exist_ok=True)
        return path
