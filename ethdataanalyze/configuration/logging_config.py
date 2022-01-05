import logging
import os
from os import path
from logging.handlers import RotatingFileHandler


class LoggingConfig:

    def __init__(self, settings):
        self.__settings = settings

    def config(self):
        logging_conf = self.__settings.retrieve_logging_config()
        logfile_base_dir = logging_conf['log_files_base_dir']
        format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        full_path_ingester_log_file = f'{logfile_base_dir}/ethdataanalyzer.log'

        # Check if the log dir exists
        if not path.isdir(logfile_base_dir):
            os.mkdir(logfile_base_dir)

        # Check if the log file exists
        if not path.exists(full_path_ingester_log_file):
            file = open(full_path_ingester_log_file, 'w+')
            file.close()

        logging.getLogger('numba.core').setLevel(logging.ERROR)

        logging.basicConfig(
            handlers=[RotatingFileHandler(full_path_ingester_log_file, maxBytes=10000000, backupCount=30)],
            level=logging_conf['level'],
            format=format
        )

        formatter = logging.Formatter(format)

        console = logging.StreamHandler()
        console.setLevel(logging_conf['level'])
        console.setFormatter(formatter)

        logging.getLogger('').addHandler(console)








