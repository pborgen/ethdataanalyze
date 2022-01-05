import logging
import csv
import os
from ethdataanalyze.configuration.settings import SettingsFactory
from ethdataanalyze.management.helper.pandas_helper import PandasHelper


class CsvToParquet:

    def __init__(self):
        self.__logger = logging.getLogger(__class__.__name__)
        self.__settings = SettingsFactory.get()
        self.__pandas_helper = PandasHelper()

    def convertDirectoryToParquet(self, directoryPath):
        pass
        