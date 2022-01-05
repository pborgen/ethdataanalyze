import logging
import time
import pandas as pd
import os
import numpy as np
from pathlib import Path
from datetime import date
from datetime import timedelta
import dask.dataframe as dd
from ethdataanalyze.management.data_processor.data_processor_helper import DataProcessorHelper
from ethdataanalyze.management.helper.pandas_helper import PandasHelper
from ethdataanalyze.management.helper.dask_helper import DaskHelper
from ethdataanalyze.configuration.settings import SettingsFactory

from ethdataanalyze.management.data_processor.long_term_storage.pre_processing_long_term_storage_processor \
    import PreProcessingLongTermStorageProcessor
from ethdataanalyze.management.data_processor.data_processor import CleanProcessingDirectoryDataProcessor


class DataAnalysisProcessor:

    def __init__(self, dask_client):
        self.__logger = logging.getLogger(__class__.__name__)
        self.__dask_client = dask_client

        self.__settings = self.__settings = SettingsFactory.get()
        self.__pandas_helper = PandasHelper()
        self.__dask_helper = DaskHelper()
        self.__data_processor_helper = DataProcessorHelper()
        self.__pre_processing_long_term_storage = PreProcessingLongTermStorageProcessor()
        self.__clean_processing_directory_data_processor = CleanProcessingDirectoryDataProcessor()

    def process(self):
        pass
