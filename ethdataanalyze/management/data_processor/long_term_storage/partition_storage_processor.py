import logging
import shutil
from dask.diagnostics import ProgressBar
from pathlib import Path
from ethdataanalyze.management.data_processor.data_processor_helper import DataProcessorHelper
from ethdataanalyze.management.helper.pandas_helper import PandasHelper
from ethdataanalyze.configuration.settings import SettingsFactory
from ethdataanalyze.management.helper.dask_helper import DaskHelper


class PartitionStorageProcessor:
    def __init__(self):
        self.__logger = logging.getLogger(__class__.__name__)
        self.__settings = self.__settings = SettingsFactory.get()
        self.__pandas_helper = PandasHelper()
        self.__dask_helper = DaskHelper()
        self.__data_processor_helper = DataProcessorHelper()

    def process(self):
        elements_to_partition = self.__settings.retrieve_data_elements_to_partition()

        for data_element in elements_to_partition['list']:

            if not data_element['enabled']:
                continue

            element_name_to_partition = data_element['element_name_to_partition']
            data_element_to_partition = self.__settings.retrieve_data_element_by_element_name(element_name_to_partition)

            source_directory = \
                DataProcessorHelper().get_base_directory_from_data_element(data_element_to_partition)

            destination_directory = \
                DataProcessorHelper().get_base_directory_from_data_element(data_element)

            if not Path(destination_directory).is_dir():
                Path(destination_directory).mkdir(parents=True)
            else:
                shutil.rmtree(destination_directory)

            index_name = data_element['index']['name']

            ddf = self.__dask_helper.read_parquet(path=source_directory)
            ddf = ddf.reset_index()

            self.__logger.info(f'Setting the index to {index_name}')
            with ProgressBar():
                ddf = ddf.set_index(index_name)

            self.__logger.info(f'Writing back to disk at location {destination_directory}')
            self.__dask_helper.to_parquet(
                ddf,
                destination_directory,
                write_index=True
            )
