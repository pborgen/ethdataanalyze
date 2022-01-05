import logging
import dask.dataframe as dd
import os
import glob
from pathlib import Path
from ethdataanalyze.management.data_processor.data_processor_helper import DataProcessorHelper
from ethdataanalyze.management.helper.pandas_helper import PandasHelper
from ethdataanalyze.configuration.settings import SettingsFactory
from ethdataanalyze.management.helper.dask_helper import DaskHelper


class PostProcessingLongTermStorageProcessor:
    def __init__(self):
        self.__logger = logging.getLogger(__class__.__name__)
        self.__settings = SettingsFactory.get()
        self.__pandas_helper = PandasHelper()
        self.__data_processor_helper = DataProcessorHelper()
        self.__dask_helper = DaskHelper()

    def store_ddf(self, ddf_new_data, data_element_long_term_storage):
        """ Store to long term storage """
        element_name = data_element_long_term_storage['element_name']

        self.__logger.info(f'About to process {element_name}')

        if ddf_new_data is None:
            self.__logger.info(f'No new data to store for {element_name} Exiting.')
            return

        long_term_storage_directory = \
            DataProcessorHelper() \
            .get_base_directory_from_data_element(data_element_long_term_storage)

        if not Path(long_term_storage_directory).is_dir():
            Path(long_term_storage_directory).mkdir(parents=True)

        # are there files currently in long term storage
        long_term_storage_directory_files = os.listdir(long_term_storage_directory)  # dir is your directory path
        number_files_in_long_term_storage = len(long_term_storage_directory_files)

        ddf = None
        index_name = data_element_long_term_storage['index']['name']

        if number_files_in_long_term_storage > 0:

            ddf_long_term_storage = \
                self.__dask_helper.read_parquet(path=long_term_storage_directory, index=index_name)

            # sometimes the READ_TIME is NaN. This happens for Voltage for example
            ddf_new_data = ddf_new_data.fillna({index_name: 0})

            ddf_new_data = \
                self.__dask_helper.set_index(
                    ddf_new_data,
                    data_element_long_term_storage
                )

            ddf = dd.concat([ddf_long_term_storage, ddf_new_data], axis=0)
        else:
            ddf = ddf_new_data

        # We currently have to move the index to a a column to drop the duplicates
        unique_columns = data_element_long_term_storage['unique_columns']

        drop_index = False
        if ddf.index.name != data_element_long_term_storage['index']['name']:
            drop_index = True

        ddf = ddf.reset_index(drop=drop_index)

        self.__logger.info(f'Dropping duplicate entries')
        ddf = ddf.drop_duplicates(subset=unique_columns, keep='last')

        self.__logger.info(f'Setting the index (this is a long running task)')
        ddf = self.__dask_helper.set_index(ddf, data_element_long_term_storage)

        # sometimes the index is NaN. This happens for Voltage for example
        ddf = ddf.fillna({index_name: 0})

        # if we have the default index we do not want to write that
        write_index = False
        if ddf.index.name == index_name:
            write_index = True

        self.__dask_helper.to_parquet(
            ddf,
            long_term_storage_directory,
            write_index=write_index
        )

        self.__data_processor_helper.cleanup_epcm_messages_incremental()

    def get_long_term_storage_ddf(self, element_name):
        # Start Long term storage
        long_term_storage = \
            self.__settings.retrieve_data_element_by_element_name(element_name)

        long_term_storage_base_directory = \
            self.__data_processor_helper.get_base_directory_from_data_element(
                long_term_storage
            )

        # do we have long term storage
        ddf_long_term_storage = None

        if Path(long_term_storage_base_directory).is_dir():
            number_files = len(os.listdir(long_term_storage_base_directory))

            if number_files > 0:
                ddf_long_term_storage = \
                    self.__dask_helper.read_parquet(path=long_term_storage_base_directory)
        return ddf_long_term_storage
