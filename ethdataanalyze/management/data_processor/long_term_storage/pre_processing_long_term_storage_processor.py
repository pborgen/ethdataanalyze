import logging
import dask.dataframe as dd
import os
import numpy as np
from pathlib import Path
from ethdataanalyze.management.data_processor.data_processor_helper import DataProcessorHelper
from ethdataanalyze.management.helper.pandas_helper import PandasHelper
from ethdataanalyze.configuration.settings import SettingsFactory
from ethdataanalyze.management.helper.dask_helper import DaskHelper


class PreProcessingLongTermStorageProcessor:
    def __init__(self):
        self.__logger = logging.getLogger(__class__.__name__)
        self.__settings = SettingsFactory.get()
        self.__pandas_helper = PandasHelper()
        self.__data_processor_helper = DataProcessorHelper()
        self.__dask_helper = DaskHelper()

    def process(self):
        self.__logger.info(f'START -- PreProcessingLongTermStorageProcessor')
        long_term_storage = self.__settings.retrieve_pre_processing_long_term_data_storage()

        for data_element_long_term_storage in long_term_storage['list']:

            data_element_long_term_storage_name = data_element_long_term_storage['element_name']

            if data_element_long_term_storage['enabled']:
                self.__logger.info(f'Processing {data_element_long_term_storage_name}')
            else:
                self.__logger.info(f'Skipping {data_element_long_term_storage_name} it is not enabled')
                continue

            element_name_to_store = data_element_long_term_storage['element_name_to_store']
            data_element_to_store = self.__settings.retrieve_data_element_by_element_name(element_name_to_store)

            directory_to_add_to_long_term_storage = \
                DataProcessorHelper().get_base_directory_from_data_element(data_element_to_store)

            if not self.__data_processor_helper.is_data_element_long_term_storage(data_element_to_store):
                directory_to_add_to_long_term_storage = directory_to_add_to_long_term_storage + '/processing'

            long_term_storage_directory = \
                DataProcessorHelper().get_base_directory_from_data_element(data_element_long_term_storage)

            if not Path(long_term_storage_directory).is_dir():
                Path(long_term_storage_directory).mkdir(parents=True)

            # are there files currently in long term storage
            long_term_storage_directory_files = os.listdir(long_term_storage_directory)  # dir is your directory path
            number_files_in_long_term_storage = len(long_term_storage_directory_files)

            # are there files to put in long term storage
            if not Path(directory_to_add_to_long_term_storage).is_dir():
                self.__logger.error(
                    f'This directory does not exists {directory_to_add_to_long_term_storage}. '
                    f'This is where we look for files to put in long term storage based on the configuration. ')
                continue

            if len(os.listdir(directory_to_add_to_long_term_storage)) == 0:
                self.__logger.warning(
                    f'There were no files in {directory_to_add_to_long_term_storage} '
                    f'to add to long term storage even though the configuration expects files. ')
                continue

            ddf = None
            index_name = data_element_long_term_storage['index']['name']

            if number_files_in_long_term_storage > 0:

                ddf_long_term_storage = \
                    self.__dask_helper.read_parquet(path=long_term_storage_directory, index=index_name)

                ddf_new_data = \
                    self.__dask_helper.read_parquet(
                        path=directory_to_add_to_long_term_storage,
                        index=index_name
                    )

                # sometimes the READ_TIME is NaN. This happens for Voltage for example
                ddf_new_data = ddf_new_data.fillna({index_name: 0})

                if 'filter' in data_element_long_term_storage:
                    my_filter = data_element_long_term_storage['filter']
                    ddf_new_data = ddf_new_data.query(f'{my_filter}')

                    number_of_new_rows_to_add = ddf_new_data.shape[0].compute()
                    if number_of_new_rows_to_add == 0:
                        self.__logger.info(f'No new rows to add')
                        continue
                    else:
                        self.__logger.info(f'About to add {number_of_new_rows_to_add} rows of data')

                # Filter on date
                if 'filter_date_greater_then' in data_element_long_term_storage:
                    filter_date_greater_then = data_element_long_term_storage['filter_date_greater_then']
                    ddf_new_data = ddf_new_data[ddf_new_data.READ_TIME > np.datetime64(filter_date_greater_then)]

                self.__logger.info(f'Setting the index (this is a long running task)')

                ddf_new_data = \
                    self.__dask_helper.set_index(ddf_new_data, data_element_long_term_storage)

                self.__logger.info(f'Concat new and previous dask dataframes')
                ddf = dd.concat([ddf_long_term_storage, ddf_new_data], axis=0)

            else:
                ddf_new_data = \
                    self.__dask_helper.read_parquet(
                        directory_to_add_to_long_term_storage,
                        index=index_name
                    )

                if 'filter_date_greater_then' in data_element_long_term_storage:
                    filter_date_greater_then = data_element_long_term_storage['filter_date_greater_then']
                    ddf_new_data = ddf_new_data[ddf_new_data.READ_TIME > np.datetime64(filter_date_greater_then)]

                if 'filter' in data_element_long_term_storage:
                    my_filter = data_element_long_term_storage['filter']
                    ddf_new_data = ddf_new_data.query(f'{my_filter}')

                    if ddf_new_data.shape[0].compute() == 0:
                        continue

                ddf = ddf_new_data

            self.__logger.info(f'Dropping duplicate entries')
            if 'drop_duplicates' in data_element_long_term_storage and \
               'subset' in data_element_long_term_storage['drop_duplicates']:
                subset = data_element_long_term_storage['drop_duplicates']['subset']

                ddf = ddf.reset_index().drop_duplicates(subset=subset, keep='last')
            else:
                pass
                #ddf = ddf.drop_duplicates(keep='last')

            if ddf.index.name != index_name:
                ddf = self.__dask_helper.set_index_with_name(ddf, index_name)

            # if we have the default index we do not want to write that
            write_index = False
            if ddf.index.name == index_name:
                write_index = True

            # self.__dask_helper.to_parquet(
            #     ddf,
            #     long_term_storage_directory,
            #     write_index=write_index
            # )

            if data_element_long_term_storage['export_to_csv']:
                self.__logger.info(f'START -- Write to csv')
                self.__dask_helper.export_to_csv(
                    data_element_long_term_storage_name, 
                    ddf, 
                    data_element_long_term_storage
                );

        self.__data_processor_helper.cleanup_epcm_messages_incremental()

        self.__logger.info(f'END -- PreProcessingLongTermStorageProcessor')

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
        index_name = long_term_storage['index']['name']

        if Path(long_term_storage_base_directory).is_dir():
            ddf_long_term_storage = \
                self.__dask_helper.read_parquet(long_term_storage_base_directory, index=index_name)

        return ddf_long_term_storage


