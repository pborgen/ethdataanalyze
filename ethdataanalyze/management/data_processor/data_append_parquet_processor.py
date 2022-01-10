import pathlib
import logging
import os
import pyarrow as pa
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import concurrent.futures
import pandas as pd
import time
import dask.dataframe as dd
from ethdataanalyze.management.data_processor.data_processor_helper import DataProcessorHelper
from ethdataanalyze.management.helper.pandas_helper import PandasHelper
from ethdataanalyze.configuration.settings import SettingsFactory
from ethdataanalyze.management.helper.dask_helper import DaskHelper


class DataAppendParquetProcessor:
    def __init__(self):
        self.__logger = logging.getLogger(__class__.__name__)
        self.__settings = self.__settings = SettingsFactory.get()
        self.__data_processor_helper = DataProcessorHelper()
        self.__max_workers = self.__settings.max_workers()
        self.__dask_helper = DaskHelper()

    def process(self):
        if not self.__settings.run_data_append_process():
            self.__logger.info(f'Skipping the data append process because it is not currently enabled')
            return

        data_to_append_list = self.__settings.retrieve_data_elements_to_append()['list']

        for data_element in data_to_append_list:
            if data_element['enabled']:

                self.__data_processor_helper.clean_processing_directory(data_element)

                # this data element
                element_name = data_element['element_name']
                #index_name = data_element['index']['name']
                self.__logger.info(f'Processing {element_name}')

                data_export_directory = self.__data_processor_helper.get_base_directory_from_data_element(data_element)
                data_element_processing_directory = f'{data_export_directory}/processing'

                # Data elements that we are appending
                element_name_to_append = data_element['element_name_to_append']

                data_element_to_append = self.__settings.retrieve_data_element_by_element_name(element_name_to_append)

                data_element_to_append_directory = \
                    self.__data_processor_helper.get_base_directory_from_data_element(data_element_to_append) + '/processing'

                parquet_files_to_append = \
                    list(pathlib.Path(data_element_to_append_directory).glob(f'*.parquet'))

                if len(parquet_files_to_append) == 0:
                    self.__logger.info(
                        f'No parquet files found in {data_element_to_append_directory}. Skipping the append process.'
                    )
                    continue

                # add the base dataframe
                device_df_list = []
                ddf = None

                with concurrent.futures.ThreadPoolExecutor(max_workers=self.__max_workers) as executor:
                    log_every_n = 500
                    counter = 0
                    for file in parquet_files_to_append:
                        executor.submit(
                            device_df_list.append(PandasHelper().read_parquet(file))
                        )

                        if counter != 0 and (counter % log_every_n == 0):
                            self.__logger.info(f'Converted {counter} files to dataframes')

                            self.__logger.info(f'About to append dataframes (This can be a long running task)')

                            # trying to not have the memory expand too much with all these dataframes
                            # in this list in memory
                            temp_df = pd.concat(device_df_list, axis=0)

                            # clear our the list because it is in temp_df now
                            device_df_list = []

                            ddf_new_data = dd.from_pandas(temp_df, chunksize=1000000, sort=True)

                            # is this the first time we are wrting to this directory
                            schema = \
                                pa.schema([('block_number', pa.int32()), ('transaction_number', pa.string())])


                            if not os.listdir(data_element_processing_directory) :

                                os.makedirs(data_element_processing_directory, exist_ok=True)

                                self.__dask_helper.to_parquet(
                                    ddf_new_data,
                                    f'{data_element_processing_directory}',
                                    False, 
                                    schema
                                )
                            else:
                                ddf_long_term_storage = \
                                    self.__dask_helper.read_parquet(
                                        path=data_element_processing_directory
                                    )

                                ddf = dd.concat([ddf_long_term_storage, ddf_new_data], axis=0)

                                self.__dask_helper.to_parquet(
                                    ddf,
                                    f'{data_element_processing_directory}',
                                    False,
                                    schema
                                )

                        counter = counter + 1

                    self.__logger.info(f'Waiting for threads to stop')
                    executor.shutdown(wait=True)

                self.__logger.info(f'About to append dataframes (This can be a long running task)')
                result_df = pd.concat(device_df_list, axis=0)

                ddf_new_data = dd.from_pandas(result_df, chunksize=1000000, sort=True)

                ddf_long_term_storage = \
                    self.__dask_helper.read_parquet(
                        path=data_element_processing_directory
                    )

                ddf = dd.concat([ddf_long_term_storage, ddf_new_data], axis=0)

                self.__dask_helper.to_parquet(
                    ddf,
                    f'{data_element_processing_directory}',
                    write_index=False
                )

                self.__logger.info(f'Done appending {counter} parquet files')
            else:
                self.__logger.info(f'Skipping {data_element} because it is not currently enabled')