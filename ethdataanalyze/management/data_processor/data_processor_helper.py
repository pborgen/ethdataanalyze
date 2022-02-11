
from pathlib import Path
import shutil
import logging
import os
import glob
import pandas as pd

from ethdataanalyze.management.helper.dask_helper import DaskHelper
from ethdataanalyze.configuration.settings import SettingsFactory


class DataProcessorHelper:
    def __init__(self):
        self.__logger = logging.getLogger(__class__.__name__)
        self.__settings = SettingsFactory.get()
        self.__data_export_base_dir = self.__settings.data_export_base_dir()
        self.__dask_helper = DaskHelper()

    def retrieve_data_element_by_element_name(self, element_name: str):
        return self.__settings.retrieve_data_element_by_element_name(element_name)

    def get_base_directory_from_data_element(self, data_element):
        data_export_relative_directory = data_element['data_export_relative_directory']
        return self.__data_export_base_dir + '/' + data_export_relative_directory

    def get_processing_directory_from_data_element(self, data_element):
        return self.get_base_directory_from_data_element(data_element) + '/processing'

    def get_ddf_by_data_element_name(self, data_element_name, read_processing_directory=False):
        data_element = \
            self.__settings.retrieve_data_element_by_element_name(
                data_element_name
            )

        return self.get_ddf_by_data_element(data_element, read_processing_directory)

    def get_ddf_by_data_element(self, data_element, read_processing_directory=False):
        directory_to_read = None
        if read_processing_directory:
            directory_to_read = self.get_processing_directory_from_data_element(data_element)
        else:
            directory_to_read = self.get_base_directory_from_data_element(data_element)

        ddf = self.__dask_helper.read_parquet(directory_to_read)

        return ddf

    def get_df_by_data_element(self, data_element):
        element_name = data_element['element_name']
        processing_directory = self.get_processing_directory_from_data_element(data_element)

        return pd.read_parquet(f'{processing_directory}/{element_name}.parquet', engine=self.__settings.dask_engine())

    def clean_processing_directory(self, data_element):
        processing_directory = self.get_processing_directory_from_data_element(data_element)

        if Path(processing_directory).is_dir():
            shutil.rmtree(processing_directory)

        Path(processing_directory).mkdir(parents=True, exist_ok=True)

    def copy_all_files_with_pattern(self, pattern: str, data_element_to_copy, to_directory: str):

        element_name = data_element_to_copy['element_name']
        from_directory = \
            self.get_processing_directory_from_data_element(data_element_to_copy)

        parquet_files_to_copy = \
            list(Path(from_directory).glob(pattern))

        if len(parquet_files_to_copy) == 0:
            self.__logger.info(
                f'No files found in {from_directory}. Skipping the copy process for {element_name}'
            )
        else:

            if not Path(to_directory).is_dir():
                Path(to_directory).mkdir(parents=True)

            for file in parquet_files_to_copy:
                file_name = os.path.basename(file)
                destination_file_path = to_directory + '/' + file_name
                shutil.copyfile(file, destination_file_path)

    def is_data_element_long_term_storage(self, data_element):
        my_list = \
            self.__settings.retrieve_pre_processing_long_term_data_storage()['list'] + \
            self.__settings.retrieve_post_processing_long_term_data_storage()['list']

        is_data_element_long_term_storage = False

        for element in my_list:
            if data_element == element:
                is_data_element_long_term_storage = True
                break

        return is_data_element_long_term_storage

    def cleanup_epcm_messages_incremental(self):
        """
        clean up epcm_messages_incremental files
        """
        on_prem_config = self.__settings.get()
        csv_export_location = on_prem_config['csv_export_location']

        self.__logger.info(f'Cleaning up incremental files here {csv_export_location}')

        data_element_to_clean_up = self.__settings.retrieve_data_element_by_element_name('epcm_messages_incremental')

        data_element_to_clean_up_directory = \
            DataProcessorHelper().get_base_directory_from_data_element(data_element_to_clean_up)

        file_pattern = '*_incremental_*'
        files_matching_pattern = glob.glob(f'{data_element_to_clean_up_directory}/processing/{file_pattern}')

        for the_file in files_matching_pattern:
            file_name = os.path.basename(the_file)

            file_to_delete = Path(f'{csv_export_location}/{file_name}')
            if file_to_delete.is_file():
                os.remove(file_to_delete)