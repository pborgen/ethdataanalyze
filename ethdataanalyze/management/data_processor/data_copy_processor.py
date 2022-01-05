import pathlib
import logging
import os
import shutil
import dask.dataframe as dd
from pathlib import Path
from ethdataanalyze.management.helper.dask_helper import DaskHelper
from ethdataanalyze.management.data_processor.data_processor_helper import DataProcessorHelper
from ethdataanalyze.management.helper.pandas_helper import PandasHelper
from ethdataanalyze.configuration.settings import SettingsFactory


class DataCopyProcessor:
    def __init__(self):
        self.__logger = logging.getLogger(__class__.__name__)
        self.__settings = self.__settings = SettingsFactory.get()
        self.__pandas_helper = PandasHelper()
        self.__data_processor_helper = DataProcessorHelper()
        self.__dask_helper = DaskHelper()

    def process(self):
        data_to_copy_list = self.__settings.retrieve_data_elements_to_copy()['list']

        for data_element in data_to_copy_list:
            element_name = data_element['name']

            self.__logger.info(f'About to process data element with name {element_name}')
            if data_element['enabled']:
                data_element_name_to_copy = data_element['data_element_name_to_copy']
                name_of_onprem_parameter_with_directory_location = \
                    data_element['name_of_onprem_parameter_with_directory_location']

                to_directory = \
                    self.__settings.get_on_prem_configuration()[name_of_onprem_parameter_with_directory_location]

                data_element_to_copy = self.__settings.retrieve_data_element_by_element_name(data_element_name_to_copy)

                if self.__data_processor_helper.is_data_element_long_term_storage(data_element_to_copy):
                    base_directory = \
                        self.__data_processor_helper.get_base_directory_from_data_element(data_element_to_copy)
                    data_export_relative_directory = data_element_to_copy['data_export_relative_directory']

                    # make sure we have files to copy
                    if Path(base_directory).is_dir():
                        parquet_files_to_copy = \
                            list(pathlib.Path(base_directory).glob(f'*.parquet'))

                        if len(parquet_files_to_copy) == 0:
                            self.__logger.info(
                                f'No parquet files found in {base_directory}. '
                                f'Skipping the copy process for {element_name}'
                            )
                            continue

                        directory_copy_location = f'{to_directory}/{data_export_relative_directory}'

                        if Path(directory_copy_location).is_dir():
                            shutil.rmtree(directory_copy_location)

                        shutil.copytree(base_directory, directory_copy_location)

                        ddf = self.__dask_helper.read_parquet(directory_copy_location)

                        csv_file_to_create_temp = directory_copy_location + '_temp.csv'
                        if Path(csv_file_to_create_temp).exists():
                            os.remove(csv_file_to_create_temp)

                        csv_file_to_create = directory_copy_location + '.csv'
                        if Path(csv_file_to_create).exists():
                            os.remove(csv_file_to_create)

                        if 'index' in data_element_to_copy:
                            my_index_name = data_element_to_copy['index']['name']

                            if ddf.index.name == my_index_name:
                                ddf = ddf.reset_index(drop=False)

                        column_names = []

                        if 'columns' in data_element_to_copy:
                            for column in data_element_to_copy['columns']:
                                column_names.append(column['name'])

                        ddf.to_csv(
                            csv_file_to_create_temp,
                            index=False,
                            single_file=True,
                            columns=column_names
                        )

                        # there is currently a bug in filesystem_spec that cause blank lines to get added
                        with open(csv_file_to_create_temp, 'r') as in_file:
                            with open(csv_file_to_create, 'w') as out_file:
                                for line in in_file:
                                    if not line.isspace():
                                        out_file.write(line)

                        self.__logger.info(f'Completed copying {directory_copy_location}')
                else:
                    self.__data_processor_helper.copy_all_files_with_pattern(
                        pattern=f'*.parquet',
                        data_element_to_copy=data_element_to_copy,
                        to_directory=to_directory
                    )
                    self.__data_processor_helper.copy_all_files_with_pattern(
                        pattern=f'*.csv',
                        data_element_to_copy=data_element_to_copy,
                        to_directory=to_directory
                    )

                self.__logger.info(f'Completed copying {element_name}')
            else:
                self.__logger.info(f'Skipping {element_name} because it is currently disabled')