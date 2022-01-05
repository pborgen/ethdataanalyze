import logging
import csv
import os
from ethdataanalyze.configuration.settings import SettingsFactory
from ethdataanalyze.management.helper.pandas_helper import PandasHelper


class CSVHelper:

    def __init__(self):
        self.__logger = logging.getLogger(__class__.__name__)
        self.__settings = SettingsFactory.get()
        self.__pandas_helper = PandasHelper()

    def remove_extra_columns(self, input_file_path, column_names_to_keep, delimiter = ','):
        if input_file_path.endswith(".csv"):
            # Column indexes to be removed (starts at 0)
            column_indexes_to_remove = []
            column_indexes_to_keep = []
            all_column_indexes = []


            self.__logger.info(f'Filtering the columns for file {input_file_path}')

            with open(input_file_path, "r") as source:
                d_reader = csv.DictReader(source, delimiter=delimiter)
                headers = d_reader.fieldnames

                # create array with all the column index's
                for x in range(0, len(headers)):
                    all_column_indexes.append(x)

                # create array with all the column index's we want to keep
                index = 0
                for header in headers:
                    if header in column_names_to_keep:
                        column_indexes_to_keep.append(index)
                    index = index + 1

                column_indexes_to_remove = list(set(all_column_indexes) - set(column_indexes_to_keep))
                column_indexes_to_remove = sorted(column_indexes_to_remove,
                                                  reverse=True)  # Reverse so we remove from the end first

            temp_output_file_path = f'{processing_dir}/temp_{the_file}'
            row_count = 0
            with open(f'{processing_dir}/{the_file}', "r") as source:
                reader = csv.reader(source, delimiter=delimiter)
                with open(temp_output_file_path, "w", newline='') as result:
                    writer = csv.writer(result)
                    for row in reader:
                        row_count += 1

                        for col_index in column_indexes_to_remove:
                            del row[col_index]
                        writer.writerow(row)

            # replace file
            os.remove(input_file_path)
            os.rename(temp_output_file_path, input_file_path)

            self.__logger.info(f'Specified columns on file {temp_output_file_path}')