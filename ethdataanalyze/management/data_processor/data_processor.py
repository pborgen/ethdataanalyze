import abc
import os
import shutil
import zipfile
import time
from pathlib import Path
import csv
import logging
import concurrent
from ethdataanalyze.management.crypto.hasher import Hasher
from ethdataanalyze.management.helper.dask_helper import DaskHelper
from ethdataanalyze.management.data_processor.flow_interrupt_exception import FlowInterruptException
from ethdataanalyze.configuration.settings import SettingsFactory
from ethdataanalyze.management.helper.pandas_helper import PandasHelper
from ethdataanalyze.management.data_processor.data_processor_helper import DataProcessorHelper


class DataProcessorInterface:

    def __init__(self):
        self.settings = SettingsFactory.get()
        self.data_export_base_dir = self.settings.data_export_base_dir()
        self.max_workers = self.settings.max_workers()
        self.__dask_helper = DaskHelper()

    @classmethod
    def __subclasshook__(cls, subclass):
        return (hasattr(subclass, 'process') and
                callable(subclass.process) or
                NotImplemented)

    @abc.abstractmethod
    def process(self, data_element) -> None:
        raise NotImplementedError


class CheckIfRawDirectoryIsEmptyDataProcessor(DataProcessorInterface):
    def __init__(self):
        self.__logger = logging.getLogger(__class__.__name__)
        super().__init__()

    def process(self, data_element):

        base_directory = DataProcessorHelper().get_base_directory_from_data_element(data_element)

        raw_dir = base_directory + '/raw'

        skip_flow = False

        if not os.path.exists(raw_dir):
            skip_flow = True
        else:
            files = \
                [f for f in os.listdir(raw_dir) if os.path.isfile(os.path.join(raw_dir, f))]

            if len(files) == 0:
                skip_flow = True
        if skip_flow:
            raise FlowInterruptException(
                f'No files to process in {raw_dir}. Skipping the processing of this directory.'
            )


class CopyFileToRawDataProcessor(DataProcessorInterface):
    def __init__(self):
        self.__logger = logging.getLogger(__class__.__name__)
        super().__init__()

    def process(self, data_element):
        data_type = data_element['type']

        if data_type != 'file':
            raise FlowInterruptException(f'Invalid type {type} for {data_element}')

        file_path = data_element['file_path']
        file_name = os.path.basename(file_path)

        if not os.path.isfile(file_path):
            raise FlowInterruptException(f'File {file_path} does not exists for {data_element}')

        base_directory = DataProcessorHelper().get_base_directory_from_data_element(data_element)
        os.makedirs(base_directory, exist_ok=True)

        raw_dir = Path(base_directory, 'raw')
        raw_dir.mkdir(exist_ok=True)

        start_of_table_export_unix_time_stamp = int(time.time())

        shutil.copyfile(file_path, f'{str(raw_dir)}/{start_of_table_export_unix_time_stamp}_{file_name}')


class DeleteProcessingDirectoryDataProcessor(DataProcessorInterface):
    def __init__(self):
        self.__logger = logging.getLogger(__class__.__name__)
        super().__init__()

    def process(self, data_element):
        base_directory = DataProcessorHelper().get_base_directory_from_data_element(data_element)

        processing_dir = base_directory + '/processing'

        if os.path.isdir(processing_dir):
            shutil.rmtree(processing_dir)


class CleanProcessingDirectoryDataProcessor(DataProcessorInterface):
    def __init__(self):
        self.__logger = logging.getLogger(__class__.__name__)
        super().__init__()

    def process(self, data_element):
        DeleteProcessingDirectoryDataProcessor().process(data_element)

        processing_directory = DataProcessorHelper().get_processing_directory_from_data_element(data_element)

        Path(processing_directory).mkdir(parents=True, exist_ok=True)


class SpecifyColumnsDataProcessor(DataProcessorInterface):
    def __init__(self):
        self.__logger = logging.getLogger(__class__.__name__)
        super().__init__()

    def process(self, data_element):
        base_directory = DataProcessorHelper().get_base_directory_from_data_element(data_element)

        processing_dir = base_directory + '/processing'

        files_to_process = \
            [f for f in os.listdir(processing_dir) if os.path.isfile(os.path.join(processing_dir, f))]

        files_to_process = list(filter(lambda x: x.endswith('.csv'), files_to_process))

        column_names_to_keep = []

        if 'columns' in data_element:
            for column in data_element['columns']:
                column_names_to_keep.append(column['name'])

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            for the_file in files_to_process:
                executor.submit(
                    self.__concurrent(the_file, column_names_to_keep, data_element)
                )
        executor.shutdown(wait=True)

    def __concurrent(self, the_file, column_names_to_keep, data_element):
        if the_file.endswith(".csv"):
            base_directory = DataProcessorHelper().get_base_directory_from_data_element(data_element)

            delimiter = ','

            if 'csv_separator' in data_element:
                delimiter = data_element['csv_separator']

            processing_dir = base_directory + '/processing'
            # Column indexes to be removed (starts at 0)
            column_indexes_to_remove = []
            column_indexes_to_keep = []
            all_column_indexes = []

            input_file_path = f'{processing_dir}/{the_file}'

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


class MakeProcessedDirectoryDataProcessor(DataProcessorInterface):
    def __init__(self):
        self.__logger = logging.getLogger(__class__.__name__)
        super().__init__()

    def process(self, data_element):
        base_directory = DataProcessorHelper().get_base_directory_from_data_element(data_element)

        processed_dir = Path(base_directory, 'processed')
        processed_dir.mkdir(exist_ok=True)


class CsvConvertToParquetDataProcessor(DataProcessorInterface):
    def __init__(self):
        self.__logger = logging.getLogger(__class__.__name__)
        self.__pandas_helper = PandasHelper()
        self.__dask_helper = DaskHelper()
        self.__settings = SettingsFactory.get()
        super().__init__()

    def process(self, data_element):
        base_directory = DataProcessorHelper().get_base_directory_from_data_element(data_element)
        processing_dir = base_directory + '/processing'

        files_to_process = \
            [f for f in os.listdir(processing_dir) if os.path.isfile(os.path.join(processing_dir, f))]

        files_to_process = list(filter(lambda x: x.endswith('.csv'), files_to_process))

        columns = None
        if 'columns' in data_element:
            columns = data_element['columns']

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            for the_file in files_to_process:
                executor.submit(
                    self.__concurrent(the_file, columns, data_element)
                )
        executor.shutdown(wait=True)

    def __concurrent(self, the_file, columns, data_element):
        base_directory = DataProcessorHelper().get_base_directory_from_data_element(data_element)
        processing_dir = base_directory + '/processing'

        if the_file.endswith(".csv"):
            parquet_file_name = os.path.splitext(the_file)[0] + '.parquet'
            parquet_file_name = parquet_file_name.replace('_csv_', '_parquet_')
            parquet_file_fullpath = processing_dir + '/' + parquet_file_name
            the_file_fullpath = processing_dir + '/' + the_file

            csv_separator = ','

            if 'csv_separator' in data_element:
                csv_separator = data_element['csv_separator']

            date_format = None

            if 'date_format' in data_element:
                date_format = data_element['date_format']

            self.__logger.info(f'Converting {the_file_fullpath} to parquet')
            self.__pandas_helper.csv_to_parquet(
                csv_full_path=the_file_fullpath,
                full_path_to_parquet_file=parquet_file_fullpath,
                columns=columns,
                date_format=date_format,
                separator=csv_separator
            )
            self.__logger.info(f'Converted {the_file_fullpath} to {parquet_file_fullpath}')

            if 'orderby' in data_element:
                orderby = data_element['orderby']
                df = self.__pandas_helper.read_parquet(parquet_file_fullpath, engine=self.__settings.dask_engine())
                df = df.sort_values(by=orderby, ascending=True)
                self.__pandas_helper.to_parquet(df, parquet_file_fullpath)


class MoveZipFileToProcessedDataProcessor(DataProcessorInterface):
    def __init__(self):
        self.__logger = logging.getLogger(__class__.__name__)
        super().__init__()

    def process(self, data_element):
        base_directory = DataProcessorHelper().get_base_directory_from_data_element(data_element)
        processing_dir = base_directory + '/processing'
        processed_dir = base_directory + '/processed'

        zip_files = [f for f in os.listdir(processing_dir) if f.endswith('.zip')]

        for the_file in zip_files:
            shutil.move(processing_dir + '/' + the_file, processed_dir)


class CopyFromDirectoryToProcessingDataProcessor(DataProcessorInterface):
    def __init__(self):
        self.__logger = logging.getLogger(__class__.__name__)
        super().__init__()

    def process(self, data_element):
        base_directory = DataProcessorHelper().get_base_directory_from_data_element(data_element)
        from_directory = data_element['dir_path']
        to_dir = base_directory + '/processing'

        shutil.copytree(from_directory, to_dir)


class MoveFromRawToProcessingDataProcessor(DataProcessorInterface):
    def __init__(self):
        self.__logger = logging.getLogger(__class__.__name__)
        super().__init__()

    def process(self, data_element):
        base_directory = DataProcessorHelper().get_base_directory_from_data_element(data_element)
        raw_dir = base_directory + '/raw'
        processing_dir = base_directory + '/processing'

        if os.path.exists(processing_dir):
            # deletes the folder also
            shutil.rmtree(processing_dir)

        Path(processing_dir).mkdir()

        src_files = os.listdir(raw_dir)
        for file_name in src_files:
            shutil.copy(raw_dir + '/' + file_name, processing_dir)


class HashDataProcessor(DataProcessorInterface):
    def __init__(self):
        self.__logger = logging.getLogger(__class__.__name__)
        self.__hasher = Hasher()
        super().__init__()

    def process(self, data_element):
        base_directory = DataProcessorHelper().get_base_directory_from_data_element(data_element)
        processing_dir = base_directory + '/processing'

        self.__hasher.hash_all_files_in_directory(processing_dir)


class ZipDataProcessor(DataProcessorInterface):
    def __init__(self):
        self.__logger = logging.getLogger(__class__.__name__)
        super().__init__()

    def process(self, data_element):
        base_directory = DataProcessorHelper().get_base_directory_from_data_element(data_element)

        processing_dir = base_directory + '/processing'
        base_name = os.path.basename(os.path.normpath(base_directory))

        # get the file with the highest timestamp
        files_to_process = \
            [f for f in os.listdir(processing_dir) if os.path.isfile(os.path.join(processing_dir, f))]
        timestamp_list = []

        for the_file in files_to_process:
            index_of_underscore = the_file.index('_')
            timestamp = int(the_file[0:index_of_underscore])
            timestamp_list.append(timestamp)

        #get the largest timestamp
        timestamp_list = sorted(timestamp_list)
        largest_timestamp = timestamp_list[-1]

        zip_file_name = str(largest_timestamp) + '_' + base_name + '.zip'
        zip_file_fullpath = processing_dir + '/' + zip_file_name

        self.__logger.info(f'About to create zip file for directory {processing_dir}')
        #zip the processing directory content
        with zipfile.ZipFile(zip_file_fullpath, 'w') as my_zip:
            for the_file in files_to_process:
                file_to_add_to_zip_fullpath = processing_dir + '/' + the_file
                my_zip.write(
                    file_to_add_to_zip_fullpath, os.path.basename(file_to_add_to_zip_fullpath), compress_type=zipfile.ZIP_BZIP2
                )
        self.__logger.info(f'Zip file creation complete')
