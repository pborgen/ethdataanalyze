import logging
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import dask.dataframe as dd
import dask
import shutil
import os
from dask.diagnostics import ProgressBar
from pathlib import Path
from ethdataanalyze.management.helper.csv_metadata_extractor import CsvMetadataExtractor
from ethdataanalyze.configuration.settings import SettingsFactory


class DaskHelper:
    def __init__(self):
        self.__logger = logging.getLogger(__class__.__name__)
        self.__settings = self.__settings = SettingsFactory.get()

    def set_config(self):
        
        temp_directory = self.__settings.dask()['temp_directory']

        if not Path(temp_directory).is_dir():
            Path(temp_directory).mkdir(exist_ok=True, parents=True)

        dask.config.set({'temporary_directory': temp_directory})

    def delete_dask_temp_directory(self):
        temp_directory = self.__settings.dask()['temp_directory']

        if Path(temp_directory).is_dir():
            shutil.rmtree(temp_directory, ignore_errors=True)

    def set_data_types(self, ddf, data_element_long_term_storage):
        dtype = self.__retreive_dtypes(data_element_long_term_storage)
        return ddf.astype(dtype)

    def __retreive_dtypes(self, data_element_long_term_storage):
        dtypes = {}
        columns = data_element_long_term_storage['columns']
        if columns is not None:
            for column in columns:
                if 'type' in column:
                    dtypes[column['name']] = column['type']

        return dtypes

    def repartition(self, ddf, data_element):
        return ddf.repartition(divisions=self.get_divisions(), force=True)

    def get_divisions(self):
        self.__logger.info(f'Start - Getting the Divisions')
        min_division = 0
        max_division = 200000

        division_size = 100

        counter = min_division

        divisions = []

        while counter <= max_division:
            divisions.append(counter)
            counter = counter + division_size

        self.__logger.info(f'End - Getting the Divisions')

        return divisions

    def __get_divisions_with_group_divisions_by(self, unique_indexes_list):
        """" Create the divisions from the unique index values. This is ensure the divisions are always populates """
        self.__logger.info(f'Start - Getting the Divisions')
        min_division = 0
        max_division = 10000000

        divisions = [min_division]
        group_divisions_by = self.__settings.dask_group_divisions_by()
        counter_for_divisions = group_divisions_by

        while counter_for_divisions < len(unique_indexes_list):
            divisions.append(unique_indexes_list[counter_for_divisions])
            counter_for_divisions = counter_for_divisions + group_divisions_by

        divisions.sort()

        if len(divisions) < 2:
            divisions = [min_division, max_division]
        else:
            divisions.append(max_division)

        self.__logger.info(f'End - Getting the Divisions')
        return divisions

    def __get_divisions_by_index(self, ddf):
        return self.get_divisions(ddf.index.unique().compute().to_list())

    def set_index(self, ddf, data_element, sorted=False):
        index_name = data_element['index']['name']

        if ddf.index.name == index_name:
            ddf = ddf.reset_index(drop=False)

        self.__logger.info(f'Start - Setting the index')
        ddf = \
            ddf.set_index(
                index_name,
                divisions=self.get_divisions(),
                sorted=sorted,
                drop=True
            )
        self.__logger.info(f'End - Setting the index')

        return ddf

    def set_index_device_irn(self, ddf, data_element_long_term_storage):

        current_index_name = ddf.index.name

        if current_index_name == 'DEVICE_IRN':
            # Set the index back
            ddf = ddf.reset_index()

        drop = True

        # only drop the column if it is not a known column
        for column in data_element_long_term_storage['columns']:
            name = column['name']
            if name == current_index_name:
                drop = False
                break

        return \
            ddf.set_index(
                'DEVICE_IRN',
                npartitions=data_element_long_term_storage['npartitions'],
                sorted=False,
                drop=drop
            )

    @staticmethod
    def csv_to_dataframe(csv_full_path, dtype=None, date_columns=None, usecols=None, date_parser=None, separator=','):
        df = None

        if dtype:
            if date_columns:
                df = dd.read_csv(filepath_or_buffer=csv_full_path, parse_dates=date_columns, header=0, engine='python',
                                 dtype=dtype, usecols=usecols, date_parser=date_parser, sep=separator)
            else:
                df = dd.read_csv(filepath_or_buffer=csv_full_path, parse_dates=True, header=0, engine='python',
                                 dtype=dtype, usecols=usecols, sep=separator)

        else:
            if date_columns:
                df = \
                    dd.read_csv(
                        filepath_or_buffer=csv_full_path,
                        parse_dates=date_columns,
                        header=0,
                        engine='python',
                        usecols=usecols,
                        date_parser=date_parser,
                        sep=separator
                    )
            else:
                df = dd.read_csv(
                    filepath_or_buffer=csv_full_path,
                    parse_dates=True,
                    header=0,
                    engine='python',
                    usecols=usecols,
                    sep=separator
                )

        return df

    def csv_to_parquet(self, csv_full_path, full_path_to_parquet_file, columns=None, date_format=None, separator=','):
        is_large_csv = CsvMetadataExtractor.is_large_csv(csv_full_path)

        date_columns = []
        dtypes = {}
        my_columns = None

        if columns is not None:
            my_columns = []
            for column in columns:
                my_columns.append(column['name'])
                if 'is_date' in column:
                    if column['is_date']:
                        date_columns.append(column['name'])
                if 'type' in column:
                    dtypes[column['name']] = column['type']

        date_parser = None

        if date_format is not None:
            date_parser = lambda x: dd.datetime.strptime(x, date_format)

        if is_large_csv:
            self.__logger.info(f'Converting {csv_full_path} to parquet. This is a large file and will take some time')
            chunksize = 100000  # this is the number of lines

            pqwriter = None

            for i, df in enumerate(pd.read_csv(csv_full_path, sep=separator, chunksize=chunksize, date_parser=date_parser, parse_dates=date_columns, dtype=dtypes, usecols=my_columns)):
                df = df[my_columns]
                table = pa.Table.from_pandas(df)
                # for the first chunk of records
                if i == 0:
                    # create a parquet write object giving it an output file
                    pqwriter = pq.ParquetWriter(full_path_to_parquet_file, table.schema)
                pqwriter.write_table(table)

            # close the parquet writer
            if pqwriter:
                pqwriter.close()
        else:
            df = \
                DaskHelper.csv_to_dataframe(
                    csv_full_path=csv_full_path,
                    dtype=dtypes,
                    date_columns=date_columns,
                    usecols=my_columns,
                    date_parser=date_parser,
                    separator=separator)[my_columns]
            df.to_parquet(
                fname=full_path_to_parquet_file,
                compression=self.__settings.dask_compression(),
                engine=self.__settings.dask_engine()
            )

    def read_parquet(self, path, index=None):
        return dd.read_parquet(
            path=path,
            engine=self.__settings.dask_engine(),
            index=index
        )

    def to_parquet(self, ddf, long_term_storage_directory, write_index=True):

        if not os.path.isfile(long_term_storage_directory):
            path = Path(long_term_storage_directory)
            path.mkdir(parents=True, exist_ok=True)

        self.__logger.info(f'Start - write to parquet. {long_term_storage_directory}')
        ddf.to_parquet(
            long_term_storage_directory,
            write_index=write_index,
            engine=self.__settings.dask_engine(),
            append=False,
            ignore_divisions=True,
            partition_on=None
        )
        self.__logger.info(f'End - write to parquet. {long_term_storage_directory}')

    @staticmethod
    def export_to_file(file_name_base, df, data_element, unix_time_stamp, export_parquet=True, export_csv=True):
        data_export_base_directory = \
            DaskHelper.__get_base_directory_from_data_element(data_element)
        data_export_processing_directory = f'{data_export_base_directory}/processing'

        generated_file = f'{data_export_processing_directory}/{file_name_base}_{unix_time_stamp}'

        DaskHelper.to_parquet(
            df,
            f'{generated_file}.parquet'
        )

        df.to_csv(
            f'{generated_file}.csv',
            index=False
        )

    @staticmethod
    def __get_base_directory_from_data_element(data_element):
        data_export_relative_directory = data_element['data_export_relative_directory']

        return SettingsFactory.get().data_export_base_dir() + '/' + data_export_relative_directory

