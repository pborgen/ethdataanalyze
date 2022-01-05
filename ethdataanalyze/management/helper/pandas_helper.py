import logging
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from ethdataanalyze.management.helper.csv_metadata_extractor import CsvMetadataExtractor
from ethdataanalyze.configuration.settings import SettingsFactory


class PandasHelper:
    def __init__(self):
        self.__logger = logging.getLogger(__class__.__name__)
        self.__settings = SettingsFactory.get()

    @staticmethod
    def csv_to_dataframe(csv_full_path, dtype=None, date_columns=None, usecols=None, date_parser=None, separator=','):
        df = None

        if dtype:
            if date_columns:
                df = pd.read_csv(filepath_or_buffer=csv_full_path, parse_dates=date_columns, header=0, engine='python',
                                 dtype=dtype, usecols=usecols, date_parser=date_parser, sep=separator)
            else:
                df = pd.read_csv(filepath_or_buffer=csv_full_path, parse_dates=True, header=0, engine='python',
                                 dtype=dtype, usecols=usecols, sep=separator)

        else:
            if date_columns:
                df = \
                    pd.read_csv(
                        filepath_or_buffer=csv_full_path,
                        parse_dates=date_columns,
                        header=0,
                        engine='python',
                        usecols=usecols,
                        date_parser=date_parser,
                        sep=separator
                    )
            else:
                df = pd.read_csv(
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
            date_parser = lambda x: datetime.strptime(x, date_format)

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
                    pqwriter = \
                        pq.ParquetWriter(
                            full_path_to_parquet_file,
                            table.schema,
                            compression=self.__settings.dask_compression()
                        )
                pqwriter.write_table(table)

            # close the parquet writer
            if pqwriter:
                pqwriter.close()
        else:
            df = \
                PandasHelper.csv_to_dataframe(
                    csv_full_path=csv_full_path,
                    dtype=dtypes,
                    date_columns=date_columns, usecols=my_columns, date_parser=date_parser)[my_columns]
            df.to_parquet(
                path=full_path_to_parquet_file,
                compression=self.__settings.dask_compression(),
                engine=self.__settings.dask_engine()
            )

    def read_parquet(self, full_path_to_parquet_file, engine='pyarrow'):
        self.__logger.info(f'About to read {full_path_to_parquet_file} -- {engine}')
        return \
            pd.read_parquet(
                path=full_path_to_parquet_file,
                engine=engine
            )


    def to_parquet(self, df, full_path_to_parquet_file):
        df.to_parquet(
            path=full_path_to_parquet_file,
            compression=self.__settings.dask_compression(),
            engine=self.__settings.dask_engine()
        )

    def export_to_file(self, file_name_base, df, data_element, unix_time_stamp, export_parquet=True, export_csv=True):
        data_export_base_directory = \
            PandasHelper.__get_base_directory_from_data_element(data_element)
        data_export_processing_directory = f'{data_export_base_directory}/processing'

        generated_file = f'{data_export_processing_directory}/{file_name_base}_{unix_time_stamp}'

        self.to_parquet(
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

