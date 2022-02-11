import logging
import pandas as pd
from ethdataanalyze.configuration.settings import SettingsFactory
from ethdataanalyze.management.helper.pandas_helper import PandasHelper
from ethdataanalyze.management.data_processor.data_processor_helper import DataProcessorHelper
from ethdataanalyze.management.data_processor.data_extraction_processor import DataExtractionProcessor
from ethdataanalyze.management.data_processor.analysis.data_analysis_processor import DataAnalysisProcessor
from ethdataanalyze.management.data_processor.data_package_processor import DataPackageProcessor
from ethdataanalyze.management.data_processor.data_append_parquet_processor import DataAppendParquetProcessor
from ethdataanalyze.management.data_processor.onprem_data_organizer import OnPremDataOrganizer
from ethdataanalyze.management.data_processor.data_copy_processor import DataCopyProcessor
from ethdataanalyze.management.data_processor.long_term_storage.pre_processing_long_term_storage_processor \
    import PreProcessingLongTermStorageProcessor
from ethdataanalyze.management.data_processor.long_term_storage.partition_storage_processor import PartitionStorageProcessor


class DataProcessorEngine:

    def __init__(self):
        self.__logger = logging.getLogger(__class__.__name__)
        self.__settings = SettingsFactory.get()
        self.__pandas_helper = PandasHelper()
        self.__data_processor_helper = DataProcessorHelper()
        self.__data_extraction_processor = DataExtractionProcessor()
        self.__data_append_parquet_processor = DataAppendParquetProcessor()
        self.__data_package_processor = DataPackageProcessor()
        self.__on_prem_data_organizer = OnPremDataOrganizer()
        self.__pre_processing_long_term_data_storage = PreProcessingLongTermStorageProcessor()
        self.__data_partition = PartitionStorageProcessor()
        self.__copy = DataCopyProcessor()

        # setup dask client
        pd.set_option('mode.chained_assignment', None)
        self.__dask_client = None
        # dask_config = self.__settings.dask()
        # cluster = LocalCluster(n_workers=dask_config['n_workers'],
        #                        threads_per_worker=dask_config['threads_per_worker'],
        #                        memory_limit=dask_config['memory_limit'])
        #
        # self.__dask_client = Client(cluster)
        # dask_versions = self.__dask_client.get_versions(check=True)
        # self.__logger.info(dask_versions)
        # self.__dask_client.restart()

        self.__data_analysis_processor = DataAnalysisProcessor(self.__dask_client)

    def process(self):

        if self.__settings.run_data_organizer_process():
            self.__logger.info(f'Starting the data organizer process')
            self.__on_prem_data_organizer.process()
        else:
            self.__logger.info(f'Skipping the data organizer process')

        if self.__settings.run_data_extraction_process():
            self.__logger.info(f'Starting the data export process')
            self.__data_extraction_processor.process()
        else:
            self.__logger.info(f'Skipping the data export process')

        self.__data_append_parquet_processor.process()

        if self.__settings.run_pre_processing_lorg_term_storage():
            self.__logger.info(f'Starting the pre processing long term storage process')
            self.__pre_processing_long_term_data_storage.process()
        else:
            self.__logger.info(f'Skipping the pre processing long term storage process')

        if self.__settings.run_data_analysis_process():
            self.__logger.info(f'Starting the data analysis process')
            self.__data_analysis_processor.process()
        else:
            self.__logger.info(f'Skipping the data analysis process')

        if self.__settings.run_copy():
            self.__logger.info(f'Starting the copy processing')
            self.__copy.process()
        else:
            self.__logger.info(f'Skipping the copy processing')

        if self.__dask_client is not None:
            self.__dask_client.shutdown()
