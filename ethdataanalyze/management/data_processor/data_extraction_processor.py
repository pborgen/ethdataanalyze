import logging
from ethdataanalyze.management.data_processor.data_processor_helper import DataProcessorHelper
from ethdataanalyze.management.helper.pandas_helper import PandasHelper
from ethdataanalyze.configuration.settings import SettingsFactory
from ethdataanalyze.management.data_processor.data_processor import MoveFromRawToProcessingDataProcessor
from ethdataanalyze.management.data_processor.data_processor  import CsvConvertToParquetDataProcessor
from ethdataanalyze.management.data_processor.data_processor  import CheckIfRawDirectoryIsEmptyDataProcessor
from ethdataanalyze.management.data_processor.data_processor  import SpecifyColumnsDataProcessor
from ethdataanalyze.management.data_processor.data_processor  import DeleteProcessingDirectoryDataProcessor
from ethdataanalyze.management.data_processor.flow_interrupt_exception import FlowInterruptException


class DataExtractionProcessor:
    def __init__(self):
        self.__logger = logging.getLogger(__class__.__name__)
        self.__settings = self.__settings = SettingsFactory.get()
        self.__pandas_helper = PandasHelper()
        self.__data_processor_helper = DataProcessorHelper()

    def process(self):
        data_elements_to_extract_list = self.__settings.retrieve_data_elements_to_extract()['list']

        for data_elements_to_extract in data_elements_to_extract_list:
            data_element_name = data_elements_to_extract['element_name_to_extract']
            data_element = self.__settings.retrieve_data_element_by_element_name(data_element_name)

            if data_elements_to_extract['enabled']:
                for data_processor in self.__flow(data_element):
                    element_name = data_element['element_name']
                    self.__logger.debug(f'{data_processor.__class__.__name__} is about to process {element_name}')
                    try:
                        data_processor.process(data_element)
                    except FlowInterruptException as e:
                        self.__logger.error(f'Processing Stopped because of {e.message}')
                        break
            else:
                element_name = data_elements_to_extract['element_name_to_extract']
                self.__logger.info(f'Skipping {element_name} because it is not currently enabled')

    @staticmethod
    def __flow(data_element_to_process):
        name = data_element_to_process['element_name']
        flow = []

        flow_common = [
            DeleteProcessingDirectoryDataProcessor(),
            CheckIfRawDirectoryIsEmptyDataProcessor(),
            MoveFromRawToProcessingDataProcessor(),
            SpecifyColumnsDataProcessor(),
            CsvConvertToParquetDataProcessor()
        ]

        type_str = data_element_to_process['type']

        if type_str == 'table' or type_str == 'join':
            flow.extend(flow_common)
        elif type_str == 'file':
            flow.extend(flow_common)
        elif type_str == 'dir':
            flow = [
                DeleteProcessingDirectoryDataProcessor(),
                CheckIfRawDirectoryIsEmptyDataProcessor(),
                MoveFromRawToProcessingDataProcessor(),
                SpecifyColumnsDataProcessor(),
                CsvConvertToParquetDataProcessor()
            ]
        else:
            raise Exception(f'Invalid type {type_str} for {name}')

        return flow
