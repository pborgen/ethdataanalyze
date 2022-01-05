
import logging

from ethdataanalyze.management.data_processor.data_processor_helper import DataProcessorHelper
from ethdataanalyze.management.helper.pandas_helper import PandasHelper
from ethdataanalyze.configuration.settings import SettingsFactory
from ethdataanalyze.management.data_processor.data_processor import ZipDataProcessor
from ethdataanalyze.management.data_processor.data_processor import MakeProcessedDirectoryDataProcessor
from ethdataanalyze.management.data_processor.data_processor import MoveZipFileToProcessedDataProcessor
from ethdataanalyze.management.data_processor.data_processor import HashDataProcessor
from ethdataanalyze.management.data_processor.flow_interrupt_exception import FlowInterruptException


class DataPackageProcessor:
    def __init__(self):
        self.__logger = logging.getLogger(__class__.__name__)
        self.__settings = self.__settings = SettingsFactory.get()
        self.__pandas_helper = PandasHelper()
        self.__data_processor_helper = DataProcessorHelper()

    def process(self):
        data_to_package_list = self.__settings.retrieve_data_elements_to_package()['list']

        for data_element in data_to_package_list:
            element_name = data_element['element_name']
            element_name_to_package = data_element['element_name_to_package']

            self.__logger.info(f'About to package data with name {element_name}')
            if data_element['enabled']:
                element_to_package = self.__settings.retrieve_data_element_by_element_name(element_name_to_package)

                for data_processor in self.__flow():
                    element_name = data_element['element_name']
                    self.__logger.debug(f'{data_processor.__class__.__name__} is about to package {element_name}')
                    try:
                        data_processor.process(element_to_package)
                    except FlowInterruptException as e:
                        self.__logger.info(f'Processing Stopped because of {e.message}')
                        break
            else:
                self.__logger.info(f'Skipping {element_name} because it is not currently enabled')

    @staticmethod
    def __flow():

        flow = [
            HashDataProcessor(),
            ZipDataProcessor(),
            MakeProcessedDirectoryDataProcessor(),
            MoveZipFileToProcessedDataProcessor(),
        ]

        return flow

