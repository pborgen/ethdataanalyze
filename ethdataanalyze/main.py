import logging
from ethdataanalyze.configuration.Const import Const
from ethdataanalyze.configuration.logging_config import LoggingConfig
from ethdataanalyze.configuration.settings import SettingsFactory
from ethdataanalyze.management.helper.dask_helper import DaskHelper
from ethdataanalyze.management.data_processor.data_processor_engine import DataProcessorEngine
from ethdataanalyze.management.helper.filename_updater import FileNameUpdater
from ethdataanalyze.management.helper.zip_helper import ZipHelper

class Main:
    def __init__(self):
        propertiesFilePath = '/home/paul/dev/code/ethdataanalyze/ethdataanalyze/configuration'
        
        Const.get_instance().set_base_configuration_directory(propertiesFilePath)

        settings = SettingsFactory.get()
        print('Setting have been loaded')

        LoggingConfig(settings).config()
        self.__logger = logging.getLogger(__class__.__name__)
        self.__logger.info('Logger has been loaded')

    def zipDirectory(self, source);
        zipHelper = ZipHelper();
        zipHelper.zip_all_files_in_directory(source, source + '/zip');

    def prependToFileNamesInDirectory(self, fullpathDirectory, whatToPrepend):
        fileNameUpdater = FileNameUpdater();
        
        fileNameUpdater.prependToFileNamesInDirectory(fullpathDirectory, whatToPrepend);
        

    def runDataProcessorEngine(self):

        self.__logger.info('START -- Datacrunch')
        DaskHelper().delete_dask_temp_directory()
        DaskHelper().set_config()

        DataProcessorEngine().process()
        self.__logger.info('STOP -- Datacrunch')