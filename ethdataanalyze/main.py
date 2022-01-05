import logging
from ethdataanalyze.configuration.Const import Const
from ethdataanalyze.configuration.logging_config import LoggingConfig
from ethdataanalyze.configuration.settings import SettingsFactory

class Main:
    def main(self):
        self.run()

    @staticmethod
    def run():
        propertiesFilePath = '/home/paul/dev/code/ethdataanalyze/ethdataanalyze/configuration/configuration.yaml'
        
        Const.get_instance().set_base_configuration_directory(propertiesFilePath)

        settings = SettingsFactory.get()
        print('Setting have been loaded')

        LoggingConfig(settings).config()
        logger = logging.getLogger(__class__.__name__)
        logger.info('Logger has been loaded')

        print('here')