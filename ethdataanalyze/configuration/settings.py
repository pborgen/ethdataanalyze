import abc
import logging

from ethdataanalyze.configuration.Const import Const
from ruamel.yaml import YAML
import ruamel.yaml


class SettingsFactory:
    __settings = None

    def __init__(self):
        self.__logger = logging.getLogger(__class__.__name__)

    @staticmethod
    def get():
        if SettingsFactory.__settings is None:
            SettingsFactory.__settings = Settings()

        return SettingsFactory.__settings



class Settings:

    def __init__(self):

        self.__logger = logging.getLogger(__class__.__name__)
        self.__config_file_name = 'configuration.yaml'
        #self.__config_file_path = os.path.dirname(os.path.realpath(__file__)) + '/' + self.__config_file_name

        #if not os.path.exists(self.__config_file_path):
        base_configuration_directory = Const.get_instance().get_base_configuration_directory()

        if not base_configuration_directory:
            raise AttributeError(f'Could not find the base directory')

        self.__config_file_path = base_configuration_directory + '/' + self.__config_file_name

        with open(self.__config_file_path, 'r') as yaml_file:
            self.__configurationFile = ruamel.yaml.load(yaml_file, ruamel.yaml.RoundTripLoader)



    def get_config_file_name(self):
        return self.__config_file_name
    
    
    def retrieve_logging_config(self):
        config = {
            'log_files_base_dir': Const.get_instance().get_base_configuration_directory() + '/my_logs',
            'level': self.__configurationFile['log_level']
        }
        return config








