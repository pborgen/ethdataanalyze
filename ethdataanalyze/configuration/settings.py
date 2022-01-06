import abc
import logging

from ethdataanalyze.configuration.Const import Const
import ruamel.yaml
from ruamel.yaml import YAML


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

    def get_debug(self):
        return self.__configurationFile['debug']

    def data_organizer(self):
        return self.__configurationFile['data_organizer']

    def dask(self):
        return self.__configurationFile['dask']

    def dask_group_divisions_by(self):
        return self.__configurationFile['dask']['group_divisions_by']

    def dask_engine(self):
        return self.__configurationFile['dask']['engine']

    def dask_compression(self):
        return self.__configurationFile['dask']['compression']

    def max_workers(self):
        return self.__configurationFile['max_workers']

    def run_data_organizer_process(self):
        return self.__configurationFile['data_organizer']['enabled']
        
    def run_data_extraction_process(self):
        return self.__configurationFile['data_elements_to_extract']['enabled']

    def run_data_analysis_process(self):
        return self.__configurationFile['data_elements_to_analyze']['enabled']

    def run_data_append_process(self):
        return self.__configurationFile['data_elements_to_append']['enabled']

    def run_data_package_process(self):
        return self.__configurationFile['data_elements_to_package']['enabled']

    def run_pre_processing_lorg_term_storage(self):
        return self.__configurationFile['pre_processing_long_term_data_storage']['enabled']

    def run_copy(self):
        return self.__configurationFile['data_elements_to_copy']['enabled']

    def get_config_file_name(self):
        return self.__config_file_name

    def include_hash_file(self):
        return self.__configurationFile['include_hash_file']

    def base_storage_location(self):
        return self.__configurationFile['base_storage_location']

    def filewatcher_sleep_time(self):
        return self.__configurationFile['filewatcher_sleep_time']

    def retrieve_logging_config(self):
        config = {
            'log_files_base_dir': Const.get_instance().get_base_configuration_directory() + '/my_logs',
            'level': self.__configurationFile['log_level']
        }
        return config

    def retrieve_cron_info(self):
        cron = {
            'seconds_to_sleep': self.__configurationFile['cron_seconds_to_sleep']
        }
        return cron

    def retrieve_data_elements_to_extract(self):
        return self.__configurationFile['data_elements_to_extract']

    def retrieve_data_elements_to_append(self):
        return self.__configurationFile['data_elements_to_append']

    def retrieve_data_elements_to_analyze(self):
        return self.__configurationFile['data_elements_to_analyze']

    def retrieve_data_elements_to_copy(self):
        return self.__configurationFile['data_elements_to_copy']

    def retrieve_data_elements_to_package(self):
        return self.__configurationFile['data_elements_to_package']

    def retrieve_pre_processing_long_term_data_storage(self):
        return self.__configurationFile['pre_processing_long_term_data_storage']

    def retrieve_post_processing_long_term_data_storage(self):
        return self.__configurationFile['post_processing_long_term_data_storage']

    def retrieve_data_element_by_element_name(self, element_name: str):
        data_element_list = self.__configurationFile['data_elements']['list']
        data_element_to_return = None

        for data_element in data_element_list:
            if data_element['element_name'] == element_name:
                data_element_to_return = data_element
                break

        # Search the append list
        if data_element_to_return is None:
            data_elements_to_append_list = self.retrieve_data_elements_to_append()['list']

            for data_element in data_elements_to_append_list:
                if data_element['element_name'] == element_name:
                    data_element_to_return = data_element
                    break

        # Search the pre processing long term storage list
        if data_element_to_return is None:
            data_elements_to_store_list = self.retrieve_pre_processing_long_term_data_storage()['list']

            for data_element in data_elements_to_store_list:
                if data_element['element_name'] == element_name:
                    data_element_to_return = data_element
                    break

        # Search the analyze list
        if data_element_to_return is None:
            data_elements_to_analyze_list = self.retrieve_data_elements_to_analyze()['list']

            for data_element in data_elements_to_analyze_list:
                if data_element['element_name'] == element_name:
                    data_element_to_return = data_element
                    break

        if data_element_to_return is None:
            data_elements_to_analyze_list = self.retrieve_post_processing_long_term_data_storage()['list']

            for data_element in data_elements_to_analyze_list:
                if data_element['element_name'] == element_name:
                    data_element_to_return = data_element
                    break

        return data_element_to_return

    def directories_to_process(self):
        return self.__configurationFile['directories_to_process']

    def data_export_base_dir(self):
        return self.__configurationFile['data_export_base_dir']









