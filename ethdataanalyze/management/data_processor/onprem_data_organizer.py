import logging
import os
import shutil
import glob
import time
import zipfile
from pathlib import Path
from ethdataanalyze.management.data_processor.data_processor_helper import DataProcessorHelper
from ethdataanalyze.management.helper.pandas_helper import PandasHelper
from ethdataanalyze.configuration.settings import SettingsFactory
from ethdataanalyze.management.data_processor.flow_interrupt_exception import FlowInterruptException


class OnPremDataOrganizer:
    def __init__(self):
        self.__logger = logging.getLogger(__class__.__name__)
        self.__settings = self.__settings = SettingsFactory.get()
        self.__pandas_helper = PandasHelper()
        self.__data_processor_helper = DataProcessorHelper()

    def process(self):

        for data_processor in self.__flow():
            try:
                self.__logger.info(f'Start {data_processor.__class__.__name__}')
                data_processor.process()
                self.__logger.info(f'End {data_processor.__class__.__name__}')
            except FlowInterruptException as e:
                self.__logger.info(f'Processing Stopped because of {e.message}')
                break

        self.__logger.info('Completed organizing files')

    @staticmethod
    def __flow():

        flow = [
            DeleteOrganizeDirectory(),
            CopyFiles(),
            BackupIncrementalFiles(),
            Organize()
        ]

        return flow


class DataOrganizerInterface:
    import abc

    def __init__(self):
        self.settings = SettingsFactory.get()
        self.data_export_base_dir = self.settings.data_export_base_dir()

    @classmethod
    def __subclasshook__(cls, subclass):
        return (hasattr(subclass, 'process') and
                callable(subclass.process) or
                NotImplemented)

    @abc.abstractmethod
    def process(self) -> None:
        raise NotImplementedError


class DeleteOrganizeDirectory(DataOrganizerInterface):
    def __init__(self):
        self.__logger = logging.getLogger(__class__.__name__)
        super().__init__()

    def process(self):
        organize_processing_dir = OrganizeUtil().get_organize_processing_directory()

        if Path(organize_processing_dir).is_dir():
            shutil.rmtree(organize_processing_dir)

        Path(organize_processing_dir).mkdir(parents=True)


class CopyFiles(DataOrganizerInterface):
    def __init__(self):
        self.__logger = logging.getLogger(__class__.__name__)
        super().__init__()

    def process(self):
        on_prem_config = self.settings.get_on_prem_configuration()
        csv_export_location = on_prem_config['csv_export_location']

        # get files to copy
        files = \
            [f for f in os.listdir(csv_export_location)
             if os.path.isfile(os.path.join(csv_export_location, f))]

        processing_dir = OrganizeUtil().get_organize_processing_directory()
        processing_dir.mkdir(parents=True, exist_ok=True)

        for file_name in files:
            shutil.copyfile(f'{csv_export_location}/{file_name}', f'{str(processing_dir)}/{file_name}')

        self.__logger.info(f'Completed Copying files to {str(processing_dir)}')


class BackupIncrementalFiles(DataOrganizerInterface):
    def __init__(self):
        self.__logger = logging.getLogger(__class__.__name__)
        super().__init__()

    def process(self):
        unix_time_stamp = int(time.time())
        on_prem_config = self.settings.get_on_prem_configuration()

        if not on_prem_config['backup_incrementatal']['enabled']:
            self.__logger.info('Skipping the backup of the incremental files')
            return

        source_dir = OrganizeUtil().get_organize_processing_directory()
        destination_dir = on_prem_config['backup_incrementatal']['data_backup_dir']

        Path(destination_dir).mkdir(parents=True, exist_ok=True)

        # get files to copy
        file_pattern = '*_incremental_*'
        files_matching_pattern = glob.glob(f'{source_dir}/{file_pattern}')

        zip_file_fullpath = f'{destination_dir}/{unix_time_stamp}_incremental_files_backup.zip'

        with zipfile.ZipFile(zip_file_fullpath, 'w') as my_zip:
            for the_file in files_matching_pattern:
                file_to_add_to_zip_fullpath = the_file
                my_zip.write(
                    file_to_add_to_zip_fullpath, os.path.basename(file_to_add_to_zip_fullpath),
                    compress_type=zipfile.ZIP_BZIP2
                )

        self.__logger.info(f'Completed incremental file backup to {str(destination_dir)}')


class OrganizeUtil:
    def __init__(self):
        self.__logger = logging.getLogger(__class__.__name__)
        self.settings = SettingsFactory.get()

    def get_organize_directory(self):
        base_directory = self.settings.data_export_base_dir()

        return Path(base_directory, 'organize')

    def get_organize_processing_directory(self):
        return Path(self.get_organize_directory(), 'processing')


class Organize(DataOrganizerInterface):
    def __init__(self):
        self.__logger = logging.getLogger(__class__.__name__)
        super().__init__()

    def process(self):
        data_elements_to_extract = self.settings.retrieve_data_elements_to_extract()

        base_directory = self.settings.data_export_base_dir()
        organize_processing_dir = OrganizeUtil().get_organize_processing_directory()

        element_name_to_organize = []

        # gather the elements that we are going to organize
        for data_element in data_elements_to_extract['list']:
            if data_element['enabled']:
                element_name_to_organize.append(data_element['element_name_to_extract'])

        for data_element_to_extract in element_name_to_organize:
            data_element = self.settings.retrieve_data_element_by_element_name(data_element_to_extract)

            file_pattern = data_element['file_pattern']
            relative_directory = data_element['data_export_relative_directory']

            data_element_raw_directory = Path(base_directory, relative_directory, 'raw')

            # clear out the raw directory
            if os.path.isdir(data_element_raw_directory):
                shutil.rmtree(data_element_raw_directory)

            data_element_raw_directory.mkdir(parents=True, exist_ok=True)

            # get the files we need to organize
            files_matching_pattern = glob.glob(f'{organize_processing_dir}/{file_pattern}')

            for matched_file_full_path in files_matching_pattern:
                file_name = os.path.basename(matched_file_full_path)
                destination = f'{data_element_raw_directory}/{file_name}'
                self.__logger.debug(f'About to copy {matched_file_full_path} to {destination}')
                shutil.copyfile(matched_file_full_path, destination)
                self.__logger.info(f'Completed organizing file with name {file_name}')

