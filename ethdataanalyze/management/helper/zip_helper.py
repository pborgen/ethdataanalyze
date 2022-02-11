import logging
import os
import shutil
import zipfile
from ethdataanalyze.management.helper.csv_metadata_extractor import CsvMetadataExtractor
from ethdataanalyze.configuration.settings import SettingsFactory


class ZipHelper:
    def __init__(self):
        self.__logger = logging.getLogger(__class__.__name__)
        self.__settings = SettingsFactory.get()


    def zip_all_files_in_directory(self, source, destination):

        files_to_process = \
            [f for f in os.listdir(source) if os.path.isfile(os.path.join(source, f))]
       
        for the_file in files_to_process:
            the_file_no_extension = os.path.splitext(the_file)[0]
            zip_file_name = the_file_no_extension + '.zip'

            with zipfile.ZipFile(zip_file_name, 'w') as my_zip:
    
                my_zip.write(
                    the_file, compress_type=zipfile.ZIP_BZIP2
                )

        return SettingsFactory.get().data_export_base_dir() + '/' + data_export_relative_directory

