import logging
import pathlib
import os

class FileNameUpdater:
    def __init__(self):
        self.__logger = logging.getLogger(__class__.__name__)
    
    def prependToFileNamesInDirectory(self, fullpathDirectory: str, whatToPrepend: str):
        filesInDirectory = os.listdir(fullpathDirectory)

        for filename in filesInDirectory:
            current_file_full_path = fullpathDirectory + '/' + filename
            os.rename(current_file_full_path, current_file_full_path + whatToPrepend)


