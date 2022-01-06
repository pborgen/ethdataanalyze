import hashlib
import logging
import os


class Hasher:
    def __init__(self):
        self.__logger = logging.getLogger(__class__.__name__)

    @staticmethod
    def hash_all_files_in_directory(directory_full_path):
        files_to_process = \
            [f for f in os.listdir(directory_full_path) if os.path.isfile(os.path.join(directory_full_path, f))]

        for the_file in files_to_process:
            the_file_path_no_extension = os.path.splitext(the_file)[0]
            Hasher.hash_file(
                directory_full_path + '/' + the_file, directory_full_path + '/' + the_file_path_no_extension + '.sha1'
            )

    @staticmethod
    def hash_file(fullpath_file_to_hash, full_path_where_to_store_hash_file):
        block_size = 65536
        hasher = hashlib.sha1()

        with open(fullpath_file_to_hash, 'rb') as file:
            buf = file.read(block_size)
            while len(buf) > 0:
                hasher.update(buf)
                buf = file.read(block_size)

        hash_as_string = hasher.hexdigest()

        with open(full_path_where_to_store_hash_file, 'w') as hashFile:
            hashFile.write(hash_as_string)
