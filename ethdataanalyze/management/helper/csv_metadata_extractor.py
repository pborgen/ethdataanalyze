import logging


class CsvMetadataExtractor:
    def __init__(self):
        self.__logger = logging.getLogger(__class__.__name__)

    @staticmethod
    def get_number_lines(csv_full_path):
        with open(csv_full_path) as csv_file:
            number_of_rows = len(csv_file.readlines())
            return number_of_rows

    @staticmethod
    def is_large_csv(csv_full_path):
        number_of_lines = CsvMetadataExtractor.get_number_lines(csv_full_path)

        return number_of_lines > 500000
