

class Const:
    __instance = None
    __base_configuration_directory = None
    __bla = None

    def __init__(self):
        if Const.__instance is not None:
            raise Exception("Please use the get_instance method to retrieve this class")
        else:
            Const.__instance = self

    @staticmethod
    def get_instance():
        """ Get the instance. """
        if Const.__instance is None:
            Const()
        return Const.__instance

    @staticmethod
    def set_base_configuration_directory(base_configuration_directory):
        """ Set the base_directory. """
        Const.__base_configuration_directory = base_configuration_directory

    @staticmethod
    def get_base_configuration_directory():
        """ Get the base_directory. """
        return Const.__base_configuration_directory

