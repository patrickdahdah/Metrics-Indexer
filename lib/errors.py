
import sys


class Error(Exception):
    """Base class for other exceptions"""
    pass


class UserError(Exception):
    def __init__(self, name, parameters = None, message = None):
        self.name = name
        self.parameters = parameters
        self.message = message



# class DatabaseErros(Error):
#     def __init__(self, name, error,query):
#         super().__init__(name, error)
#         self.quety = query
