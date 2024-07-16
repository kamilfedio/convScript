import abc

import pandas as pd


class CleanerBaseClass(metaclass=abc.ABCMeta):
    def __init__(self, tables: list[pd.DataFrame], remove_newlines: bool, similarity_threshold: float = 0.85):
        self.tables = tables
        self.remove_newlines = remove_newlines
        self.similarity_threshold = similarity_threshold
        self.tables_with_no_title = 0

    @abc.abstractmethod
    def clean(self):
        raise NotImplementedError
