"""
This module defines shared functionality for all query types.
"""
from abc import ABC, abstractmethod
from typing import Mapping


class APIQuery(ABC):
    @staticmethod
    @abstractmethod
    def from_raw_query(raw_query: Mapping, config: Mapping):
        """
        Try to create a query from an API request dict.
        """
        pass

    def __eq__(self, __o: object) -> bool:
        if isinstance(__o, self.__class__):
            return self.__dict__ == __o.__dict__
        return False

    def __ne__(self, __o: object) -> bool:
        return not self.__eq__(__o)
