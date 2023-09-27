from abc import ABC, abstractmethod
from pathlib import Path

import pandas as pd


class ETLBase(ABC):
    """
    Base class for ETL classes
    """

    def __init__(self, date_range: pd.Index, output_format: str, output_path: Path):
        self.name = self.__class__.__name__.lower()[3:]
        self.date_range = date_range
        self.output_format = output_format
        self.output_path = output_path

        # Should be set by subclasses
        self.url: str | None = None

        self.log_message = (
            f"{self.name} data for date range "
            f"{self.date_range[0]} to {self.date_range[-1]}"
        )

    @abstractmethod
    async def extract(self, *args, **kwargs):
        pass

    @abstractmethod
    async def transform(self, *args, **kwargs):
        pass

    @abstractmethod
    async def load(self, *args, **kwargs):
        pass

    async def run(self):
        """
        Runs the ETL job
        """
        await self.extract()
        await self.transform()
        await self.load()
