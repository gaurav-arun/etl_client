import io
import logging
from pathlib import Path

import pandas as pd

import settings
import utils
from api import AsyncDataFetcher

from .base import ETLBase

logger = logging.getLogger(__name__)


class ETLWind(ETLBase):
    def __init__(
        self,
        date_range: pd.Index,
        output_format: str,
        output_path: Path,
    ):
        super().__init__(
            date_range=date_range, output_format=output_format, output_path=output_path
        )

        self.url = settings.WIND_URL
        self.extracted_data: list[io.StringIO] | None = None
        self.transformed_data: pd.DataFrame | None = None

    async def extract(self):
        """
        Fetches CSV data from Wind API and stores it in `extracted_data`
        """
        logger.info(f"Extracting {self.log_message}")

        adf = AsyncDataFetcher(url=self.url, date_range=self.date_range)
        self.extracted_data: list[io.StringIO] = await adf.fetch()

    async def transform(self, *args, **kwargs):
        """
        - Transforms `extracted_data` into a DataFrame
        - Sanitizes column names
        - Converts `naive_timestamp` to UTC aware timestamp
        - Ensures all dtypes are correct
        """
        logger.info(f"Transforming {self.log_message}")

        # Convert `extracted_data` to a DataFrame
        extracted_dfs = [pd.read_csv(data) for data in self.extracted_data]
        self.transformed_data: pd.DataFrame = pd.concat(
            extracted_dfs, ignore_index=True
        )

        # Rename columns by replacing spaces with underscores,
        # trimming whitespace and converting to lowercase
        self.transformed_data.columns = (
            self.transformed_data.columns.str.strip().str.replace(" ", "_").str.lower()
        )

        # Convert `naive_timestamp` to UTC aware timestamp
        self.transformed_data.naive_timestamp = pd.to_datetime(
            self.transformed_data.naive_timestamp, utc=True
        )

        # Rename `naive_timestamp` to timestamp
        self.transformed_data.rename(
            columns={"naive_timestamp": "timestamp_utc"}, inplace=True
        )

        # Cast `last_modified_utc` to datetime
        self.transformed_data.last_modified_utc = pd.to_datetime(
            self.transformed_data.last_modified_utc, utc=True
        )

    async def load(self, *args, **kwargs):
        """
        Saves `transformed_data` to disk in the specified file format.
        """
        logger.info(f"Loading {self.log_message}")

        utils.save_dataframe(
            df=self.transformed_data,
            name_prefix=self.name,
            date_range=self.date_range,
            output_format=self.output_format,
            output_path=self.output_path,
        )
