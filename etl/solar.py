import logging

import pandas as pd

import settings
import utils
from api import AsyncDataFetcher

from .base import ETLBase

logger = logging.getLogger(__name__)


class ETLSolar(ETLBase):
    def __init__(self, date_range: pd.Index, output_format, output_path):
        super().__init__(
            date_range=date_range, output_format=output_format, output_path=output_path
        )

        self.url = settings.SOLAR_URL
        self.extracted_data: list[list[dict]] | None = None
        self.transformed_data: pd.DataFrame | None = None

    async def extract(self):
        """
        Fetches JSON data from Solar API and stores it in `extracted_data`
        """
        logger.info(f"Extracting {self.log_message}")

        adf = AsyncDataFetcher(url=self.url, date_range=self.date_range)
        self.extracted_data: list[list[dict]] = await adf.fetch()

    async def transform(self):
        """
        - Transforms `extracted_data` into a DataFrame
        - Sanitizes column names
        - Converts `naive_timestamp` to UTC aware timestamp
        - Ensures all dtypes are correct
        """
        logger.info(f"Transforming {self.log_message}")

        # Convert extracted_data to a DataFrame
        flattened_list: list[dict] = [
            item for sublist in self.extracted_data for item in sublist
        ]
        self.transformed_data: pd.DataFrame = pd.DataFrame(flattened_list)

        # Rename columns by replacing spaces with underscores,
        # trimming whitespace, and converting to lowercase
        self.transformed_data.columns = (
            self.transformed_data.columns.str.strip().str.replace(" ", "_").str.lower()
        )

        # Convert `naive_timestamp` to UTC aware timestamp
        self.transformed_data.naive_timestamp = pd.to_datetime(
            self.transformed_data.naive_timestamp, unit="ms", utc=True
        )

        # Rename `naive_timestamp` to `timestamp_utc`
        self.transformed_data.rename(
            columns={"naive_timestamp": "timestamp_utc"}, inplace=True
        )

        # Cast of last_modified_utc to datetime
        self.transformed_data.last_modified_utc = pd.to_datetime(
            self.transformed_data.last_modified_utc, unit="ms", utc=True
        )

    async def load(self):
        """
        Saves `transformed_data` to disk in the specified format.
        """
        logger.info(f"Loading {self.log_message}")

        utils.save_dataframe(
            df=self.transformed_data,
            name_prefix=self.name,
            date_range=self.date_range,
            output_format=self.output_format,
            output_path=self.output_path,
        )
