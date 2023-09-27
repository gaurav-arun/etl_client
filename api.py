import asyncio
import io
import logging
import random
from typing import Any

import httpx
import pandas as pd

import settings

logger = logging.getLogger(__name__)


class AsyncDataFetcher:
    """
    Fetches data from an API asynchronously
    """

    def __init__(
        self,
        url: str,
        date_range: pd.Index,
        backoff: int = settings.INITIAL_BACKOFF,
        max_retries: int = settings.MAX_RETRIES,
    ):
        self.url = url
        self.date_range = date_range
        self.backoff = backoff
        self.max_retries = max_retries

    async def fetch(self) -> list[Any]:
        """
        Gathers data for the specified date range asynchronously
        :return: List of futures containing the fetched data
        """
        async with httpx.AsyncClient() as client:
            tasks = [self._fetch(client, date) for date in self.date_range]
            results: list[Any] = await asyncio.gather(*tasks)

        logger.info(
            f"Finished fetching data for date range "
            f"[{self.date_range[0]} to {self.date_range[-1]}] from [{self.url}]"
        )
        return results

    async def _fetch(self, client: httpx.AsyncClient, date: str):
        """
        Fetches data for a single date from the API.

        :param client: httpx.AsyncClient
        :param date: Date in YYYY-MM-DD format
        :return: Fetched data
        """
        backoff: int = self.backoff
        retries_left: int = self.max_retries

        while True:
            url: str = self._get_url_for_date(date)
            # TODO: Handle streaming response for large CSV attachments
            response = await client.get(url)

            try:
                if response.status_code == 200 and self._is_csv_content(response):
                    logger.debug(f"Fetched CSV content for [{url}]")
                    return io.StringIO(response.text)

                elif response.status_code == 200 and self._is_json_content(response):
                    logger.debug(f"Fetched JSON content for [{url}]")
                    return response.json()

                elif response.status_code == 429:
                    backoff, retries_left = await self._handle_request_throttling(
                        url=url, backoff=backoff, retries_left=retries_left
                    )
                elif response.status_code == 403:
                    raise Exception(
                        f"API Key is invalid for [{url}]"
                        f"HTTP Response Code: {response.status_code}"
                    )
                else:
                    raise Exception(
                        f"Failed to retrieve content for [{url}]"
                        f"HTTP Response Code: {response.status_code}"
                    )
            finally:
                await response.aclose()

    @staticmethod
    async def _handle_request_throttling(url, backoff, retries_left) -> tuple[int, int]:
        """
        Handles request throttling by applying exponential backoff with jitter.
        The number of retries is limited by `max_retries` defined in the settings.

        :param url: Computed URL for the request
        :param backoff: Current backoff time
        :param retries_left: Number of retries left
        :return: Updated backoff time and retries left
        """
        if retries_left <= 0:
            raise Exception(f"Max retries exceeded for [{url}]")

        wait_time = backoff + random.uniform(0, 0.1 * backoff)
        backoff *= settings.BACKOFF_MULTIPLIER
        retries_left -= 1

        logger.debug(f"Rate limited, retrying in {wait_time:.2f} seconds, for [{url}]")
        await asyncio.sleep(wait_time)

        return backoff, retries_left

    def _get_url_for_date(self, date: str) -> str:
        return self.url.format(date=date, api_key=settings.API_KEY)

    @staticmethod
    def _is_csv_content(response: httpx.Response) -> bool:
        return response.headers["content-type"] == "text/csv"

    @staticmethod
    def _is_json_content(response: httpx.Response) -> bool:
        return response.headers["content-type"] == "application/json"
