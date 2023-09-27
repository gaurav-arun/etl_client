import io
from unittest.mock import AsyncMock, patch

import httpx
import pandas as pd
import pytest

from api import AsyncDataFetcher

TEST_URL = "http://test-url.com/{date}?api_key={api_key}"
TEST_DATE_RANGE = pd.DatetimeIndex(["2023-01-01", "2023-01-02"])


@pytest.mark.asyncio
async def test_fetch_csv_content():
    csv_content = "header1,header2\nvalue1,value2"
    response = httpx.Response(
        200, content=csv_content.encode(), headers={"content-type": "text/csv"}
    )

    with patch("httpx.AsyncClient.get", new_callable=AsyncMock, return_value=response):
        fetcher = AsyncDataFetcher(TEST_URL, TEST_DATE_RANGE)
        results = await fetcher.fetch()

    assert len(results) == 2
    assert all(isinstance(result, io.StringIO) for result in results)


@pytest.mark.asyncio
async def test_fetch_json_content():
    json_content = {"key": "value"}
    response = httpx.Response(
        200, json=json_content, headers={"content-type": "application/json"}
    )

    with patch("httpx.AsyncClient.get", new_callable=AsyncMock, return_value=response):
        fetcher = AsyncDataFetcher(TEST_URL, TEST_DATE_RANGE)
        results = await fetcher.fetch()

    assert len(results) == 2
    assert all(isinstance(result, dict) for result in results)


@pytest.mark.asyncio
async def test_retry_on_rate_limit():
    rate_limit_response = httpx.Response(429)
    success_response = httpx.Response(
        200, json={}, headers={"content-type": "application/json"}
    )

    with patch("httpx.AsyncClient.get", new_callable=AsyncMock) as mocked_get:
        mocked_get.side_effect = (
            rate_limit_response,
            success_response,
            rate_limit_response,
            success_response,
        )
        fetcher = AsyncDataFetcher(TEST_URL, TEST_DATE_RANGE, max_retries=2, backoff=1)
        await fetcher.fetch()

    # Two dates, two attempts each due to rate limiting
    assert mocked_get.call_count == 4


@pytest.mark.asyncio
async def test_max_retries_exceeded():
    rate_limit_response = httpx.Response(429)

    with patch(
        "httpx.AsyncClient.get",
        new_callable=AsyncMock,
        return_value=rate_limit_response,
    ):
        fetcher = AsyncDataFetcher(TEST_URL, TEST_DATE_RANGE, max_retries=3, backoff=1)
        with pytest.raises(Exception):
            await fetcher.fetch()


@pytest.mark.asyncio
async def test_invalid_api_key():
    invalid_api_key_response = httpx.Response(403)

    with patch(
        "httpx.AsyncClient.get",
        new_callable=AsyncMock,
        return_value=invalid_api_key_response,
    ):
        fetcher = AsyncDataFetcher(TEST_URL, TEST_DATE_RANGE)
        with pytest.raises(Exception):
            await fetcher.fetch()


@pytest.mark.asyncio
async def test_unhandled_status_code():
    error_response = httpx.Response(400)

    with patch(
        "httpx.AsyncClient.get", new_callable=AsyncMock, return_value=error_response
    ):
        fetcher = AsyncDataFetcher(TEST_URL, TEST_DATE_RANGE)
        with pytest.raises(Exception):
            await fetcher.fetch()
