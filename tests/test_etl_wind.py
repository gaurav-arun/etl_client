import io
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

import pandas as pd
import pytest

from etl import ETLWind

TEST_DATE_RANGE = pd.DatetimeIndex(["2023-01-01", "2023-01-02"])
TEST_OUTPUT_FORMAT = "csv"
TEST_OUTPUT_PATH = Path("/output")


@pytest.fixture
def etl_wind():
    """Instance of ETLWind"""
    etl_wind = ETLWind(
        date_range=TEST_DATE_RANGE,
        output_format=TEST_OUTPUT_FORMAT,
        output_path=TEST_OUTPUT_PATH,
    )
    return etl_wind


@pytest.fixture
def mock_api_response():
    """Sample response from Wind API for specified date range"""
    csv_data1 = (
        "naive_timestamp, variable, value, last_modified_utc\n"
        "2023-01-01,729,38.0098121304,2023-01-01"
    )
    csv_data2 = (
        "naive_timestamp, variable, value, last_modified_utc\n"
        "2023-01-02,846,31.2335398111,2023-01-02"
    )

    return [io.StringIO(csv_data1), io.StringIO(csv_data2)]


@pytest.fixture
def mock_transformed_data():
    """Transformed data from mock_api_response"""
    return pd.DataFrame(
        [
            {
                "timestamp_utc": pd.Timestamp("2023-01-01", tz="UTC"),
                "variable": 729,
                "value": 38.0098121304,
                "last_modified_utc": pd.Timestamp("2023-01-01", tz="UTC"),
            },
            {
                "timestamp_utc": pd.Timestamp("2023-01-02", tz="UTC"),
                "variable": 846,
                "value": 31.2335398111,
                "last_modified_utc": pd.Timestamp("2023-01-02", tz="UTC"),
            },
        ]
    )


@pytest.mark.asyncio
async def test_extract(etl_wind, mock_api_response):
    """
    Given: An instance of ETLWind
    When: `extract()` is called
    Then: `extracted_data` is set to the response from the API
    """

    with patch(
        "api.AsyncDataFetcher.fetch",
        new_callable=AsyncMock,
        return_value=mock_api_response,
    ):
        await etl_wind.extract()

    expected_extracted_data = mock_api_response
    actual_extracted_data = etl_wind.extracted_data

    assert expected_extracted_data == actual_extracted_data


@pytest.mark.asyncio
async def test_transform(etl_wind, mock_api_response, mock_transformed_data):
    """
    Given: An instance of ETLWind
    When: `transform()` is called after `extract()`
    Then: `extracted_data` is transformed and set to `transformed_data`
    """
    etl_wind.extracted_data = mock_api_response

    await etl_wind.transform()

    expected_transformed_data = mock_transformed_data
    actual_transformed_data = etl_wind.transformed_data

    pd.testing.assert_frame_equal(expected_transformed_data, actual_transformed_data)


@pytest.mark.asyncio
async def test_load(etl_wind, mock_transformed_data):
    """
    Given: An instance of ETLWind
    When: `load()` is called after `extract()` and `transform()`
    Then: `transformed_data` is saved to disk in the specified format
    """
    etl_wind.transformed_data = mock_transformed_data

    with patch("utils.save_dataframe", new_callable=Mock) as mocked_save_dataframe:
        await etl_wind.load()

    mocked_save_dataframe.assert_called_once_with(
        df=etl_wind.transformed_data,
        name_prefix=etl_wind.name,
        date_range=TEST_DATE_RANGE,
        output_format=TEST_OUTPUT_FORMAT,
        output_path=TEST_OUTPUT_PATH,
    )
