from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from utils import get_date_range, save_dataframe

START_DATE = "2023-01-01"
END_DATE = "2023-01-10"
INVALID_DATE = "2023-01-32"
LOOKBACK_DAYS = 5

TEST_DF = pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]})
TEST_NAME_PREFIX = "test_prefix"
TEST_DATE_RANGE = pd.date_range(start="2023-01-01", end="2023-01-02")
TEST_OUTPUT_FORMAT_CSV = "csv"
TEST_OUTPUT_FORMAT_PARQUET = "parquet"
TEST_OUTPUT_PATH = Path("/path/to/output")


@pytest.mark.parametrize(
    "start_date, end_date, expected_length",
    [
        (None, None, LOOKBACK_DAYS + 1),
        (
            START_DATE,
            None,
            (
                pd.Timestamp.now(tz="UTC").floor("D")
                - pd.Timestamp(START_DATE, tz="UTC").floor("D")
            ).days,
        ),
        (None, END_DATE, LOOKBACK_DAYS + 1),
        (
            START_DATE,
            END_DATE,
            (
                pd.Timestamp(END_DATE, tz="UTC").floor("D")
                - pd.Timestamp(START_DATE, tz="UTC").floor("D")
            ).days
            + 1,
        ),
        (START_DATE, START_DATE, 1),
    ],
)
def test_get_date_range_valid_input(start_date, end_date, expected_length):
    """
    Given: A start_date, end_date and/or lookback_days.
    When: get_date_range is called.
    Then: Return a valid date range with expected length.
    """
    date_range = get_date_range(
        start_date=start_date, end_date=end_date, lookback_days=LOOKBACK_DAYS
    )
    assert isinstance(date_range, pd.Index)
    assert len(date_range) == expected_length


def test_get_date_range_invalid_start_date():
    """
    Given: Invalid start_date.
    When: get_date_range is called.
    Then: Raise ValueError.
    """
    with pytest.raises(ValueError, match="Invalid format for start_date"):
        get_date_range(start_date=INVALID_DATE, end_date=END_DATE)


def test_get_date_range_invalid_end_date():
    """
    Given: Invalid end_date.
    When: get_date_range is called.
    Then: Raise ValueError.
    """
    with pytest.raises(ValueError, match="Invalid format for end_date"):
        get_date_range(start_date=START_DATE, end_date=INVALID_DATE)


def test_get_date_range_end_date_before_start_date():
    """
    Given: end_date is before start_date.
    When: get_date_range is called.
    Then: Raise ValueError.
    """
    with pytest.raises(ValueError, match="end_date cannot be before start_date"):
        get_date_range(start_date=END_DATE, end_date=START_DATE)


def test_save_dataframe_csv():
    """
    Given: A DataFrame, name_prefix, date_range, output_format, and output_path.
    When: `save_dataframe()` is called with output_format="csv".
    Then: pd.DataFrame.to_csv() is called with the correct arguments.
    """
    with patch.object(pd.DataFrame, "to_csv", autospec=True) as mocked_to_csv:
        with patch("utils.logger.info", autospec=True) as mocked_logger_info:
            save_dataframe(
                TEST_DF,
                TEST_NAME_PREFIX,
                TEST_DATE_RANGE,
                TEST_OUTPUT_FORMAT_CSV,
                TEST_OUTPUT_PATH,
            )

        args = mocked_to_csv.call_args.args
        kwargs = mocked_to_csv.call_args.kwargs

        # Assert the file is saved with the correct path and format
        file_name = f"{TEST_NAME_PREFIX}_1672531200_1672617600.csv"
        assert str(args[1]) == str(TEST_OUTPUT_PATH / file_name)
        assert not kwargs["index"]
        assert kwargs["float_format"] == "%.5f"

        # Assert the logging message
        mocked_logger_info.assert_called_once()


def test_save_dataframe_parquet():
    """
    Given: A DataFrame, name_prefix, date_range, output_format, and output_path.
    When: `save_dataframe()` is called with output_format="parquet".
    Then: pd.DataFrame.to_parquet() is called with the correct arguments.
    """
    with patch.object(pd.DataFrame, "to_parquet", autospec=True) as mocked_to_parquet:
        with patch("utils.logger.info", autospec=True) as mocked_logger_info:
            save_dataframe(
                TEST_DF,
                TEST_NAME_PREFIX,
                TEST_DATE_RANGE,
                TEST_OUTPUT_FORMAT_PARQUET,
                TEST_OUTPUT_PATH,
            )

        args = mocked_to_parquet.call_args.args
        kwargs = mocked_to_parquet.call_args.kwargs

        # Assert the file is saved with the correct path and format
        file_name = f"{TEST_NAME_PREFIX}_1672531200_1672617600.parquet"
        assert str(args[1]) == str(TEST_OUTPUT_PATH / file_name)
        assert not kwargs["index"]

        # Assert the logging message
        mocked_logger_info.assert_called_once()


def test_save_dataframe_invalid_format():
    """
    Given: A DataFrame, name_prefix, date_range, output_format, and output_path.
    When: `save_dataframe()` is called with invalid output_format="csv".
    Then: Raise ValueError.
    """
    invalid_format = "invalid_format"

    with pytest.raises(ValueError):
        save_dataframe(
            TEST_DF, TEST_NAME_PREFIX, TEST_DATE_RANGE, invalid_format, TEST_OUTPUT_PATH
        )
