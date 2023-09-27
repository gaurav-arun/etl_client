import logging
from pathlib import Path

import pandas as pd

import settings

logger = logging.getLogger(__name__)


def save_dataframe(
    df: pd.DataFrame,
    name_prefix: str,
    date_range: pd.Index,
    output_format: str,
    output_path: Path,
):
    """
    Saves a DataFrame to a CSV or Parquet file at output_path directory.

    :param df: DataFrame to save
    :param name_prefix: An identifier to use in the file name
    :param date_range: The date range used to generate the data
    :param output_format: The output format to use. Either "csv" or "parquet".
    :param output_path: The directory to save the file to
    """
    start_timestamp = int(pd.Timestamp(date_range[0], tz="UTC").timestamp())
    end_timestamp = int(pd.Timestamp(date_range[-1], tz="UTC").timestamp())
    file_name = f"{name_prefix}_{start_timestamp}_{end_timestamp}"

    if output_format == "csv":
        file_name += ".csv"
        df.to_csv(output_path / file_name, index=False, float_format="%.5f")
    elif output_format == "parquet":
        file_name += ".parquet"
        df.to_parquet(output_path / file_name, index=False)
    else:
        raise ValueError(
            f"Invalid output format [{output_format}]. "
            f"Must be one of: csv, parquet."
        )

    logger.info(
        f"Saved ETL output for {name_prefix} data to [{output_path / file_name}]"
    )


def combine_dataframes(df_list: list[pd.DataFrame]) -> pd.DataFrame:
    """
    Combines a list of DataFrames into a single DataFrame.

    :param df_list: List of DataFrames to combine
    :return: Combined DataFrame
    """
    return pd.concat(df_list, ignore_index=True)


def get_date_range(
    start_date: str | None = None,
    end_date: str | None = None,
    lookback_days: int = settings.LOOKBACK_DAYS,
) -> pd.Index:
    """
    Generates a date range based on the given start_date, end_date and lookback_days.

    :param start_date: The start date of the date range. If not given, it will be
        calculated based on the end_date and lookback_days.
    :param end_date: The end date of the date range. If not given, it will be
        calculated based on the current date.
    :param lookback_days: The number of days to look back if start_date is not given.
    :raises ValueError: If end_date is before start_date.
    :return: A date range as a pd.Index.

    Examples:
        - get_date_range()
          (Assuming today is 2023-09-08 and LOOKBACK_DAYS=7 (default))
          Index([
            '2023-09-01', '2023-09-02', '2023-09-03',
            '2023-09-04', '2023-09-05', '2023-09-06',
            '2023-09-07'
          ])

        - get_date_range(start_date="2023-09-01", end_date="2023-09-03")
          Index(['2023-09-01', '2023-09-02', '2023-09-03'])

        - get_date_range(end_date="2023-01-03", lookback_days=1)
          Index(['2023-09-02', '2023-09-03'])

        - get_date_range(start_date="2023-01-01")
          (Assuming today is 2023-09-04)
          Index(['2023-09-01', '2023-09-02', '2023-09-03'])
    """
    if not end_date:
        end_date = pd.Timestamp.now(tz="UTC").floor("D") - pd.Timedelta(days=1)
    else:
        try:
            end_date = pd.Timestamp(end_date, tz="UTC").floor("D")
        except ValueError:
            raise ValueError("Invalid format for end_date. It should be YYYY-MM-DD.")

    if not start_date:
        start_date = end_date - pd.Timedelta(days=lookback_days)
    else:
        try:
            start_date = pd.Timestamp(start_date, tz="UTC").floor("D")
        except ValueError:
            raise ValueError("Invalid format for start_date. It should be YYYY-MM-DD.")

    if end_date < start_date:
        raise ValueError("end_date cannot be before start_date.")

    # Generate date range
    date_range = pd.date_range(start_date, end_date, freq="D")
    date_range = date_range.strftime("%Y-%m-%d")

    return date_range
