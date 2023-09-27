import argparse
import asyncio
import logging
import time
from pathlib import Path

import pandas as pd

import settings
import utils
from etl import ETLSolar, ETLWind

logger = logging.getLogger(__name__)


async def etl_main(
    sources: list[str],
    output_format: str,
    output_path: Path,
    combine_output: bool,
    start_date: str,
    end_date: str,
    lookback_days: int,
):
    """
    Main ETL function that is handed over to the asyncio event loop.
    Runs the ETL jobs in parallel if 'all' is specified as source.

    :param sources: 'wind', 'solar' or 'all'. If 'all' is specified, both
                    wind and solar data will be fetched.
    :param output_format: 'csv' or 'parquet' (default).
    :param output_path: The directory to save the output file to.
    :param combine_output: If True, results of all ETL jobs will be combined
                           into a single file.
    :param start_date: The start date in YYYY-MM-DD format.
    :param end_date: The end date in YYYY-MM-DD format.
    :param lookback_days: The number of days to look back.
    """
    date_range: pd.Index = utils.get_date_range(
        start_date=start_date, end_date=end_date, lookback_days=lookback_days
    )

    if not output_path.exists():
        output_path.mkdir(parents=True)

    if "wind" in sources:
        etl_wind = ETLWind(
            date_range=date_range, output_format=output_format, output_path=output_path
        )
        await etl_wind.run()

    elif "solar" in sources:
        etl_solar = ETLSolar(
            date_range=date_range, output_format=output_format, output_path=output_path
        )
        await etl_solar.run()

    elif "all" in sources:
        etl_jobs = (
            ETLWind(
                date_range=date_range,
                output_format=output_format,
                output_path=output_path,
            ),
            ETLSolar(
                date_range=date_range,
                output_format=output_format,
                output_path=output_path,
            ),
        )

        # Run ETL jobs in parallel
        async with asyncio.TaskGroup() as tg:
            for etl_job in etl_jobs:
                tg.create_task(etl_job.run())

        # Combine output from all ETL jobs
        if combine_output:
            transformed_data: list[pd.DataFrame] = []
            for etl_job in etl_jobs:
                etl_job.transformed_data["source"] = etl_job.name
                transformed_data.append(etl_job.transformed_data)

            logger.info(
                f"Combining wind and solar data for date "
                f"range {date_range[0]} to {date_range[-1]}"
            )

            combined_dfs: pd.DataFrame = utils.combine_dataframes(transformed_data)
            utils.save_dataframe(
                df=combined_dfs,
                name_prefix="combined",
                date_range=date_range,
                output_format=output_format,
                output_path=output_path,
            )


def parse_arguments():
    """
    Parses command line arguments.
    """
    parser = argparse.ArgumentParser(description="CLI client for running ETL jobs")

    parser.add_argument(
        "--source",
        "-s",
        choices=["wind", "solar", "all"],
        required=True,
        help="Data source to fetch. Specify 'all' to fetch both wind and solar data.",
        # nargs="+",
    )
    parser.add_argument(
        "--output-format",
        "-f",
        choices=["csv", "parquet"],
        default="parquet",
        help="Output format for the result of ETL jobs. " "Defaults to 'parquet'.",
    )
    parser.add_argument(
        "--output-path",
        "-o",
        default=str(settings.DEFAULT_OUTPUT_PATH),
        help="The path to the directory where to output file "
        "should be saved. Defaults to './output' directory.",
    )
    parser.add_argument(
        "--combine-output",
        "-co",
        action="store_true",
        default=True,
        help="Combine the wind and solar data generated by ETL "
        "jobs into a single file. Defaults to True.",
    )
    parser.add_argument(
        "--start-date",
        "-sd",
        default=None,
        help="Start date in YYYY-MM-DD format",
    )
    parser.add_argument(
        "--end-date", "-e", default=None, help="End date in YYYY-MM-DD format"
    )
    parser.add_argument(
        "--lookback-days", "-d", type=int, default=7, help="Number of days to look back"
    )

    return parser.parse_args()


def main():
    """
    Main function that is called when the script is run from the command line.
    """
    args = parse_arguments()

    logger.info(f"{'-' * 30} Starting ETL process for {args.source} data {'-' * 30}")
    start = time.perf_counter()

    # Start the asyncio event loop and run the main ETL function
    asyncio.run(
        etl_main(
            sources=args.source,
            output_format=args.output_format,
            output_path=Path(args.output_path),
            combine_output=args.combine_output,
            start_date=args.start_date,
            end_date=args.end_date,
            lookback_days=args.lookback_days,
        )
    )

    elapsed = time.perf_counter() - start
    logger.info(f"{'-' * 30} ETL process completed in {elapsed:.3f} seconds {'-' * 30}")


if __name__ == "__main__":
    main()
