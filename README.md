# CLI client for ETL

This project implements a CLI client for performing ETL on Solar and Wind APIs, and storing results on disk in the specified output format.

## Prerequisites

This project requires Python 3.11 or later to support new asyncio features.

`Exact version used for this project: Python3.11.4`

## Installation

- Install Python 3.11 using [this](https://www.python.org/downloads/release/python-3114/) link.
- Create a virtual environment

```shell
$ python3.11 -m venv venv
```

- Activate the virtual environment

```shell
$ source venv/bin/activate
```

- Install dependencies

```shell
$ pip install -r requirements.txt
```

## Usage and Examples

- This project uses `argparse` to parse command line arguments. To see the list of available commands, run:

```commandline
All commands should be run from the root directory of the project.
```

````
```shell
$ python3.11 cli.py -h

usage: cli.py [-h] --source {wind,solar,all} [--output-format {csv,parquet}] [--output-path OUTPUT_PATH] [--combine-output] [--start-date START_DATE] [--end-date END_DATE]
              [--lookback-days LOOKBACK_DAYS]

CLI client for running ETL jobs

options:
  -h, --help            show this help message and exit
  --source {wind,solar,all}, -s {wind,solar,all}
                        Data source to fetch. Specify 'all' to fetch both wind and solar data.
  --output-format {csv,parquet}, -f {csv,parquet}
                        Output format for the result of ETL jobs. Defaults to 'parquet'.
  --output-path OUTPUT_PATH, -o OUTPUT_PATH
                        The path to the directory where to output file should be saved. Defaults to './output' directory.
  --combine-output, -co
                        Combine the wind and solar data generated by ETL jobs into a single file. Defaults to True.
  --start-date START_DATE, -sd START_DATE
                        Start date in YYYY-MM-DD format
  --end-date END_DATE, -e END_DATE
                        End date in YYYY-MM-DD format
  --lookback-days LOOKBACK_DAYS, -d LOOKBACK_DAYS
                        Number of days to look back

````

- Run ETL job for wind API for the last 7 days and store the result in `parquet` format in `./output` directory

```shell
$ python3.11 cli.py --source wind --output-format parquet --output-path ./output --lookback-days 7

[2023-09-27 12:17:28] [INFO] [__main__] - ------------------------------ Starting ETL process for wind data ------------------------------
[2023-09-27 12:17:28] [INFO] [etl.wind] - Extracting wind data for date range 2023-09-19 to 2023-09-26
[2023-09-27 12:17:29] [INFO] [api] - Finished fetching data for date range [2023-09-19 to 2023-09-26] from [http://localhost:8000/{date}/renewables/windgen.csv?api_key=ADU8S67Ddy!d7f?]
[2023-09-27 12:17:29] [INFO] [etl.wind] - Transforming wind data for date range 2023-09-19 to 2023-09-26
[2023-09-27 12:17:29] [INFO] [etl.wind] - Loading wind data for date range 2023-09-19 to 2023-09-26
[2023-09-27 12:17:29] [INFO] [utils] - Saved ETL output for wind data to [output/wind_1695081600_1695686400.parquet]
[2023-09-27 12:17:29] [INFO] [__main__] - ------------------------------ ETL process completed in 1.117 seconds ------------------------------

```

The output file will be created in the `./output` directory with file name `wind_<start_epoch>_<end_epoch>.parquet`

- Run all the ETL jobs (Both solar and wind APIs in this case) for the last 7 days and store the result in `csv` format in `./output` directory

```shell
$ python3.11 cli.py --source all  --output-format csv --lookback-days 7

[2023-09-27 12:19:40] [INFO] [__main__] - ------------------------------ Starting ETL process for all data ------------------------------
[2023-09-27 12:19:40] [INFO] [etl.wind] - Extracting wind data for date range 2023-09-19 to 2023-09-26
[2023-09-27 12:19:40] [INFO] [etl.solar] - Extracting solar data for date range 2023-09-19 to 2023-09-26
[2023-09-27 12:19:40] [INFO] [api] - Finished fetching data for date range [2023-09-19 to 2023-09-26] from [http://localhost:8000/{date}/renewables/solargen.json?api_key=ADU8S67Ddy!d7f?]
[2023-09-27 12:19:40] [INFO] [etl.solar] - Transforming solar data for date range 2023-09-19 to 2023-09-26
[2023-09-27 12:19:40] [INFO] [etl.solar] - Loading solar data for date range 2023-09-19 to 2023-09-26
[2023-09-27 12:19:40] [INFO] [utils] - Saved ETL output for solar data to [/Users/gaurav/projects/etl_client/output/solar_1695081600_1695686400.csv]
[2023-09-27 12:19:42] [INFO] [api] - Finished fetching data for date range [2023-09-19 to 2023-09-26] from [http://localhost:8000/{date}/renewables/windgen.csv?api_key=ADU8S67Ddy!d7f?]
[2023-09-27 12:19:42] [INFO] [etl.wind] - Transforming wind data for date range 2023-09-19 to 2023-09-26
[2023-09-27 12:19:42] [INFO] [etl.wind] - Loading wind data for date range 2023-09-19 to 2023-09-26
[2023-09-27 12:19:42] [INFO] [utils] - Saved ETL output for wind data to [/Users/gaurav/projects/etl_client/output/wind_1695081600_1695686400.csv]
[2023-09-27 12:19:42] [INFO] [__main__] - Combining wind and solar data for date range 2023-09-19 to 2023-09-26
[2023-09-27 12:19:42] [INFO] [utils] - Saved ETL output for combined data to [/Users/gaurav/projects/etl_client/output/combined_1695081600_1695686400.csv]
[2023-09-27 12:19:42] [INFO] [__main__] - ------------------------------ ETL process completed in 2.366 seconds ------------------------------
```

- You can also specify the start and end date for the ETL jobs and combine the output into a single file.

```shell
$ python3.11 cli.py --source all  --output-format csv --end-date 2023-09-28 --start-date 2023-09-01 --combine-output
```

## Running Tests

This project uses `pytest` for running tests. To run all tests, use in the root directory of the project:

```shell
$ pytest
```

## Logging

This project uses `logging` module for logging. The default log level is `INFO`. To change the log level, set the `LOG_LEVEL` environment variable to one of the following values: `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`.

## Design

### Project Structure

- `api.py` - Async API client for concurrently fetching data from the API data source.
- `cli.py` - CLI client for running ETL jobs.
- `etl` - ETL jobs for solar and wind data.
- `pytest.ini` - Contains the pytest configuration.
- `requirements.txt` - Third-part dependencies for the project.
- `settings.py` - Global project settings.
- `.env` - Environment variables for the project. This project uses `python-decouple` module to load environment variables from `.env` file and update the settings. Typically, this file should not be checked into version control, but for the sake of simplicity, I have included it in the project.
- `tests` - Unit tests for the project.
- `utils.py` - Common util functions used across the project.

```
.
├── README.md
├── .env
├── api.py
├── cli.py
├── etl
│   ├── __init__.py
│   ├── base.py
│   ├── solar.py
│   └── wind.py
├── pytest.ini
├── requirements.txt
├── settings.py
├── tests
│   ├── test_api.py
│   ├── test_cli.py
│   ├── test_etl_solar.py
│   ├── test_etl_wind.py
│   └── test_utils.py
├── utils.py
```

### ETL Design

- This project uses `asyncio` to run ETL jobs concurrently.
- The ETL jobs are implemented as classes that inherit from the `ETLBase` class. This allows us to easily add new ETL jobs in the future. The changes in the data at each step of the ETL are tracked in the member variables.
- The CLI client is the main driver of the ETL process. It parses the command line arguments, creates the ETL jobs, and runs them concurrently.
- `cli.etl_main()` is the main function that runs the ETL jobs. It creates the ETL jobs, runs them concurrently, and combines the output if required.
- `cli.main()` is the main function that parses the command line arguments, starts an asyncio event loop and schedules the `cli.etl_main()` function to run.

## Assumptions

- The ETL client is designed to run on a single machine.
- The streaming response for the wind API is small enough to be loaded into memory.
- The data at each step in the ETL process is small enough to be loaded into memory.

## Future Improvements

- Handle streaming response for the wind API.
- While the Extract step for ETL jobs is concurrent, the Transform and Load steps are not.
- The Transform step is using `pandas` to transform the data, which is not designed to be concurrent. We can use `dask` to parallelize the Transform step. The Load step is also not concurrent, but we can use `aiofiles` to write the data to disk in chunks.
- Add more unit tests and integration tests for the project.
