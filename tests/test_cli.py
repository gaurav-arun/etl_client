from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from cli import etl_main


@pytest.mark.asyncio
async def test_etl_main_wind():
    """
    Given: 'wind' is specified as source
    When: `etl_main()` is called
    Then: ETLWind.run() is called
    """

    with patch("etl.ETLWind.run", new_callable=AsyncMock) as mocked_run:
        await etl_main(
            sources=["wind"],
            output_format="csv",
            output_path=Path("."),
            combine_output=False,
            start_date="2023-09-22",
            end_date="2023-09-23",
            lookback_days=7,
        )
        mocked_run.assert_called_once()


@pytest.mark.asyncio
async def test_etl_main_solar():
    """
    Given: 'solar' is specified as source
    When: `etl_main()` is called
    Then: ETLSolar.run() is called
    """
    with patch("etl.ETLSolar.run", new_callable=AsyncMock) as mocked_run:
        await etl_main(
            sources=["solar"],
            output_format="csv",
            output_path=Path("."),
            combine_output=False,
            start_date="2023-09-22",
            end_date="2023-09-23",
            lookback_days=7,
        )
        mocked_run.assert_called_once()


@pytest.mark.asyncio
async def test_etl_main_all():
    """
    Given: 'all' is specified as source
    When: `etl_main()` is called
    Then: ETLWind.run() and ETLSolar.run() are called
    """
    with patch("etl.ETLWind.run", new_callable=AsyncMock) as mocked_wind_run, patch(
        "etl.ETLSolar.run", new_callable=AsyncMock
    ) as mocked_solar_run:
        await etl_main(
            sources=["all"],
            output_format="csv",
            output_path=Path("."),
            combine_output=False,
            start_date="2023-09-22",
            end_date="2023-09-23",
            lookback_days=7,
        )
        mocked_wind_run.assert_called_once()
        mocked_solar_run.assert_called_once()


# TODO: Add tests for invalid source and combine_output
