import logging.config
from pathlib import Path

from decouple import AutoConfig, Config, RepositoryEnv

DOTENV_FILE = Path(__file__).parent / ".env"
dotenv_path = DOTENV_FILE.resolve()

# Read environment variables from .env file if it exists
if Path(dotenv_path).is_file():
    config = Config(RepositoryEnv(dotenv_path))
else:
    config = AutoConfig()


# ---------------- API Configuration ----------------
API_KEY = config("API_KEY", default="")
BASE_URL = config("BASE_URL", default="localhost:8000")
WIND_URL = f"{BASE_URL}/{{date}}/renewables/windgen.csv?api_key={API_KEY}"
SOLAR_URL = f"{BASE_URL}/{{date}}/renewables/solargen.json?api_key={API_KEY}"


# ---------------- API Rate Limiting Configuration ----------------
INITIAL_BACKOFF = config("INITIAL_BACKOFF", default=1, cast=int)
BACKOFF_MULTIPLIER = config("BACKOFF_MULTIPLIER", default=1.5, cast=float)
MAX_RETRIES = config("MAX_RETRIES", default=5, cast=int)
LOOKBACK_DAYS = config("LOOKBACK_DAYS", default=7, cast=int)


# ---------------- Output Configuration ----------------
DEFAULT_OUTPUT_PATH = Path(__file__).parent / Path(
    config("DEFAULT_OUTPUT_PATH", default="output")
)

# Create output directory if it doesn't exist
if not DEFAULT_OUTPUT_PATH.exists():
    DEFAULT_OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

DEFAULT_OUTPUT_FORMAT = config("DEFAULT_OUTPUT_FORMAT", default="parquet")


# ---------------- Logging Configuration ----------------
LOG_LEVEL = config("LOG_LEVEL", default="INFO")

LOGGING_CONFIG = {
    "version": 1,
    "handlers": {
        "default": {
            "class": "logging.StreamHandler",
            "formatter": "default",
        }
    },
    "formatters": {
        "default": {
            "format": "[%(asctime)s] [%(levelname)s] [%(name)s] - %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S",
        }
    },
    "loggers": {
        # Disable verbose logging for httpx and httpcore
        "httpx": {
            "handlers": ["default"],
            "level": "WARNING",
        },
        "httpcore": {
            "handlers": ["default"],
            "level": "WARNING",
        },
    },
    "root": {
        "level": LOG_LEVEL,
        "handlers": ["default"],
    },
}

logging.config.dictConfig(LOGGING_CONFIG)
