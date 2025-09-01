from multiprocessing import cpu_count
from os import getenv
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(override=True)


def is_bool_env(env: str) -> bool:
    """Check if env is a boolean."""
    return env.lower() in ("true", "yes", "on", "1")


PROVIDER_GOOGLE = "google"
PROVIDER_MICROSOFT = "microsoft"

RUN_GOOGLE = is_bool_env(getenv("RUN_GOOGLE", "NO"))
RUN_MICROSOFT = is_bool_env(getenv("RUN_MICROSOFT", "NO"))

GLOBAL_ADM0 = "https://data.fieldmaps.io/adm0/osm/intl/adm0_polygons.parquet"
GLOBAL_ADM1 = (
    "https://data.fieldmaps.io/edge-matched/humanitarian/intl/adm1_polygons.parquet"
)

AWS_ENDPOINT_URL = "https://data.source.coop"
AWS_ENDPOINT_S3 = "us-west-2.opendata.source.coop"

ATTEMPT = 24  # for 1 day
WAIT = 10  # 10 seconds
TIMEOUT = 60 * 60  # 1 hour

CONCURRENCY_LIMIT = int(getenv("CONCURRENCY_LIMIT", str(cpu_count())))

HDX_MAX_SIZE = 1.5 * 1024 * 1024 * 1024  # 1.5 GB

SKIP_DOWNLOAD = is_bool_env(getenv("SKIP_DOWNLOAD", "YES"))
SKIP_GROUPING = is_bool_env(getenv("SKIP_GROUPING", "NO"))

ISO_3_LEN = 3

cwd = Path(__file__).parent
data_dir = cwd / "../../../../../saved_data"
data_dir.mkdir(exist_ok=True, parents=True)

iso3_filter = [
    x for x in getenv("ISO3_FILTER", "").upper().split(",") if len(x) == ISO_3_LEN
]
