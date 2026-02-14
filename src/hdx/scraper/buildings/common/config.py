from multiprocessing import cpu_count
from os import getenv
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(override=True)


def _is_bool_env(env: str) -> bool:
    """Check if env is a boolean."""
    return env.lower() in ("true", "yes", "on", "1")


PROVIDER_GOOGLE = "google"
PROVIDER_MICROSOFT = "microsoft"

RUN_GOOGLE = _is_bool_env(getenv("RUN_GOOGLE", "NO"))
RUN_MICROSOFT = _is_bool_env(getenv("RUN_MICROSOFT", "NO"))

AWS_ENDPOINT_URL = "https://data.source.coop"
AWS_ENDPOINT_S3 = "us-west-2.opendata.source.coop"

ARCGIS_SERVER = getenv("ARCGIS_SERVER", "https://gis.unocha.org")
ARCGIS_USERNAME = getenv("ARCGIS_USERNAME", "")
ARCGIS_PASSWORD = getenv("ARCGIS_PASSWORD", "")
ARCGIS_ADM0_URL = (
    f"{ARCGIS_SERVER}/server/rest/services/Hosted/Global_AB_1M_fs_gray/FeatureServer/5"
)

ATTEMPT = 24  # for 1 day
WAIT = 10  # 10 seconds
TIMEOUT = 60 * 60  # 1 hour

CONCURRENCY_LIMIT = int(getenv("CONCURRENCY_LIMIT", str(cpu_count())))

HDX_MAX_SIZE = 1.5 * 1024 * 1024 * 1024  # 1.5 GB

RUN_DOWNLOAD = _is_bool_env(getenv("RUN_DOWNLOAD", "NO"))
RUN_GROUPING = _is_bool_env(getenv("RUN_GROUPING", "YES"))

ISO_3_LEN = 3

iso3_include = [
    x for x in getenv("ISO3_INCLUDE", "").upper().split(",") if len(x) == ISO_3_LEN
]
iso3_exclude = [
    x for x in getenv("ISO3_EXCLUDE", "").upper().split(",") if len(x) == ISO_3_LEN
]

cwd = Path(__file__).parent
data_dir = cwd / "../../../../../saved_data"
data_dir.mkdir(exist_ok=True, parents=True)

GLOBAL_ADM0 = data_dir / "bnda_cty.parquet"
