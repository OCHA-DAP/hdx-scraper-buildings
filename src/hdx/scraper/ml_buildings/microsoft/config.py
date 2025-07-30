from multiprocessing import cpu_count
from pathlib import Path

cwd = Path(__file__).parent

DATASET_LINKS_URL = (
    "https://minedbuildings.z5.web.core.windows.net/global-buildings/dataset-links.csv"
)
COUNTRY_LOOKUP = cwd / "iso3_to_region_name.csv"
# GLOBAL_ADMIN0 = "https://data.fieldmaps.io/adm0/osm/intl/adm0_polygons.parquet"
GLOBAL_ADMIN0 = "/Users/computer/GitHub/fieldmaps/adm0-generator/outputs/adm0/osm/intl/adm0_polygons.parquet"
GLOBAL_ADMIN1 = (
    "https://data.fieldmaps.io/edge-matched/humanitarian/intl/adm1_polygons.parquet"
)
HDX_MAX_SIZE = 1.5 * 1024 * 1024 * 1024  # 1.5 GB
PARALLEL_DOWNLOADS = cpu_count() * 4

data_dir = cwd / "../../../../../data"
data_dir.mkdir(exist_ok=True, parents=True)
