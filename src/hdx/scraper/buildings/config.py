from multiprocessing import cpu_count
from pathlib import Path

cwd = Path(__file__).parent

GLOBAL_ADM0 = "https://data.fieldmaps.io/adm0/osm/intl/adm0_polygons.parquet"
GLOBAL_ADM1 = (
    "https://data.fieldmaps.io/edge-matched/humanitarian/intl/adm1_polygons.parquet"
)
HDX_MAX_SIZE = 1.5 * 1024 * 1024 * 1024  # 1.5 GB
PARALLEL_DOWNLOADS = cpu_count() * 4

data_dir = cwd / "../../../../data"
data_dir.mkdir(exist_ok=True, parents=True)
