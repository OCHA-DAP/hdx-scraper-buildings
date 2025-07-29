from geopandas import read_file
from shapely.geometry.multipolygon import MultiPolygon
from shapely.geometry.polygon import Polygon

from .config import data_dir


def download_file(url: str):
    file_name = url.split("/global-buildings.geojsonl/")[-1]
    output_file = (
        data_dir / "microsoft" / "inputs" / file_name.replace(".csv.gz", ".parquet")
    )
    output_file.parent.mkdir(exist_ok=True, parents=True)
    gdf = read_file(f"GeoJSONSeq:/vsigzip//vsicurl/{url}", use_arrow=True)
    gdf["geometry"] = [
        MultiPolygon([feature]) if isinstance(feature, Polygon) else feature
        for feature in gdf["geometry"]
    ]
    gdf.to_parquet(
        output_file,
        compression="zstd",
        write_covering_bbox=True,
        schema_version="1.1.0",
    )
