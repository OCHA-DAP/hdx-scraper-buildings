import logging
from shutil import make_archive, rmtree

from geopandas import read_file

from hdx.scraper.ml_buildings.microsoft.config import data_dir

logger = logging.getLogger(__name__)


def iterate(dataset_links, row):
    output_dir = data_dir / f"{row['ISO3']}.gdb"
    output_file = output_dir.with_suffix(".gdb.zip")
    if output_file.exists() and not output_dir.exists():
        return
    dataset_links_country = dataset_links[
        dataset_links["Location"].isin(row["RegionName"])
    ]
    rmtree(output_dir, ignore_errors=True)
    for url in dataset_links_country["Url"].to_list():
        gdf = read_file(f"GeoJSONSeq:/vsigzip//vsicurl/{url}", use_arrow=True)
        write_mode = "a" if output_dir.exists() else "w"
        gdf.to_file(output_dir, mode=write_mode, driver="OpenFileGDB")
    make_archive(str(output_dir), "zip", output_dir)
    rmtree(output_dir, ignore_errors=True)
