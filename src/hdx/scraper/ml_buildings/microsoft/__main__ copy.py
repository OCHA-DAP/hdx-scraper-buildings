from shutil import make_archive, rmtree

from geopandas import read_file
from pandas import read_csv
from tqdm import tqdm

from .config import COUNTRY_LOOKUP, DATASET_LINKS_URL, HDX_MAX_SIZE, data_dir
from .split import split_admin1


def main():
    dataset_links = read_csv(DATASET_LINKS_URL)
    country_lookup = read_csv(COUNTRY_LOOKUP)
    country_lookup = (
        country_lookup.groupby("ISO3")["RegionName"].apply(list).reset_index()
    )
    pbar = tqdm(country_lookup.to_dict("records"))
    for row in pbar:
        pbar.set_description(row["ISO3"])
        dataset_links_country = dataset_links[
            dataset_links["Location"].isin(row["RegionName"])
        ]
        output_dir = data_dir / f"{row['ISO3']}_buildings.gdb".lower()
        output_file = output_dir.with_suffix(".gdb.zip")
        rmtree(output_dir, ignore_errors=True)
        for url in dataset_links_country["Url"].to_list():
            gdf = read_file(f"GeoJSONSeq:/vsigzip//vsicurl/{url}", use_arrow=True)
            write_mode = "a" if output_dir.exists() else "w"
            gdf.to_file(
                output_dir,
                mode=write_mode,
                driver="OpenFileGDB",
                layer=f"{row['ISO3']}_buildings".lower(),
            )
        make_archive(str(output_dir), "zip", output_dir)
        if output_file.stat().st_size > HDX_MAX_SIZE:
            output_file.unlink()
            split_admin1(row["ISO3"])
        rmtree(output_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
