import logging
from pathlib import Path
from shutil import rmtree

from dotenv import load_dotenv
from hdx.api.configuration import Configuration
from hdx.facades.infer_arguments import facade
from hdx.utilities.path import script_dir_plus_file, wheretostart_tempdir_batch
from pandas import read_csv
from tqdm import tqdm

from ._version import __version__
from .common.admin0 import download_admin0
from .common.config import (
    PROVIDER_GOOGLE,
    PROVIDER_MICROSOFT,
    RUN_DOWNLOAD,
    RUN_GOOGLE,
    RUN_GROUPING,
    RUN_MICROSOFT,
    data_dir,
    iso3_exclude,
    iso3_include,
)
from .common.group import group
from .dataset import generate_dataset
from .google import __main__ as google
from .microsoft import __main__ as microsoft

load_dotenv(override=True)
logger = logging.getLogger(__name__)
cwd = Path(__file__).parent

_LOOKUP = "hdx-scraper-buildings"
_UPDATED_BY_SCRIPT = "HDX Scraper: Buildings"


def _package(provider: str, iso3: str, output_dir: Path) -> None:
    """Make a dataset."""
    with wheretostart_tempdir_batch(folder=_LOOKUP) as info:
        dataset = generate_dataset(provider, iso3, output_dir)
        if dataset:
            dataset.update_from_yaml(
                script_dir_plus_file(
                    str(cwd / f"config/hdx_dataset_{provider}.yaml"),
                    main,
                ),
            )
            dataset.create_in_hdx(
                remove_additional_resources=True,
                match_resource_order=False,
                hxl_update=False,
                updated_by_script=_UPDATED_BY_SCRIPT,
                batch=info["batch"],
            )


def _group_and_package(provider: str) -> None:
    """Create resources for each country and then create a HDX dataset."""
    country_list = cwd / provider / "countries.csv"
    country_lookup = read_csv(country_list, usecols=["iso_3"]).drop_duplicates()
    country_codes = country_lookup["iso_3"].to_list()
    pbar = tqdm(country_codes)
    for iso3 in pbar:
        pbar.set_description(iso3)
        if len(iso3_include) and iso3 not in iso3_include:
            continue
        if len(iso3_exclude) and iso3 in iso3_exclude:
            continue
        output_dir = data_dir / provider / "outputs" / iso3.lower()
        group(provider, iso3, output_dir)
        _package(provider, iso3, output_dir)
        rmtree(output_dir)


def main() -> None:
    """Generate datasets and create them in HDX."""
    logger.info("##### %s version %s ####", _LOOKUP, __version__)
    Configuration.read()
    if RUN_GROUPING:
        download_admin0(data_dir)
    if RUN_GOOGLE:
        if RUN_DOWNLOAD:
            google.main()
        if RUN_GROUPING:
            _group_and_package(PROVIDER_GOOGLE)
    if RUN_MICROSOFT:
        if RUN_DOWNLOAD:
            microsoft.main()
        if RUN_GROUPING:
            _group_and_package(PROVIDER_MICROSOFT)


if __name__ == "__main__":
    facade(
        main,
        user_agent_config_yaml=str(Path("~").expanduser() / ".useragents.yaml"),
        user_agent_lookup=_LOOKUP,
    )
