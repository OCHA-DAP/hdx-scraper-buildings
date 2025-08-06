import logging
from pathlib import Path

from dotenv import load_dotenv

from hdx.api.configuration import Configuration
from hdx.data.user import User
from hdx.facades.infer_arguments import facade
from hdx.utilities.path import script_dir_plus_file, wheretostart_tempdir_batch

from ._version import __version__
from .common.config import (
    PROVIDER_GOOGLE,
    PROVIDER_MICROSOFT,
    RUN_GOOGLE,
    RUN_MICROSOFT,
    SKIP_DOWNLOAD,
    data_dir,
)
from .dataset import generate_dataset
from .google import __main__ as google
from .microsoft import __main__ as microsoft

load_dotenv(override=True)
logger = logging.getLogger(__name__)

_LOOKUP = "hdx-scraper-buildings"
_UPDATED_BY_SCRIPT = "HDX Scraper: Buildings"


def make_dataset(provider: str) -> None:
    """Make a dartaset."""
    with wheretostart_tempdir_batch(folder=_LOOKUP) as info:
        for resources in (data_dir / provider / "outputs").iterdir():
            if not resources.is_dir():
                continue
            iso3 = resources.name.upper()
            dataset = generate_dataset(provider, iso3, resources)
            if dataset:
                dataset.update_from_yaml(
                    script_dir_plus_file(
                        str(Path("config") / f"hdx_dataset_{provider}.yaml"),
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


def main() -> None:
    """Generate datasets and create them in HDX."""
    logger.info("##### %s version %s ####", _LOOKUP, __version__)
    Configuration.read()
    User.check_current_user_write_access("hdx")

    if RUN_GOOGLE:
        google.main()
        make_dataset(PROVIDER_GOOGLE)
        if not SKIP_DOWNLOAD:
            google.cleanup()
    if RUN_MICROSOFT:
        microsoft.main()
        make_dataset(PROVIDER_MICROSOFT)
        if not SKIP_DOWNLOAD:
            microsoft.cleanup()


if __name__ == "__main__":
    facade(
        main,
        user_agent_config_yaml=str(Path("~").expanduser() / ".useragents.yaml"),
        user_agent_lookup=_LOOKUP,
    )
