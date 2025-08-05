#!/usr/bin/python
"""Top level script.

Calls other functions that generate datasets that this script then creates in HDX.
"""

import logging
from os import getenv
from pathlib import Path

from dotenv import load_dotenv

from hdx.api.configuration import Configuration
from hdx.data.user import User
from hdx.facades.infer_arguments import facade
from hdx.utilities.downloader import Download
from hdx.utilities.path import script_dir_plus_file, wheretostart_tempdir_batch
from hdx.utilities.retriever import Retrieve

from ._version import __version__
from .common.config import is_bool_env
from .google import __main__ as google
from .microsoft import __main__ as microsoft
from .pipeline import Pipeline

load_dotenv(override=True)
logger = logging.getLogger(__name__)

RUN_GOOGLE = is_bool_env(getenv("RUN_GOOGLE", "NO"))
RUN_MICROSOFT = is_bool_env(getenv("RUN_MICROSOFT", "NO"))

_LOOKUP = "hdx-scraper-buildings"
_SAVED_DATA_DIR = "saved_data"  # Keep in repo to avoid deletion in /tmp
_UPDATED_BY_SCRIPT = "HDX Scraper: Buildings"


def main(
    save: bool = False,  # noqa: FBT001, FBT002, Save downloaded data
    use_saved: bool = False,  # noqa: FBT001, FBT002, Use saved data
) -> None:
    """Generate datasets and create them in HDX."""
    logger.info("##### %s version %s ####", _LOOKUP, __version__)
    configuration = Configuration.read()
    User.check_current_user_write_access("hdx")

    with wheretostart_tempdir_batch(folder=_LOOKUP) as info:
        tempdir = info["folder"]
        with Download() as downloader:
            retriever = Retrieve(
                downloader=downloader,
                fallback_dir=tempdir,
                saved_dir=_SAVED_DATA_DIR,
                temp_dir=tempdir,
                save=save,
                use_saved=use_saved,
            )
            pipeline = Pipeline(configuration, retriever, tempdir)

            if RUN_GOOGLE:
                google.main()
            if RUN_MICROSOFT:
                microsoft.main()

            dataset = pipeline.generate_dataset()
            if dataset:
                dataset.update_from_yaml(
                    script_dir_plus_file(
                        str(Path("config") / "hdx_dataset_static.yaml"),
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


if __name__ == "__main__":
    facade(
        main,
        user_agent_config_yaml=str(Path("~").expanduser() / ".useragents.yaml"),
        user_agent_lookup=_LOOKUP,
    )
