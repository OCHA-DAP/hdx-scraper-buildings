from pathlib import Path

import pytest
from hdx.api.configuration import Configuration
from hdx.api.locations import Locations
from hdx.data.vocabulary import Vocabulary
from hdx.location.country import Country
from hdx.utilities.useragent import UserAgent


@pytest.fixture(scope="session")
def fixtures_dir() -> Path:
    """Return fixtures directory."""
    return Path("tests") / "fixtures"


@pytest.fixture(scope="session")
def input_dir(fixtures_dir: Path) -> Path:
    """Return input directory."""
    return fixtures_dir / "input"


@pytest.fixture(scope="session")
def config_dir() -> Path:
    """Return configuration directory."""
    return Path("src/hdx/scraper/buildings/config")


@pytest.fixture(scope="session")
def configuration(config_dir: Path) -> Configuration:
    """Return configuration."""
    UserAgent.set_global("test")
    Configuration._create(  # noqa: SLF001
        hdx_read_only=True,
        hdx_site="prod",
        project_config_yaml=config_dir / "project_configuration.yaml",
    )
    # Change locations below to match those needed in tests
    Locations.set_validlocations(
        [
            {"name": "afg", "title": "Afghanistan"},
            {"name": "sdn", "title": "Sudan"},
            {"name": "world", "title": "World"},
        ],
    )
    Country.countriesdata(include_unofficial=False)
    Vocabulary._approved_vocabulary = {  # noqa: SLF001
        "tags": [
            {"name": tag}
            # Change tags below to match those needed in tests
            for tag in (
                "hxl",
                "humanitarian needs overview - hno",
                "people in need - pin",
            )
        ],
        "id": "b891512e-9516-4bf5-962a-7a289772a2a1",
        "name": "approved",
    }
    return Configuration.read()
