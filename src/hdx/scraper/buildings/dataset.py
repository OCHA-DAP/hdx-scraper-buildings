import logging
from pathlib import Path

from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.data.resource import Resource
from hdx.location.country import Country
from tenacity import retry, stop_after_attempt, wait_fixed

from .common.config import ATTEMPT, WAIT

logger = logging.getLogger(__name__)


@retry(stop=stop_after_attempt(ATTEMPT), wait=wait_fixed(WAIT))
def _add_resource(dataset: Dataset, resource_path: Path) -> None:
    """Add a resource to a dataset."""
    resource_data = {
        "name": resource_path.name,
        "description": "Building footprint data as File Geodatabase.",
    }
    resource = Resource(resource_data)
    resource.set_file_to_upload(str(resource_path))
    resource.set_format("Geodatabase")
    dataset.add_update_resource(resource)


def generate_datasets(provider: str, iso3: str, resources: Path) -> list[Dataset]:
    """Return one dataset per resource file, each with a single resource."""
    country_name = Country.get_country_name_from_iso3(iso3)
    if not country_name:
        logger.error("Country not found for %s", iso3)
        return []
    datasets = []
    for resource_path in sorted(resources.iterdir()):
        if resource_path.suffixes != [".gdb", ".zip"]:
            continue
        dataset = Dataset(
            {
                "name": f"buildings-{provider}-{iso3.lower()}",
                "title": f"{country_name}: {provider.title()} Building Footprints",
            },
        )
        dataset.add_tags(["facilities-infrastructure", "geodata"])
        dataset.set_subnational(True)
        try:
            dataset.add_country_location(iso3)
        except HDXError:
            logger.exception("Couldn't find country %s, skipping", iso3)
            continue
        _add_resource(dataset, resource_path)
        datasets.append(dataset)
    return datasets
