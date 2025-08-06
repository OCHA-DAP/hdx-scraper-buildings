import logging
from pathlib import Path

from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.data.resource import Resource
from hdx.location.country import Country

logger = logging.getLogger(__name__)


def generate_dataset(provider: str, iso3: str, resources: Path) -> Dataset | None:
    """Dataset generator."""
    country_name = Country.get_country_name_from_iso3(iso3)
    if not country_name:
        logger.error("Country not found for %s", iso3)
        return None
    dataset = Dataset(
        {
            "name": f"buildings-{provider}-{iso3.lower()}",
            "title": f"{country_name}: {provider.title()} Building Footprints",
        },
    )
    dataset_tags = ["facilities-infrastructure"]
    dataset.add_tags(dataset_tags)
    dataset.set_subnational(True)
    try:
        dataset.add_country_location(iso3)
    except HDXError:
        logger.exception("Couldn't find country %s, skipping", iso3)
        return None
    for resource_path in resources.iterdir():
        if resource_path.suffixes != [".gdb", ".zip"]:
            continue
        resource_data = {
            "name": resource_path.name,
            "description": "Building footprint data as File Geodatabase.",
        }
        resource = Resource(resource_data)
        resource.set_file_to_upload(str(resource_path))
        resource.set_format("Geodatabase")
        dataset.add_update_resource(resource)
    return dataset
