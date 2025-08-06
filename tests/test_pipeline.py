from pathlib import Path

from hdx.scraper.buildings.dataset import generate_dataset


class TestPipeline:
    """Test Pipeline."""

    def test_pipeline(self, config_dir: Path) -> None:
        """Test pipeline."""
        dataset = generate_dataset()
        if dataset:
            dataset.update_from_yaml(
                path=str(config_dir / "hdx_dataset_static.yaml"),
            )
