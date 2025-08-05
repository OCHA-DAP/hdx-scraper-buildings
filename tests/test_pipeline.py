from pathlib import Path

from hdx.api.configuration import Configuration
from hdx.scraper.buildings.pipeline import Pipeline
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve


class TestPipeline:
    """Test Pipeline."""

    def test_pipeline(
        self,
        configuration: Configuration,
        fixtures_dir: Path,  # noqa: ARG002
        input_dir: Path,
        config_dir: Path,
    ) -> None:
        """Test pipeline."""
        with (
            temp_dir(
                "TestBuildings",
                delete_on_success=True,
                delete_on_failure=False,
            ) as tempdir,
            Download(user_agent="test") as downloader,
        ):
            retriever = Retrieve(
                downloader=downloader,
                fallback_dir=tempdir,
                saved_dir=str(input_dir),
                temp_dir=tempdir,
                save=False,
                use_saved=True,
            )
            pipeline = Pipeline(configuration, retriever, tempdir)
            dataset = pipeline.generate_dataset()
            dataset.update_from_yaml(
                path=str(config_dir / "hdx_dataset_static.yaml"),
            )
