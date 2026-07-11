"""Volume path management utilities."""
from dataclasses import dataclass


@dataclass
class VolumePaths:
    """Centralized volume path management.

    Attributes:
        catalog: Unity Catalog name
        landing: Path to landing volume
        bronze_checkpoint: Path to bronze checkpoint volume
        checkpoints: Path to data quality checkpoints
    """
    catalog: str

    def __post_init__(self):
        """Initialize derived paths."""
        self.landing = f"/Volumes/{self.catalog}/raw/chi311_landing"
        self.bronze_checkpoint = f"/Volumes/{self.catalog}/bronze/chi311_landing/autoloader"
        self.checkpoints = f"/Volumes/{self.catalog}/bronze/checkpoints"

    def get_checkpoint_path(self, stage: str) -> str:
        """Get checkpoint path for a specific stage.

        Args:
            stage: Pipeline stage name (e.g., "bronze", "silver")

        Returns:
            Checkpoint path for the stage
        """
        return f"{self.checkpoints}/{stage}"


def get_volume_path(catalog: str, schema: str, volume: str) -> str:
    """Construct volume path.

    Args:
        catalog: Catalog name
        schema: Schema name
        volume: Volume name

    Returns:
        Full volume path
    """
    return f"/Volumes/{catalog}/{schema}/{volume}"
