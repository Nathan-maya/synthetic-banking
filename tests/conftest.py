
"""Pytest fixtures for Synthetic Banking."""

import csv
import json
import os
from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def spark():
    """Create a Databricks Connect session only when a test requests Spark."""
    from databricks.connect import DatabricksSession

    builder = DatabricksSession.builder

    if not (
        os.getenv("DATABRICKS_SERVERLESS_COMPUTE_ID")
        or os.getenv("DATABRICKS_CLUSTER_ID")
        or os.getenv("SPARK_REMOTE")
    ):
        os.environ["DATABRICKS_SERVERLESS_COMPUTE_ID"] = "auto"

    if hasattr(builder, "validateSession"):
        return builder.validateSession().getOrCreate()

    return builder.getOrCreate()


@pytest.fixture()
def load_fixture(spark):
    """Load JSON or CSV files from the fixtures directory."""

    def _loader(filename: str):
        path = Path(__file__).parent.parent / "fixtures" / filename

        if not path.exists():
            raise FileNotFoundError(f"Fixture not found: {path}")

        suffix = path.suffix.lower()

        if suffix == ".json":
            rows = json.loads(path.read_text(encoding="utf-8"))
            return spark.createDataFrame(rows)

        if suffix == ".csv":
            with path.open(newline="", encoding="utf-8") as file:
                rows = list(csv.DictReader(file))
            return spark.createDataFrame(rows)

        raise ValueError(f"Unsupported fixture type: {filename}")

    return _loader
