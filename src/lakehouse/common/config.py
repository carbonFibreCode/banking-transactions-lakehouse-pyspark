"""Pipeline configuration loading."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[3] / "config" / "pipeline.yaml"


@dataclass(frozen=True)
class PipelineConfig:
    raw: dict[str, Any]
    base_path: str

    def path(self, layer: str, table: str) -> str:
        """Physical location of a table, e.g. path("silver", "transactions")."""
        layer_dir = self.raw["layers"][layer]
        return f"{self.base_path.rstrip('/')}/{layer_dir}/{table}"

    def source(self, name: str) -> dict[str, Any]:
        try:
            return self.raw["sources"][name]
        except KeyError as exc:
            raise KeyError(f"Source '{name}' is not declared in the pipeline config") from exc

    @property
    def source_names(self) -> list[str]:
        return list(self.raw["sources"])

    def quality(self, dataset: str) -> dict[str, Any]:
        return self.raw.get("quality", {}).get(dataset, {"rules": [], "max_error_rate": 1.0})

    def pii(self, dataset: str) -> dict[str, list[str]]:
        return self.raw.get("pii", {}).get(dataset, {})

    @property
    def processing(self) -> dict[str, Any]:
        return self.raw["processing"]

    @property
    def fx_rates(self) -> dict[str, float]:
        return self.raw["fx_rates_to_gbp"]


def _read_text(location: str) -> str:
    if location.startswith("s3://"):
        import boto3

        bucket, _, key = location[len("s3://") :].partition("/")
        body = boto3.client("s3").get_object(Bucket=bucket, Key=key)["Body"]
        return body.read().decode("utf-8")
    return Path(location).read_text(encoding="utf-8")


def load_config(location: str | Path | None = None, base_path: str | None = None) -> PipelineConfig:
    """Load config. Precedence for base_path: argument > LAKEHOUSE_BASE_PATH env > YAML."""
    raw = yaml.safe_load(_read_text(str(location or DEFAULT_CONFIG_PATH)))
    resolved_base = base_path or os.environ.get("LAKEHOUSE_BASE_PATH") or raw["base_path"]
    return PipelineConfig(raw=raw, base_path=str(resolved_base))
