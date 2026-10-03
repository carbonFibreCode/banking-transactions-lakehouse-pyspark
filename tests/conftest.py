from __future__ import annotations

import os
import time

import pytest

os.environ["TZ"] = "UTC"
time.tzset()

from lakehouse.common.config import load_config
from lakehouse.common.spark import get_spark


@pytest.fixture(scope="session")
def spark():
    session = get_spark("lakehouse-tests", shuffle_partitions=2, master="local[2]")
    yield session
    session.stop()


@pytest.fixture
def cfg(tmp_path):
    return load_config(base_path=str(tmp_path / "lake"))


def _none_safe(t):
    return [(v is None, str(v)) for v in t]


def rows(df, *cols):
    """Collect (selected columns of) a DataFrame as a sorted list of tuples for assertions."""
    selected = df.select(*cols) if cols else df
    return sorted((tuple(r) for r in selected.collect()), key=_none_safe)
