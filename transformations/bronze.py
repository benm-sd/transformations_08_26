"""
bronze_pipeline.py
================================================================================
Config-Driven Bronze Layer — Lakeflow Spark Declarative Pipelines
================================================================================

PURPOSE: Reads from Staging streaming tables, pure append.
         Adds bronze-layer metadata columns.
         No CDC, no deduplication, no transformation.
         Silver reads from the tables this pipeline produces.

================================================================================
"""

import os
import glob
import yaml
from pyspark import pipelines as dp
from pyspark.sql import functions as F


# ─────────────────────────────────────────────────────────────────────────────
# Config folder scan
# ─────────────────────────────────────────────────────────────────────────────

_CONFIG_DIR = "/Workspace/Users/allen@sunnydata.ai/New Pipeline 2026-05-19 11:14/utilities"

if not os.path.isdir(_CONFIG_DIR):
    raise FileNotFoundError(
        f"Config directory not found: '{_CONFIG_DIR}'. "
        "Set pipeline_param.config_dir to the full path of your sources/ folder."
    )

_YAML_FILES = sorted(glob.glob(os.path.join(_CONFIG_DIR, "*.yml")))

if not _YAML_FILES:
    raise FileNotFoundError(
        f"No .yaml files found in '{_CONFIG_DIR}'."
    )

def _load_source(path: str) -> dict:
    with open(path, "r") as f:
        data = yaml.safe_load(f)
    if isinstance(data, list):
        return data[0]
    return data

_SOURCES = [_load_source(f) for f in _YAML_FILES]


# ─────────────────────────────────────────────────────────────────────────────
# Validation
# Validates fields Bronze uses: source_table, staging_catalog, staging_schema.
# ─────────────────────────────────────────────────────────────────────────────

def _validate_sources(sources: list, yaml_files: list):
    seen_names = set()

    for src, filepath in zip(sources, yaml_files):
        filename = os.path.basename(filepath)
        name     = src.get("source_table", "<unnamed>")

        if not src.get("source_table"):
            raise ValueError(
                f"[{filename}] Missing required field 'source_table'."
            )
        if name in seen_names:
            raise ValueError(
                f"[{filename}] Duplicate source_table '{name}'."
            )
        seen_names.add(name)

        if not src.get("staging_catalog"):
            raise ValueError(
                f"[{filename}] Missing required field 'staging_catalog' for '{name}'. "
                "Bronze needs to know which catalog the Staging table lives in."
            )

        if not src.get("staging_schema"):
            raise ValueError(
                f"[{filename}] Missing required field 'staging_schema' for '{name}'. "
                "Bronze needs to know which schema the Staging table lives in."
            )


_validate_sources(_SOURCES, _YAML_FILES)


# ─────────────────────────────────────────────────────────────────────────────
# Dynamic Bronze table registration
# ─────────────────────────────────────────────────────────────────────────────

for src in _SOURCES:

    @dp.table(
        # Three-part name overrides the pipeline-level catalog/schema defaults.
        name=f"{src['source_catalog']}.{src['source_schema']}.{src['source_table']}",
        comment=src.get("description", f"Bronze append table: {src['source_table']}"),
        table_properties={
            "quality":                        "bronze",
            "pipelines.autoOptimize.managed": "true",
            **{f"source.{k}": str(v) for k, v in src.get("tags", {}).items()},
        },
    )
    def ingest_bronze(src=src):
        staging_table = (
            f"{src['staging_catalog']}"
            f".{src['staging_schema']}"
            f".{src['source_table']}"
        )
        return (
            spark.readStream
            .table(staging_table)
            .withColumn("_bronze_ingested_at", F.current_timestamp())
        )


# ─────────────────────────────────────────────────────────────────────────────
# Ingestion manifest — temporary view (pipeline-scoped, not persisted to UC)
# Query during a pipeline run to audit which sources are active.
# ─────────────────────────────────────────────────────────────────────────────

@dp.temporary_view(name="_bronze_manifest")
def bronze_manifest():
    rows = [
        {
            "source_table":          s["source_table"],
            "staging_catalog":       s.get("staging_catalog", ""),
            "staging_schema":        s.get("staging_schema", ""),
            "source_catalog":        s.get("source_catalog", ""),
            "source_schema":         s.get("source_schema", ""),
            "target_catalog_prefix": s.get("target_catalog_prefix", ""),
            "target_schema":         s.get("target_schema", ""),
            "config_file":           os.path.basename(f),
            "tags":                  str(s.get("tags", {})),
        }
        for s, f in zip(_SOURCES, _YAML_FILES)
    ]
    return spark.createDataFrame(rows)