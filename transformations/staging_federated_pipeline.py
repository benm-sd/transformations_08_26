"""
staging_federated_pipeline.py
================================================================================
Config-Driven Federated Staging — Lakeflow Spark Declarative Pipelines
================================================================================

PURPOSE: Read from FOREIGN CATALOG tables (Lakehouse Federation) into a
         private MV that holds the federated read, then APPEND those rows
         into a public streaming table that Bronze can stream from without
         needing skipChangeCommits.
LOAD MODES:

  full        — read entire foreign table every run. Private MV holds the
                 full source. Public streaming table appends the full
                 source each run → Bronze sees DUPLICATES on every refresh.
                 Suitable for: small immutable reference tables, OR when
                 Silver's SCD2 apply_changes will dedupe downstream.

  incremental — read only rows where watermark > previous max. Private MV
                 holds the delta. Public streaming table appends cleanly,
                 no duplicates. Watermark state is MAX(watermark_col) read
                 from the private MV itself.


WATERMARK PATTERNS (incremental mode only):

  watermark_column     — existing column on the foreign table. NULLs cause
                         rows to be silently dropped.
  watermark_expression — synthesize from a SQL expression, materialized
                         into the MV as a _federated_watermark column so
                         the next run's MAX() can read it back.


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
        f"Config directory not found: '{_CONFIG_DIR}'."
    )

_YAML_FILES = sorted(glob.glob(os.path.join(_CONFIG_DIR, "*_foreign.yml")))

if not _YAML_FILES:
    raise FileNotFoundError(
        f"No *_foreign.yml files found in '{_CONFIG_DIR}'. "
        "Federated sources must use the _foreign.yml suffix."
    )


def _load_source(path: str) -> dict:
    with open(path, "r") as f:
        data = yaml.safe_load(f)
    return data[0] if isinstance(data, list) else data


_SOURCES = [_load_source(f) for f in _YAML_FILES]


# ─────────────────────────────────────────────────────────────────────────────
# Constants & naming helpers
# ─────────────────────────────────────────────────────────────────────────────

_VALID_LOAD_MODES          = {"full", "incremental"}
_SYNTHESIZED_WATERMARK_COL = "_federated_watermark"
_PRIVATE_MV_PREFIX         = "_"
_PRIVATE_MV_SUFFIX         = "_federated"


def _private_mv_fqn(src: dict) -> str:
    """FQN of the private federated MV (not consumed by Bronze)."""
    return (
        f"{src['staging_catalog']}"
        f".{src['staging_schema']}"
        f".{_PRIVATE_MV_PREFIX}{src['source_table']}{_PRIVATE_MV_SUFFIX}"
    )


def _public_staging_fqn(src: dict) -> str:
    """FQN of the public append-only staging table (Bronze reads this)."""
    return (
        f"{src['staging_catalog']}"
        f".{src['staging_schema']}"
        f".{src['source_table']}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Validation
# ─────────────────────────────────────────────────────────────────────────────

def _validate_sources(sources: list, yaml_files: list):
    seen_names = set()

    for src, filepath in zip(sources, yaml_files):
        filename = os.path.basename(filepath)
        name     = src.get("source_table", "<unnamed>")

        if not src.get("source_table"):
            raise ValueError(f"[{filename}] Missing required field 'source_table'.")
        if name in seen_names:
            raise ValueError(f"[{filename}] Duplicate source_table '{name}'.")
        seen_names.add(name)

        if not src.get("staging_catalog") or not src.get("staging_schema"):
            raise ValueError(
                f"[{filename}] Missing staging_catalog or staging_schema for '{name}'."
            )

        fed = src.get("federated_source") or {}
        if not fed:
            raise ValueError(
                f"[{filename}] Missing 'federated_source' block for '{name}'."
            )

        if not fed.get("foreign_fqn"):
            raise ValueError(
                f"[{filename}] Missing federated_source.foreign_fqn for '{name}'. "
                "Provide the three-part name of the foreign catalog table, "
                "e.g. sqlserver_prod.dbo.customers."
            )

        mode = fed.get("load_mode", "full").lower()
        if mode not in _VALID_LOAD_MODES:
            raise ValueError(
                f"[{filename}] Invalid federated_source.load_mode '{mode}' for "
                f"'{name}'. Valid: {sorted(_VALID_LOAD_MODES)}."
            )

        if mode == "incremental":
            has_col  = bool(fed.get("watermark_column"))
            has_expr = bool(fed.get("watermark_expression"))
            if not (has_col or has_expr):
                raise ValueError(
                    f"[{filename}] Incremental mode for '{name}' requires either "
                    "federated_source.watermark_column or "
                    "federated_source.watermark_expression."
                )
            if has_col and has_expr:
                raise ValueError(
                    f"[{filename}] '{name}' has both watermark_column and "
                    "watermark_expression — set only one."
                )


_validate_sources(_SOURCES, _YAML_FILES)


# ─────────────────────────────────────────────────────────────────────────────
# Watermark lookup — read MAX(watermark) from the PRIVATE MV
# Returns None on first run (MV doesn't exist) → no filter → full initial load.
# ─────────────────────────────────────────────────────────────────────────────

def _read_last_watermark(private_mv_fqn: str, watermark_col_in_mv: str):
    try:
        row = spark.sql(
            f"SELECT MAX({watermark_col_in_mv}) AS max_wm FROM {private_mv_fqn}"
        ).first()
        return row["max_wm"] if row and row["max_wm"] is not None else None
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Helper: add provenance columns matching Auto Loader staging shape
# ─────────────────────────────────────────────────────────────────────────────

def _add_provenance(df):
    return (
        df
        .withColumn("_staging_ingested_at",  F.current_timestamp())
        .withColumn("_source_file_path",     F.lit(None).cast("string"))
        .withColumn("_source_file_name",     F.lit(None).cast("string"))
        .withColumn("_source_file_size",     F.lit(None).cast("long"))
        .withColumn("_source_file_modified", F.lit(None).cast("timestamp"))
    )


# ─────────────────────────────────────────────────────────────────────────────
# Dynamic registration: PRIVATE MV + PUBLIC streaming table per source
# ─────────────────────────────────────────────────────────────────────────────

for src in _SOURCES:

    private_mv = _private_mv_fqn(src)
    public_st  = _public_staging_fqn(src)
    fed        = src["federated_source"]
    load_mode  = fed.get("load_mode", "full").lower()

    _common_props = {
        "pipelines.autoOptimize.managed": "true",
        "ingestion.source_type":          "federated",
        "ingestion.foreign_fqn":          fed["foreign_fqn"],
        "ingestion.load_mode":            load_mode,
        **{f"source.{k}": str(v) for k, v in src.get("tags", {}).items()},
    }

    # ─────────────────────────────────────────────────────────────────────────
    # PRIVATE MV — branches by load_mode.
    #
    # full mode:        simple full table read, optional where/select.
    # incremental mode: same, plus watermark filter and (if using
    #                   watermark_expression) a materialized watermark column.
    # ─────────────────────────────────────────────────────────────────────────

    if load_mode == "full":

        @dp.materialized_view(
            name             = private_mv,
            comment          = f"PRIVATE federated full snapshot for {src['source_table']} — do not query directly. Consumed by {public_st}.",
            table_properties = {
                **_common_props,
                "quality":                "staging_internal",
                "ingestion.public_table": public_st,
            },
        )
        def staging_federated_private_mv_full(src=src):
            fed = src["federated_source"]
            df  = spark.read.table(fed["foreign_fqn"])

            if fed.get("where_clause"):
                df = df.where(fed["where_clause"])

            if fed.get("select_columns"):
                df = df.select(*fed["select_columns"])

            return _add_provenance(df)

    else:  # incremental

        @dp.materialized_view(
            name             = private_mv,
            comment          = f"PRIVATE federated delta for {src['source_table']} — do not query directly. Consumed by {public_st}.",
            table_properties = {
                **_common_props,
                "quality":                        "staging_internal",
                "ingestion.public_table":         public_st,
                "ingestion.watermark_column":     fed.get("watermark_column", ""),
                "ingestion.watermark_expression": fed.get("watermark_expression", ""),
            },
        )
        def staging_federated_private_mv_incremental(src=src, private_mv=private_mv):
            fed = src["federated_source"]
            df  = spark.read.table(fed["foreign_fqn"])

            # ── Watermark filter ─────────────────────────────────────────────
            if fed.get("watermark_column"):
                # Existing foreign column. Used as-is.
                wm_col_in_mv = fed["watermark_column"]
            else:
                # Synthesize from expression. Materialize into MV so next
                # run's MAX() can read it back as state.
                wm_col_in_mv = _SYNTHESIZED_WATERMARK_COL
                df = df.withColumn(
                    _SYNTHESIZED_WATERMARK_COL,
                    F.expr(fed["watermark_expression"]),
                )

            last_seen = _read_last_watermark(private_mv, wm_col_in_mv)
            if last_seen is not None:
                df = df.where(
                    F.col(wm_col_in_mv)
                    > F.lit(last_seen).cast(df.schema[wm_col_in_mv].dataType)
                )

            # ── Optional pre-filter ──────────────────────────────────────────
            if fed.get("where_clause"):
                df = df.where(fed["where_clause"])

            # ── Optional column pinning ──────────────────────────────────────
            # Synthesized watermark column MUST survive the projection.
            if fed.get("select_columns"):
                cols = list(fed["select_columns"])
                if (
                    not fed.get("watermark_column")
                    and _SYNTHESIZED_WATERMARK_COL not in cols
                ):
                    cols.append(_SYNTHESIZED_WATERMARK_COL)
                df = df.select(*cols)

            return _add_provenance(df)

    # ─────────────────────────────────────────────────────────────────────────
    # PUBLIC streaming table — what Bronze reads.
    # Identical for both load modes.
    # ─────────────────────────────────────────────────────────────────────────

    dp.create_streaming_table(
        name             = public_st,
        comment          = src.get("description", f"Federated staging (append-only): {src['source_table']}"),
        table_properties = {
            **_common_props,
            "quality":              "staging",
            "ingestion.private_mv": private_mv,
        },
    )

    @dp.append_flow(
        target = public_st,
        name   = f"{src['source_table']}_federated_append_flow",
    )
    def append_federated(private_mv=private_mv):
        return (
            spark.readStream
            .option("ignoreChanges", "true")  # tolerate MV rewrites
            .table(private_mv)
        )