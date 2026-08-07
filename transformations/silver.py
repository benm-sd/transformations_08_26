"""
silver_pipeline.py
================================================================================
Config-Driven Silver Layer — Lakeflow Spark Declarative Pipelines
================================================================================

PURPOSE: Reads from Bronze streaming tables, applies SCD Type 2 via
         apply_changes. Produces clean, history-tracked Silver tables.

================================================================================
"""

import os
import re
import glob
import yaml
from pyspark import pipelines as dp
from pyspark.sql import functions as F


# ─────────────────────────────────────────────────────────────────────────────
# Config folder scan
# ─────────────────────────────────────────────────────────────────────────────

_CONFIG_DIR = _CONFIG_DIR = "/Workspace/Users/allen@sunnydata.ai/New Pipeline 2026-05-19 11:14/utilities"

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
# Validates all fields Silver uses. Clear errors for the associate.
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

        if not src.get("source_catalog"):
            raise ValueError(
                f"[{filename}] Missing required field 'source_catalog' for '{name}'. "
                "Silver needs to know which catalog the Bronze table lives in."
            )

        if not src.get("source_schema"):
            raise ValueError(
                f"[{filename}] Missing required field 'source_schema' for '{name}'. "
                "Silver needs to know which schema the Bronze table lives in."
            )

        if not src.get("target_catalog_prefix"):
            raise ValueError(
                f"[{filename}] Missing required field 'target_catalog_prefix' for '{name}'. "
                "Silver needs this to build the fully-qualified target table name."
            )

        if not src.get("target_schema"):
            raise ValueError(
                f"[{filename}] Missing required field 'target_schema' for '{name}'."
            )

        if not src.get("scd_2_key_list"):
            raise ValueError(
                f"[{filename}] Missing required field 'scd_2_key_list' for '{name}'. "
                "Provide at least one column that uniquely identifies a record. "
                "If one column is not unique, add a second to make the combination unique."
            )

        for col_def in (src.get("silver_columns") or []):
            if not col_def.get("name"):
                raise ValueError(
                    f"[{filename}] Every entry in 'silver_columns' for '{name}' "
                    "must have a 'name' field."
                )

        for rule in (src.get("data_quality_rules") or []):
            if not rule.get("description") or not rule.get("expr"):
                raise ValueError(
                    f"[{filename}] Every entry in 'data_quality_rules' for '{name}' "
                    "must have both a 'description' and an 'expr' field."
                )

        scd_seq = src.get("history_timestamp_source") or "pipeline_timestamp"
        if scd_seq != "pipeline_timestamp" and src.get("silver_columns"):
            silver_col_names = [c["name"] for c in (src.get("silver_columns") or [])]
            if scd_seq not in silver_col_names:
                raise ValueError(
                    f"[{filename}] history_timestamp_source '{scd_seq}' for '{name}' is not "
                    "listed in silver_columns. Add it to silver_columns (with or "
                    "without a rename) so it propagates into Silver and can drive "
                    "__START_AT / __END_AT. Or set history_timestamp_source: pipeline_timestamp "
                    "to use the pipeline run time instead."
                )

        scd_2_exclude = src.get("scd_2_exclude_list") or []
        if not isinstance(scd_2_exclude, list):
            raise ValueError(
                f"[{filename}] 'scd_2_exclude_list' for '{name}' must be a list of "
                "Bronze column names (strings)."
            )
        for col in scd_2_exclude:
            if not isinstance(col, str) or not col:
                raise ValueError(
                    f"[{filename}] All entries in 'scd_2_exclude_list' for '{name}' "
                    "must be non-empty strings."
                )

        scd_type = src.get("scd_type", 2)
        if scd_type not in (1, 2):
            raise ValueError(
                f"[{filename}] Invalid scd_type '{scd_type}' for '{name}'. "
                "Valid values: 1 or 2."
            )


_validate_sources(_SOURCES, _YAML_FILES)


# ─────────────────────────────────────────────────────────────────────────────
# Stream helpers
# ─────────────────────────────────────────────────────────────────────────────

def _normalize_column_name(name: str) -> str:
    """Remove all non-alphanumeric characters (whitespace, punctuation, symbols).

    Applied to every Silver column name and Silver table name so the Silver
    schema and table identifiers contain only [a-zA-Z0-9]. Aggressive — also
    strips underscores, so `payment_id` becomes `paymentid`. Use clean source
    naming in silver_columns[].rename if that matters.
    """
    return re.sub(r"[^a-zA-Z0-9]", "", name)


def _silver_col_name(col_def: dict) -> str:
    """User-facing Silver column name (pre-normalize).
    """
    return col_def.get("rename") or col_def["name"]


def _translate_expr_columns(expr: str, rename_map: dict) -> str:
    """Generic SQL expression rewrite: replace occurrences of each key in
    rename_map with its value. Handles both backticked and bare identifier
    forms. Output is always backticked for safety (works even if the target
    name contains spaces or SQL keywords).
    """
    for from_name, to_name in sorted(rename_map.items(), key=lambda x: -len(x[0])):
        # Replace backticked references first (literal match, no regex needed)
        expr = expr.replace(f"`{from_name}`", f"`{to_name}`")
        # Then replace bare identifier references via word boundary
        expr = re.sub(rf"\b{re.escape(from_name)}\b", f"`{to_name}`", expr)
    return expr


def _translate_expr_to_bronze(expr: str, silver_columns: list) -> str:
    """Rewrite a DQ expression that references user-silver column names so it
    references original Bronze column names instead. Used for the Quarantine
    path which preserves raw Bronze schema.
    """
    reverse_rename = {}
    for c in (silver_columns or []):
        bronze_name = c["name"]
        silver_name = _silver_col_name(c)
        if silver_name != bronze_name:
            reverse_rename[silver_name] = bronze_name
    return _translate_expr_columns(expr, reverse_rename)


def _build_silver_stream(src: dict):
    """Streaming read of Bronze with renames, select, and exclude applied.
    Returns a streaming DataFrame ready for apply_changes.
    """
    bronze_fqn = (
        f"{src['source_catalog']}"
        f".{src['source_schema']}"
        f".{src['source_table']}"
    )

    df = spark.readStream.table(bronze_fqn)

    silver_cols_cfg = src.get("silver_columns") or []

    # DQ gate: keep only rows that pass every rule — exact complement of the quarantine filter.
    df = df.filter(F.expr(" AND ".join(f"(({_translate_expr_to_bronze(r['expr'], silver_cols_cfg)}) IS NOT FALSE)" for r in (src.get('data_quality_rules') or [])) or "true"))

    # Build rename map: bronze_name -> user_silver_name (no normalize here).
    rename_map = {}
    for col_def in silver_cols_cfg:
        bronze_name = col_def["name"]
        silver_name = _silver_col_name(col_def)
        if silver_name != bronze_name:
            rename_map[bronze_name] = silver_name

    # Apply renames
    for orig, final in rename_map.items():
        df = df.withColumnRenamed(orig, final)

    if silver_cols_cfg:
        allow_missing = bool(src.get("allow_missing_columns", False))
        if allow_missing:
            existing = set(df.columns)
            select_exprs = []
            for c in silver_cols_cfg:
                user_silver = _silver_col_name(c)
                if user_silver in existing:
                    select_exprs.append(F.col(user_silver))
                else:
                    select_exprs.append(F.lit(None).cast("string").alias(user_silver))
            df = df.select(*select_exprs)
        else:
            df = df.select(*[_silver_col_name(c) for c in silver_cols_cfg])

    # Drop excluded columns
    for col in (src.get("exclude_columns") or []):
        if col in df.columns:
            df = df.drop(col)

    # Stamp _silver_processed_at if pipeline_timestamp mode is selected.
    scd_seq = src.get("history_timestamp_source") or "pipeline_timestamp"
    if scd_seq == "pipeline_timestamp":
        df = df.withColumn("_silver_processed_at", F.current_timestamp())

    return df


def _build_quarantine_stream(src: dict):
    """Reads Bronze as a STREAM and quarantines DQ-failing rows with FULL raw
    Bronze context preserved — no renames, no select, no exclude.
    """
    bronze_fqn = (
        f"{src['source_catalog']}"
        f".{src['source_schema']}"
        f".{src['source_table']}"
    )

    df = spark.readStream.table(bronze_fqn)

    # Tag each row with the names of rules it fails.
    silver_cols_cfg = src.get("silver_columns") or []
    rules           = src.get("data_quality_rules") or []
    failed_exprs    = [
        F.when(
            ~F.expr(_translate_expr_to_bronze(rule["expr"], silver_cols_cfg)),
            F.lit(rule["description"])
        )
        for rule in rules
    ]

    df = (
        df
        .withColumn("_failed_dq_rules",     F.array_compact(F.array(*failed_exprs)))
        .withColumn("_quarantine_timestamp", F.current_timestamp())
        .withColumn("_source_table",         F.lit(src["source_table"]))
        .filter(F.size(F.col("_failed_dq_rules")) > 0)
    )
    return df


# ─────────────────────────────────────────────────────────────────────────────
# Dynamic Silver table registration
# ─────────────────────────────────────────────────────────────────────────────

for src in _SOURCES:

    table_name              = src["source_table"]
    silver_table            = _normalize_column_name(table_name)
    silver_stream_view_name = f"{table_name}_silver_stream"
    scd_type                = src.get("scd_type", 2)

    # Fully-qualified names — three-part UC path is the only way to override
    # the pipeline-level catalog/schema defaults in create_streaming_table.
    silver_fqn = (
        f"{src['target_catalog_prefix']}"
        f".{src['target_schema']}"
        f".{silver_table}"
    )
    quarantine_fqn = (
        f"{src['target_catalog_prefix']}"
        f".{src['target_schema']}"
        f".{silver_table}_qtn"
    )

    # User-silver column metadata. These names match what the YAML uses in
    # scd_2_key_list, history_timestamp_source, and DQ rules.
    silver_cols_cfg  = src.get("silver_columns") or []
    rename_map       = {
        c["name"]: _silver_col_name(c)
        for c in silver_cols_cfg
        if _silver_col_name(c) != c["name"]
    }
    silver_cols_user = [_silver_col_name(c) for c in silver_cols_cfg]

    # Boundary renames: user-silver → normalized. Applied at the END of the
    # silver stream view (step A) so apply_changes writes a normalized Silver
    # schema while upstream renames/select/exclude all use friendlier user-silver
    # names. Only includes columns where normalize() actually changes the name.
    cdf_renames = {
        user: _normalize_column_name(user)
        for user in silver_cols_user
        if _normalize_column_name(user) != user
    }
    silver_cols = [cdf_renames.get(u, u) for u in silver_cols_user]

    if silver_cols_cfg:
        def to_silver(bronze_name, _rm=rename_map, _cdf=cdf_renames):
            user_silver = _rm.get(bronze_name, bronze_name)
            return _cdf.get(user_silver, user_silver)
    else:
        def to_silver(bronze_name):
            return _normalize_column_name(bronze_name)

    # DQ expectations: translate column refs in each expr to their final Silver
    # form (normalized + renamed if applicable).
    if silver_cols_cfg:
        dq_expectations = {
            rule["description"]: _translate_expr_columns(rule["expr"], cdf_renames)
            for rule in (src.get("data_quality_rules") or [])
        }
    else:
        def _build_implicit_dq_renames(rules):
            referenced = set()
            for rule in rules:
                referenced.update(re.findall(r"`([^`]+)`", rule["expr"]))
            return {n: _normalize_column_name(n) for n in referenced
                    if _normalize_column_name(n) != n}
        implicit_dq_renames = _build_implicit_dq_renames(
            src.get("data_quality_rules") or []
        )
        dq_expectations = {
            rule["description"]: _translate_expr_columns(rule["expr"], implicit_dq_renames)
            for rule in (src.get("data_quality_rules") or [])
        }

    # ── A: Silver stream view (temporary — not persisted to UC) ────────────────
    @dp.temporary_view(name=silver_stream_view_name)
    def silver_stream(src=src,
                      silver_cols_cfg=silver_cols_cfg,
                      cdf_renames=cdf_renames):
        df = _build_silver_stream(src)
        if silver_cols_cfg:
            for user_name, norm_name in cdf_renames.items():
                df = df.withColumnRenamed(user_name, norm_name)
        else:
            for c in list(df.columns):
                if c.startswith("_"):
                    continue
                norm = _normalize_column_name(c)
                if norm != c:
                    df = df.withColumnRenamed(c, norm)
        return df

    # ── B: Final Silver target table (persistent) ─────────────────────────────
    cluster_by_normalized = [to_silver(c) for c in (src.get("cluster_by") or [])]
    dp.create_streaming_table(
        name               = silver_fqn,
        comment            = src.get("description", f"Silver SCD{scd_type} table: {table_name}"),
        cluster_by         = cluster_by_normalized,
        expect_all_or_drop = dq_expectations,
        table_properties   = {
            "quality":                        "silver",
            "pipelines.autoOptimize.managed": "true",
            "silver.scd_type":                str(scd_type),
            **{f"source.{k}": str(v) for k, v in src.get("tags", {}).items()},
        },
    )

    # ── C: apply_changes (streaming) → final Silver ───────────────────────────
    keys_normalized = [to_silver(k) for k in src["scd_2_key_list"]]

    scd_seq = src.get("history_timestamp_source") or "pipeline_timestamp"
    if scd_seq == "pipeline_timestamp":
        sequence_by_col = "_silver_processed_at"
        except_cols     = ["_silver_processed_at"]
    else:
        sequence_by_col = to_silver(scd_seq)
        except_cols     = []

    # Track-history strategy depends on whether silver_columns was specified:
    #   Explicit silver_columns: track ONLY those columns (changes elsewhere are no-ops).
    #   Omitted silver_columns ("SELECT *" mode): track EVERY non-system column.
    #     Use track_history_except_column_list with the standard audit columns so
    #     re-ingestion timestamps and source file metadata don't trigger spurious
    #     SCD2 history rows when the same business data lands in a new file.
    #
    # _silver_processed_at is only included if we actually created it (i.e., when
    # history_timestamp_source = pipeline_timestamp). Listing a non-existent
    # column in except_column_list raises UNRESOLVED_COLUMN at validation time.
    _SYSTEM_AUDIT_COLS = [
        "_staging_ingested_at",
        "_source_file_path",
        "_source_file_name",
        "_source_file_size",
        "_source_file_modified",
        "_bronze_ingested_at",
    ]
    if scd_seq == "pipeline_timestamp":
        _SYSTEM_AUDIT_COLS = _SYSTEM_AUDIT_COLS + ["_silver_processed_at"]

    scd_2_exclude_normalized = [to_silver(c) for c in (src.get("scd_2_exclude_list") or [])]

    if silver_cols_cfg:
        silver_cols_tracked = [c for c in silver_cols if c not in scd_2_exclude_normalized]
        track_history_kwargs = {"track_history_column_list": silver_cols_tracked}
    else:
        track_history_kwargs = {
            "track_history_except_column_list": _SYSTEM_AUDIT_COLS + scd_2_exclude_normalized
        }

    dp.apply_changes(
        target                    = silver_fqn,
        source                    = silver_stream_view_name,
        keys                      = keys_normalized,
        sequence_by               = sequence_by_col,
        stored_as_scd_type        = scd_type,
        except_column_list        = except_cols,
        **track_history_kwargs,
    )

    # ── D + E: Quarantine table + append flow (only when DQ rules are defined) 
    if dq_expectations:

        dp.create_streaming_table(
            name             = quarantine_fqn,
            comment          = f"Quarantine: rows from {table_name} that failed data quality rules.",
            table_properties = {
                "quality":                        "quarantine",
                "pipelines.autoOptimize.managed": "true",
                **{f"source.{k}": str(v) for k, v in src.get("tags", {}).items()},
            },
        )

        @dp.append_flow(target=quarantine_fqn, name=f"{silver_table}_quarantine_flow")
        def quarantine_flow(src=src):
            return _build_quarantine_stream(src)


# ─────────────────────────────────────────────────────────────────────────────
# Silver manifest — temporary view (pipeline-scoped, not persisted to UC)
# Query during a pipeline run to audit which sources are active.
# ─────────────────────────────────────────────────────────────────────────────

@dp.temporary_view(name="_silver_manifest")
def silver_manifest():
    rows = [
        {
            "source_table":      s["source_table"],
            "source_catalog":    s.get("source_catalog", ""),
            "source_schema":     s.get("source_schema", ""),
            "target_catalog":    s.get("target_catalog_prefix", ""),
            "target_schema":     s.get("target_schema", ""),
            "scd_type":          str(s.get("scd_type", 2)),
            "scd_2_key_list":    str(s.get("scd_2_key_list", [])),
            "sequence_by":       s.get("sequence_by", ""),
            "history_timestamp_source":   s.get("history_timestamp_source") or "pipeline_timestamp",
            "silver_columns":    str([c["name"] for c in (s.get("silver_columns") or [])]),
            "silver_renames":    str({c["name"]: c["rename"] for c in (s.get("silver_columns") or []) if c.get("rename")}),
            "exclude_columns":   str(s.get("exclude_columns") or []),
            "cluster_by":        str(s.get("cluster_by") or []),
            "dq_rules":          str([r["description"] for r in (s.get("data_quality_rules") or [])]),
            "config_file":       os.path.basename(f),
            "tags":              str(s.get("tags", {})),
        }
        for s, f in zip(_SOURCES, _YAML_FILES)
    ]
    return spark.createDataFrame(rows)