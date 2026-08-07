"""
staging_federated_rolling.py
================================================================================
Rolling-Window Federated Staging + Reconciliation Sweep — Lakeflow SDP
================================================================================

PURPOSE: Two deletion-detection controls over a keyless, source-deletable
         foreign table, emitted as one append-only change stream.

         bronze.py and silver_pipeline.py consume this WITHOUT MODIFICATION.

  CONTROL 1 — ROLLING WINDOW (every run)
      Re-read the trailing N days. Diff against the previous snapshot. Rows that
      vanished while still inside the window are soft-deleted. Cheap, fast,
      catches the common case within one run.

      Blind spot: a row deleted AFTER it aged out of the window is invisible.
      The daily diff cannot distinguish "deleted at day 27" from "aged out at
      day 8" — by then the row is not being read at all.

  CONTROL 2 — RECONCILIATION SWEEP (periodic)
      Read the FULL source (or a wide scope), compare against every record
      Silver currently believes is live, and soft-delete the orphans. This is
      what closes the blind spot. Expensive, so it runs on a cadence rather
      than every run.

      The two controls do not double-count: once Control 1 soft-deletes a row,
      it leaves Silver's open set and the sweep will not see it as an orphan.

--------------------------------------------------------------------------------
HOW DELETES REACH SILVER WITHOUT TOUCHING SILVER
--------------------------------------------------------------------------------
silver_pipeline.py calls apply_changes() without apply_as_deletes, so it cannot
process hard-delete events. Deletes are therefore SOFT: a vanished row is
re-emitted with the same rowkey, a higher snapshotseq, and isdeleted flipped to
true. SCD2 closes the previous version (__END_AT) and opens a new one.

    live rows:  SELECT * FROM <silver> WHERE __END_AT IS NULL AND isdeleted = false
    lifespan:   __END_AT - __START_AT on the closed version

changetype distinguishes how a deletion was found — 'delete' (rolling window)
vs 'reconcile' (sweep) — which is worth having when explaining a close-out date
that lags the actual source deletion.

--------------------------------------------------------------------------------
SWEEP SAFETY — READ THIS BEFORE ENABLING
--------------------------------------------------------------------------------
A control that closes Silver records based on absence from a federated read is
only as trustworthy as that read. A connector hiccup, a permissions change, or a
source mid-reload can all return a partial result, and a naive sweep would then
soft-delete most of the table. Three circuit breakers, all configurable:

    min_source_rows   floor on rows returned by the full read
    max_delete_pct    ceiling on orphans as a share of Silver's open records
    on_breach         fail (default) or skip

Default is to FAIL the pipeline update on breach. A reconciliation control that
silently declines to run is worse than one that stops the line.

--------------------------------------------------------------------------------
COLUMN AND TABLE NAMING — NOT COSMETIC
--------------------------------------------------------------------------------
silver_pipeline._normalize_column_name() strips every non-alphanumeric character,
underscores included, and is applied to BOTH column names and the Silver table
name. Two consequences:

  1. Columns this file adds are bare lowercase alphanumeric. In "SELECT *" mode
     silver's stream rename loop skips "_"-prefixed columns, but to_silver()
     still normalizes them when resolving scd_2_key_list / scd_2_exclude_list:

         _row_key -> stays "_row_key" in the stream, resolved as "rowkey" -> UNRESOLVED_COLUMN
         rowkey   -> stays "rowkey"   in the stream, resolved as "rowkey" -> OK

     The provenance block (_staging_ingested_at, _source_file_*) is the
     exception: those names are hard-coded in silver's _SYSTEM_AUDIT_COLS.

  2. The sweep must locate the Silver table, and source_table 'loan_details'
     lands at Silver table 'loandetails'. _normalize_identifier() below mirrors
     silver's function. If that function ever changes, this must change with it.

--------------------------------------------------------------------------------
REQUIRED YAML SETTINGS FOR THE SILVER SIDE (validated at startup)
--------------------------------------------------------------------------------
    scd_2_key_list:           [rowkey]
    history_timestamp_source: snapshotseq        (or snapshotts — see below)
    scd_2_exclude_list:       [snapshotseq, snapshotts, windowstart, windowdays,
                               changetype, rowoccurrence]

scd_2_exclude_list is load-bearing. Every run re-delivers every in-window row
with a fresh snapshotseq; these columns land in silver's
track_history_except_column_list so an unchanged row updates in place instead of
opening a version. Drop any of them and every run fabricates a version per row.

__START_AT / __END_AT inherit the type of the sequence column. snapshotseq is
bigint epoch-millis, so intervals need arithmetic. Set history_timestamp_source
to snapshotts instead for timestamp-typed boundaries that datediff() understands
directly — both are accepted, both order identically.

Leave silver_columns UNSET.

================================================================================
"""

import os
import re
import glob
import yaml
from datetime import datetime, timedelta, timezone

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ─────────────────────────────────────────────────────────────────────────────
# Config folder scan
#
# Same folder bronze.py and silver_pipeline.py scan. Their *.yml glob picks up
# *_rolling.yml automatically, which is what lets them run unmodified. The
# existing staging_federated_pipeline.py globs *_foreign.yml and ignores these.
# ─────────────────────────────────────────────────────────────────────────────

_CONFIG_DIR = "/Workspace/Users/allen@sunnydata.ai/New Pipeline 2026-05-19 11:14/utilities"

if not os.path.isdir(_CONFIG_DIR):
    raise FileNotFoundError(f"Config directory not found: '{_CONFIG_DIR}'.")

_YAML_FILES = sorted(glob.glob(os.path.join(_CONFIG_DIR, "*_rolling.yml")))

if not _YAML_FILES:
    raise FileNotFoundError(
        f"No *_rolling.yml files found in '{_CONFIG_DIR}'. "
        "Rolling-window federated sources must use the _rolling.yml suffix."
    )


def _load_source(path: str) -> dict:
    with open(path, "r") as f:
        data = yaml.safe_load(f)
    return data[0] if isinstance(data, list) else data


_SOURCES = [_load_source(f) for f in _YAML_FILES]


# ─────────────────────────────────────────────────────────────────────────────
# Run stamp
#
# This module is re-executed at the start of every pipeline update, so _RUN_TS
# is evaluated once per update and is constant across every source and every row
# in that update. That is exactly what a snapshot identifier needs to be.
#
# Do NOT substitute F.current_timestamp() — it evaluates per row/per task and
# would shatter one snapshot into many sequence values.
# ─────────────────────────────────────────────────────────────────────────────

_RUN_TS  = datetime.now(timezone.utc).replace(microsecond=0)
_RUN_SEQ = int(_RUN_TS.timestamp() * 1000)


# Manual sweep override. Set in the pipeline configuration to force a
# reconciliation on the next update regardless of cadence:
#     sunnydata.reconcile.force  =  true
try:
    _FORCE_RECONCILE = str(
        spark.conf.get("sunnydata.reconcile.force", "false")
    ).strip().lower() == "true"
except Exception:
    _FORCE_RECONCILE = False


# ─────────────────────────────────────────────────────────────────────────────
# Constants & naming helpers
# ─────────────────────────────────────────────────────────────────────────────

_VALID_ANCHORS        = {"midnight_utc", "run_time"}
_VALID_IDENTITY_MODES = {"hash_all", "key_columns"}
_VALID_SEQUENCE_COLS  = {"snapshotseq", "snapshotts"}
_VALID_ON_BREACH      = {"fail", "skip"}
_WEEKDAYS             = {"MON": 0, "TUE": 1, "WED": 2, "THU": 3,
                         "FRI": 4, "SAT": 5, "SUN": 6}

_NULL_SENTINEL       = "<<NULL>>"
_HASH_SEP            = "||"
_DEFAULT_WINDOW_DAYS = 7
_DEFAULT_CADENCE     = 7

# Columns this file adds. Bare alphanumeric on purpose — see the header.
_CHANGE_COLS = [
    "rowhash", "contenthash", "rowoccurrence", "rowkey",
    "changetype", "isdeleted", "deletedat",
    "snapshotseq", "snapshotts", "windowstart", "windowdays",
]

# What the YAML must place in scd_2_exclude_list.
_MUST_EXCLUDE_FROM_HISTORY = [
    "snapshotseq", "snapshotts", "windowstart", "windowdays",
    "changetype", "rowoccurrence",
]

_RECONCILE_LOG_SCHEMA = (
    "source_table string, snapshotseq bigint, sweepts timestamp, "
    "trigger string, scope_days int, closed_count bigint, status string"
)


def _normalize_identifier(name: str) -> str:
    """Mirror of silver_pipeline._normalize_column_name().

    Applied by Silver to both column names and the Silver table name. Replicated
    here so the sweep can resolve where Silver actually wrote the table. Keep in
    lockstep with the Silver implementation.
    """
    return re.sub(r"[^a-zA-Z0-9]", "", name)


def _snapshot_mv_fqn(src: dict) -> str:
    """PRIVATE MV holding the current N-day federated read."""
    return f"{src['staging_catalog']}.{src['staging_schema']}._{src['source_table']}_snapshot"


def _deletes_mv_fqn(src: dict) -> str:
    """PRIVATE MV holding this run's rolling-window soft deletes."""
    return f"{src['staging_catalog']}.{src['staging_schema']}._{src['source_table']}_deletes"


def _reconcile_mv_fqn(src: dict) -> str:
    """PRIVATE MV holding this run's reconciliation soft deletes."""
    return f"{src['staging_catalog']}.{src['staging_schema']}._{src['source_table']}_reconcile"


def _reconcile_stats_mv_fqn(src: dict) -> str:
    """PRIVATE MV emitting one audit row per completed sweep."""
    return f"{src['staging_catalog']}.{src['staging_schema']}._{src['source_table']}_reconcile_stats"


def _reconcile_log_fqn(src: dict) -> str:
    """Sweep audit log. Doubles as cadence state — see _sweep_decision()."""
    return f"{src['staging_catalog']}.{src['staging_schema']}._{src['source_table']}_reconcile_log"


def _public_staging_fqn(src: dict) -> str:
    """PUBLIC append-only staging table. bronze.py reads this."""
    return f"{src['staging_catalog']}.{src['staging_schema']}.{src['source_table']}"


def _silver_fqn(src: dict) -> str:
    """Where silver_pipeline.py actually writes — table name is normalized."""
    return (
        f"{src['target_catalog_prefix']}"
        f".{src['target_schema']}"
        f".{_normalize_identifier(src['source_table'])}"
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
            raise ValueError(f"[{filename}] Missing 'federated_source' block for '{name}'.")

        if not fed.get("foreign_fqn"):
            raise ValueError(
                f"[{filename}] Missing federated_source.foreign_fqn for '{name}'. "
                "Provide the three-part name of the foreign catalog table, "
                "e.g. sqlserver_prod.dbo.transactions."
            )

        if not fed.get("window_column"):
            raise ValueError(
                f"[{filename}] Missing federated_source.window_column for '{name}'. "
                "The rolling window needs a date/timestamp column that determines "
                "whether a row is still inside the mutable region. It is also what "
                "separates a real deletion from a row that simply aged out."
            )

        days = fed.get("rolling_window_days", _DEFAULT_WINDOW_DAYS)
        if not isinstance(days, int) or days < 1:
            raise ValueError(
                f"[{filename}] federated_source.rolling_window_days for '{name}' "
                f"must be a positive integer. Got '{days}'."
            )

        anchor = str(fed.get("window_start_anchor", "midnight_utc")).lower()
        if anchor not in _VALID_ANCHORS:
            raise ValueError(
                f"[{filename}] Invalid federated_source.window_start_anchor "
                f"'{anchor}' for '{name}'. Valid: {sorted(_VALID_ANCHORS)}."
            )

        ri   = src.get("row_identity") or {}
        mode = str(ri.get("mode", "hash_all")).lower()
        if mode not in _VALID_IDENTITY_MODES:
            raise ValueError(
                f"[{filename}] Invalid row_identity.mode '{mode}' for '{name}'. "
                f"Valid: {sorted(_VALID_IDENTITY_MODES)}."
            )
        if mode == "key_columns" and not ri.get("key_columns"):
            raise ValueError(
                f"[{filename}] row_identity.mode 'key_columns' for '{name}' "
                "requires a non-empty row_identity.key_columns list."
            )
        if mode == "hash_all" and ri.get("key_columns"):
            raise ValueError(
                f"[{filename}] row_identity.key_columns is set for '{name}' but "
                "row_identity.mode is 'hash_all'."
            )
        for field in ("key_columns", "exclude_columns"):
            val = ri.get(field) or []
            if not isinstance(val, list) or any(not isinstance(c, str) or not c for c in val):
                raise ValueError(
                    f"[{filename}] row_identity.{field} for '{name}' must be a "
                    "list of non-empty column-name strings."
                )

        # ── Reconciliation ───────────────────────────────────────────────────
        rc = src.get("reconciliation") or {}
        if rc.get("enabled", False):

            for field in ("target_catalog_prefix", "target_schema"):
                if not src.get(field):
                    raise ValueError(
                        f"[{filename}] reconciliation is enabled for '{name}' but "
                        f"'{field}' is missing. The sweep compares the source "
                        "against Silver's open records and needs to locate the "
                        "Silver table."
                    )

            wd = rc.get("run_on_weekday")
            if wd is not None and str(wd).upper()[:3] not in _WEEKDAYS:
                raise ValueError(
                    f"[{filename}] Invalid reconciliation.run_on_weekday '{wd}' for "
                    f"'{name}'. Valid: {sorted(_WEEKDAYS)}."
                )

            cadence = rc.get("cadence_days", _DEFAULT_CADENCE)
            if not isinstance(cadence, int) or cadence < 1:
                raise ValueError(
                    f"[{filename}] reconciliation.cadence_days for '{name}' must be "
                    f"a positive integer. Got '{cadence}'."
                )

            max_age = rc.get("max_age_days")
            if max_age is not None:
                if not isinstance(max_age, int) or max_age < days:
                    raise ValueError(
                        f"[{filename}] reconciliation.max_age_days for '{name}' must "
                        f"be an integer >= rolling_window_days ({days}), or null for "
                        f"a full-table sweep. Got '{max_age}'. A sweep narrower than "
                        "the rolling window reconciles less than the cheap control "
                        "already covers."
                    )

            pct = rc.get("max_delete_pct", 5.0)
            if not isinstance(pct, (int, float)) or not (0 < float(pct) <= 100):
                raise ValueError(
                    f"[{filename}] reconciliation.max_delete_pct for '{name}' must be "
                    f"a number in (0, 100]. Got '{pct}'."
                )

            floor = rc.get("min_source_rows", 1)
            if not isinstance(floor, int) or floor < 1:
                raise ValueError(
                    f"[{filename}] reconciliation.min_source_rows for '{name}' must "
                    f"be a positive integer. Got '{floor}'."
                )

            breach = str(rc.get("on_breach", "fail")).lower()
            if breach not in _VALID_ON_BREACH:
                raise ValueError(
                    f"[{filename}] Invalid reconciliation.on_breach '{breach}' for "
                    f"'{name}'. Valid: {sorted(_VALID_ON_BREACH)}."
                )

        # ── Cross-check the settings Silver depends on ───────────────────────
        # Silver is unmodified, so a bad YAML would otherwise fail two pipelines
        # downstream with an opaque error. Catch it here.

        keys = src.get("scd_2_key_list") or []
        if keys != ["rowkey"]:
            raise ValueError(
                f"[{filename}] scd_2_key_list for '{name}' must be exactly ['rowkey'] "
                f"— the synthetic identity this pipeline emits. Got {keys}. The source "
                "has no natural key; to change what identity means, set "
                "row_identity.mode / row_identity.key_columns instead."
            )

        seq = src.get("history_timestamp_source")
        if seq not in _VALID_SEQUENCE_COLS:
            raise ValueError(
                f"[{filename}] history_timestamp_source for '{name}' must be one of "
                f"{sorted(_VALID_SEQUENCE_COLS)}. Got '{seq}'. Sequencing by "
                "pipeline_timestamp uses arrival time, which cannot order snapshots "
                "correctly on replay. Use snapshotts for timestamp-typed "
                "__START_AT/__END_AT, snapshotseq for bigint."
            )

        missing_excl = [
            c for c in _MUST_EXCLUDE_FROM_HISTORY
            if c not in (src.get("scd_2_exclude_list") or [])
        ]
        if missing_excl:
            raise ValueError(
                f"[{filename}] scd_2_exclude_list for '{name}' is missing "
                f"{missing_excl}. These columns change on every run. If they are "
                "tracked for history, every run opens a new SCD2 version for every "
                f"row. Required entries: {_MUST_EXCLUDE_FROM_HISTORY}."
            )

        if src.get("silver_columns"):
            required = {"rowkey", "snapshotseq", "snapshotts", "isdeleted", "deletedat"}
            listed   = {c.get("rename") or c.get("name") for c in src["silver_columns"]}
            gap      = sorted(required - listed)
            if gap:
                raise ValueError(
                    f"[{filename}] silver_columns is set for '{name}' but does not "
                    f"list {gap}. Silver's projection would drop them and the SCD key, "
                    "sequence, or delete flag would disappear. Either add them or "
                    "leave silver_columns unset."
                )


_validate_sources(_SOURCES, _YAML_FILES)


# ─────────────────────────────────────────────────────────────────────────────
# Window boundary
# ─────────────────────────────────────────────────────────────────────────────

def _window_start(fed: dict) -> datetime:
    """Inclusive lower bound of the rolling window, computed once per update."""
    days   = int(fed.get("rolling_window_days", _DEFAULT_WINDOW_DAYS))
    anchor = str(fed.get("window_start_anchor", "midnight_utc")).lower()

    base = (
        _RUN_TS
        if anchor == "run_time"
        else _RUN_TS.replace(hour=0, minute=0, second=0, microsecond=0)
    )
    return base - timedelta(days=days)


# ─────────────────────────────────────────────────────────────────────────────
# Row identity
# ─────────────────────────────────────────────────────────────────────────────

def _hash_expr(cols: list):
    """Deterministic SHA-256 over the given columns.

    Hashed in sorted order so a change in physical column ordering at the source
    does not change the hash. NULL maps to a sentinel, so the only collision risk
    is a genuine value of '<<NULL>>' (accepted).
    """
    parts = [
        F.coalesce(F.col(c).cast("string"), F.lit(_NULL_SENTINEL))
        for c in sorted(cols)
    ]
    return F.sha2(F.concat_ws(_HASH_SEP, *parts), 256)


def _identity_columns(src: dict, available: list) -> list:
    """Columns defining row identity, resolved against a projection."""
    ri   = src.get("row_identity") or {}
    mode = str(ri.get("mode", "hash_all")).lower()

    if mode == "key_columns":
        cols = list(ri["key_columns"])
    else:
        excluded = set(ri.get("exclude_columns") or [])
        cols     = [c for c in available if c not in excluded]

    missing = [c for c in cols if c not in available]
    if missing:
        raise ValueError(
            f"[{src['source_table']}] row_identity columns not present in the "
            f"projection: {missing}. Available: {sorted(available)}. If you pinned "
            "federated_source.select_columns, add them there."
        )
    if not cols:
        raise ValueError(f"[{src['source_table']}] row_identity resolved to zero columns.")
    return cols


def _add_identity(src: dict, df):
    """rowhash / rowoccurrence / rowkey. Must be IDENTICAL between the rolling
    snapshot and the reconciliation sweep, or the sweep would see every row as
    an orphan. Single implementation, called by both.
    """
    business_cols = list(df.columns)

    collisions = sorted(set(business_cols) & set(_CHANGE_COLS))
    if collisions:
        raise ValueError(
            f"[{src['source_table']}] Source columns collide with the change-stream "
            f"columns this pipeline adds: {collisions}. Rename them via "
            "federated_source.select_columns."
        )

    id_cols = _identity_columns(src, business_cols)
    df = df.withColumn("rowhash", _hash_expr(id_cols))

    # Rows sharing a rowhash are indistinguishable by definition, so any
    # assignment of 1..n is valid — only the multiplicity carries meaning, and
    # that is preserved.
    if src.get("allow_duplicate_rows", True):
        dup_window = Window.partitionBy("rowhash").orderBy(F.lit(1))
        df = df.withColumn("rowoccurrence", F.row_number().over(dup_window))
    else:
        df = df.withColumn("rowoccurrence", F.lit(1).cast("int"))

    return df.withColumn(
        "rowkey", F.concat_ws("#", F.col("rowhash"), F.col("rowoccurrence"))
    )


def _read_foreign(src: dict, since: datetime):
    """Foreign-table read with the shared projection and filters applied.

    `since` bounds the window column: the rolling window start for the daily
    control, the sweep scope for reconciliation, or None for the whole table.
    """
    fed  = src["federated_source"]
    wcol = fed["window_column"]

    df = spark.read.table(fed["foreign_fqn"])

    # Rows with a NULL window column are excluded everywhere: they can never be
    # evaluated for deletion and would otherwise oscillate in and out of scope.
    if since is not None:
        df = df.where(F.col(wcol).cast("timestamp") >= F.lit(since).cast("timestamp"))
    else:
        df = df.where(F.col(wcol).isNotNull())

    if fed.get("where_clause"):
        df = df.where(fed["where_clause"])

    if fed.get("select_columns"):
        cols = list(fed["select_columns"])
        if wcol not in cols:
            cols.append(wcol)
        df = df.select(*cols)

    return df


# ─────────────────────────────────────────────────────────────────────────────
# Stamping helpers
#
# Provenance column names must match silver's _SYSTEM_AUDIT_COLS exactly, so
# that unchanged re-deliveries do not open SCD2 versions.
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


def _add_snapshot_stamps(df, window_start: datetime, days: int):
    return (
        df
        .withColumn("snapshotseq",  F.lit(_RUN_SEQ).cast("long"))
        .withColumn("snapshotts",   F.lit(_RUN_TS).cast("timestamp"))
        .withColumn("windowstart",  F.lit(window_start).cast("timestamp"))
        .withColumn("windowdays",   F.lit(days).cast("int"))
    )


def _stamp_as_deleted(df, change_type: str, window_start: datetime, days: int):
    """Turn a last-known payload into a soft-delete event."""
    return _add_snapshot_stamps(
        df
        .withColumn("changetype", F.lit(change_type))
        .withColumn("isdeleted",  F.lit(True))
        .withColumn("deletedat",  F.lit(_RUN_TS).cast("timestamp"))
        .withColumn("_staging_ingested_at", F.current_timestamp()),
        window_start,
        days,
    )


# ─────────────────────────────────────────────────────────────────────────────
# State reads
#
# Both read committed state from this pipeline's own tables, the same escape
# hatch staging_federated_pipeline.py already uses for watermark state. Raw
# spark.sql is deliberate: it resolves at execution rather than during graph
# analysis, so it registers no dependency and cannot form a cycle.
#
# The `snapshotseq < _RUN_SEQ` predicate makes results independent of whether
# this run's own rows have committed yet. Without it, the answer would depend on
# the order the append flows and these MVs happen to finish in.
# ─────────────────────────────────────────────────────────────────────────────

def _read_prior_snapshot_seq(public_st: str):
    """snapshotseq of the most recent completed snapshot. None on initial load."""
    try:
        row = spark.sql(f"""
            SELECT MAX(snapshotseq) AS prior_seq
            FROM {public_st}
            WHERE snapshotseq < {_RUN_SEQ}
              AND changetype = 'upsert'
        """).first()
        return row["prior_seq"] if row and row["prior_seq"] is not None else None
    except Exception:
        return None


def _read_last_sweep(log_fqn: str):
    """Timestamp of the last completed sweep. None if none has run."""
    try:
        row = spark.sql(f"""
            SELECT MAX(sweepts) AS last_sweep
            FROM {log_fqn}
            WHERE snapshotseq < {_RUN_SEQ}
              AND status = 'completed'
        """).first()
        return row["last_sweep"] if row and row["last_sweep"] is not None else None
    except Exception:
        return None


def _sweep_decision(src: dict, log_fqn: str):
    """(due: bool, trigger: str). Evaluated identically by the sweep MV and the
    stats MV, so both agree within a run.

    Fires when ANY of:
      - the force flag is set in pipeline configuration
      - no sweep has ever completed (first run establishes the baseline)
      - run_on_weekday is set and today matches
      - cadence_days have elapsed since the last completed sweep

    run_on_weekday pins the sweep to a predictable low-traffic day; cadence_days
    is the backstop that still fires if that day's update failed.
    """
    rc = src.get("reconciliation") or {}
    if not rc.get("enabled", False):
        return False, "disabled"
    if _FORCE_RECONCILE:
        return True, "forced"

    last = _read_last_sweep(log_fqn)
    if last is None:
        return True, "initial"

    if last.tzinfo is None:
        last = last.replace(tzinfo=timezone.utc)

    wd = rc.get("run_on_weekday")
    if wd is not None and _RUN_TS.weekday() == _WEEKDAYS[str(wd).upper()[:3]]:
        # Don't re-fire on repeated same-day updates.
        if (_RUN_TS - last) >= timedelta(hours=20):
            return True, "weekday"

    cadence = int(rc.get("cadence_days", _DEFAULT_CADENCE))
    if (_RUN_TS - last) >= timedelta(days=cadence):
        return True, "cadence"

    return False, "not_due"


# ─────────────────────────────────────────────────────────────────────────────
# Dynamic registration per source:
#   _<t>_snapshot        PRIVATE MV — current N-day federated read
#   _<t>_deletes         PRIVATE MV — rolling-window soft deletes
#   _<t>_reconcile       PRIVATE MV — sweep soft deletes (empty when not due)
#   _<t>_reconcile_stats PRIVATE MV — one audit row per completed sweep
#   _<t>_reconcile_log   ST         — sweep audit trail + cadence state
#   <t>                  PUBLIC ST  — append-only change stream, read by bronze.py
# ─────────────────────────────────────────────────────────────────────────────

for src in _SOURCES:

    snapshot_mv   = _snapshot_mv_fqn(src)
    deletes_mv    = _deletes_mv_fqn(src)
    reconcile_mv  = _reconcile_mv_fqn(src)
    stats_mv      = _reconcile_stats_mv_fqn(src)
    log_fqn       = _reconcile_log_fqn(src)
    public_st     = _public_staging_fqn(src)
    silver_fqn    = _silver_fqn(src)

    fed            = src["federated_source"]
    ri             = src.get("row_identity") or {}
    rc             = src.get("reconciliation") or {}
    detect_deletes = bool((src.get("delete_detection") or {}).get("enabled", True))
    reconcile_on   = bool(rc.get("enabled", False))

    _common_props = {
        "pipelines.autoOptimize.managed": "true",
        "ingestion.source_type":          "federated",
        "ingestion.foreign_fqn":          fed["foreign_fqn"],
        "ingestion.load_mode":            "rolling_window",
        "ingestion.window_column":        fed["window_column"],
        "ingestion.rolling_window_days":  str(fed.get("rolling_window_days", _DEFAULT_WINDOW_DAYS)),
        "ingestion.window_start_anchor":  str(fed.get("window_start_anchor", "midnight_utc")),
        "ingestion.row_identity_mode":    str(ri.get("mode", "hash_all")),
        "ingestion.row_identity_keys":    ",".join(ri.get("key_columns") or []),
        "ingestion.delete_detection":     str(detect_deletes).lower(),
        "ingestion.reconciliation":       str(reconcile_on).lower(),
        **{f"source.{k}": str(v) for k, v in src.get("tags", {}).items()},
    }

    # ─────────────────────────────────────────────────────────────────────────
    # PRIVATE MV — the trailing N-day snapshot, fully recomputed each run.
    # ─────────────────────────────────────────────────────────────────────────

    @dp.materialized_view(
        name    = snapshot_mv,
        comment = (
            f"PRIVATE rolling-window snapshot for {src['source_table']} — "
            f"do not query directly. Consumed by {public_st}."
        ),
        table_properties = {
            **_common_props,
            "quality":                "staging_internal",
            "ingestion.public_table": public_st,
        },
    )
    def rolling_snapshot(src=src):
        fed  = src["federated_source"]
        days = int(fed.get("rolling_window_days", _DEFAULT_WINDOW_DAYS))
        ws   = _window_start(fed)

        df = _add_identity(src, _read_foreign(src, ws))

        # contenthash is the full-row fingerprint — the change detector in
        # key_columns identity mode, and useful for diffing regardless.
        payload_cols = [c for c in df.columns if c not in _CHANGE_COLS]

        df = (
            df
            .withColumn("contenthash", _hash_expr(payload_cols))
            .withColumn("changetype",  F.lit("upsert"))
            .withColumn("isdeleted",   F.lit(False))
            .withColumn("deletedat",   F.lit(None).cast("timestamp"))
        )

        return _add_provenance(_add_snapshot_stamps(df, ws, days))

    # ─────────────────────────────────────────────────────────────────────────
    # CONTROL 1 — rolling-window soft deletes.
    #
    # Diff the current snapshot against the most recent prior snapshot. A rowkey
    # in the prior but not the current has vanished. It counts as a DELETE only
    # if its window column is still inside the current window: a row can equally
    # vanish because it AGED OUT of the trailing N days, and closing an SCD2
    # record for that would be wrong.
    #
    # Exactly once — next run's prior snapshot no longer contains the row, so no
    # second tombstone. A later reappearance at the source is just a new upsert
    # with isdeleted = false, which correctly opens a fresh version.
    # ─────────────────────────────────────────────────────────────────────────

    if detect_deletes:

        @dp.materialized_view(
            name    = deletes_mv,
            comment = (
                f"PRIVATE rolling-window soft deletes for {src['source_table']} — "
                "one row per rowkey that disappeared while still inside the window."
            ),
            table_properties = {
                **_common_props,
                "quality":                "staging_internal",
                "ingestion.public_table": public_st,
                "ingestion.event_type":   "delete",
                "ingestion.control":      "rolling_window",
            },
        )
        def rolling_deletes(src=src, snapshot_mv=snapshot_mv, public_st=public_st):
            fed  = src["federated_source"]
            wcol = fed["window_column"]
            days = int(fed.get("rolling_window_days", _DEFAULT_WINDOW_DAYS))
            ws   = _window_start(fed)

            current = spark.read.table(snapshot_mv)
            schema  = current.columns

            prior_seq = _read_prior_snapshot_seq(public_st)
            if prior_seq is None:
                return current.limit(0)          # initial load

            prior = spark.sql(f"""
                SELECT * FROM {public_st}
                WHERE snapshotseq = {prior_seq}
                  AND changetype  = 'upsert'
            """)

            vanished = prior.join(
                current.select("rowkey").distinct(), on="rowkey", how="left_anti"
            )

            # The aged-out guard. Without it, every row rolling past the
            # trailing-window boundary would be flagged as deleted.
            vanished = vanished.filter(
                F.col(wcol).cast("timestamp") >= F.lit(ws).cast("timestamp")
            )

            return _stamp_as_deleted(vanished, "delete", ws, days).select(*schema)

    # ─────────────────────────────────────────────────────────────────────────
    # CONTROL 2 — reconciliation sweep.
    #
    # Read the full source (or a wide scope), compare against every record
    # Silver believes is live, soft-delete the orphans. Emits nothing when the
    # sweep is not due, so this MV is cheap on ordinary runs.
    #
    # Silver is read cross-pipeline via raw spark.sql, so it reflects the last
    # completed Silver update — which is exactly the question being asked:
    # "as of the last load, what does Silver still assert is live?"
    # ─────────────────────────────────────────────────────────────────────────

    if reconcile_on:

        @dp.materialized_view(
            name    = reconcile_mv,
            comment = (
                f"PRIVATE reconciliation soft deletes for {src['source_table']} — "
                "orphans found by comparing the full source against Silver's open "
                "records. Empty on runs where the sweep is not due."
            ),
            table_properties = {
                **_common_props,
                "quality":                   "staging_internal",
                "ingestion.public_table":    public_st,
                "ingestion.event_type":      "delete",
                "ingestion.control":         "reconciliation",
                "reconcile.cadence_days":    str(rc.get("cadence_days", _DEFAULT_CADENCE)),
                "reconcile.run_on_weekday":  str(rc.get("run_on_weekday") or ""),
                "reconcile.max_age_days":    str(rc.get("max_age_days") or "full"),
                "reconcile.max_delete_pct":  str(rc.get("max_delete_pct", 5.0)),
                "reconcile.min_source_rows": str(rc.get("min_source_rows", 1)),
                "reconcile.on_breach":       str(rc.get("on_breach", "fail")),
            },
        )
        def rolling_reconcile(src=src, snapshot_mv=snapshot_mv, public_st=public_st,
                              silver_fqn=silver_fqn, log_fqn=log_fqn):
            fed  = src["federated_source"]
            wcol = fed["window_column"]
            days = int(fed.get("rolling_window_days", _DEFAULT_WINDOW_DAYS))
            ws   = _window_start(fed)
            rc   = src.get("reconciliation") or {}
            name = src["source_table"]

            current = spark.read.table(snapshot_mv)
            schema  = current.columns
            nothing = current.limit(0)

            due, trigger = _sweep_decision(src, log_fqn)
            if not due:
                return nothing

            # ── Sweep scope ──────────────────────────────────────────────────
            # None sweeps the whole table. max_age_days bounds it, which matters
            # when the source holds years of settled history. Validation already
            # guarantees max_age_days >= rolling_window_days.
            max_age     = rc.get("max_age_days")
            sweep_start = (_RUN_TS - timedelta(days=int(max_age))) if max_age else None

            # ── 1. Live identity set from the source ─────────────────────────
            live_keys = (
                _add_identity(src, _read_foreign(src, sweep_start))
                .select("rowkey")
                .distinct()
            )
            source_count = live_keys.count()

            # CIRCUIT BREAKER: a suspiciously empty read means the connector,
            # not the data, changed. Never let that close records.
            floor = int(rc.get("min_source_rows", 1))
            if source_count < floor:
                raise ValueError(
                    f"[{name}] Reconciliation aborted: the source read returned "
                    f"{source_count:,} rows, below reconciliation.min_source_rows "
                    f"({floor:,}). This usually means a federation or permissions "
                    "problem rather than a genuine mass deletion. No records were "
                    "closed. Investigate the foreign catalog before re-running."
                )

            # ── 2. What Silver still believes is live ────────────────────────
            scope_sql = ""
            if sweep_start is not None:
                # Silver normalizes column names, so the window column is
                # addressed by its normalized form on this side.
                scope_sql = (
                    f"AND {_normalize_identifier(wcol)} >= "
                    f"TIMESTAMP'{sweep_start.strftime('%Y-%m-%d %H:%M:%S')}'"
                )
            try:
                open_keys = spark.sql(f"""
                    SELECT rowkey FROM {silver_fqn}
                    WHERE __END_AT IS NULL
                      AND isdeleted = false
                      {scope_sql}
                """)
                open_count = open_keys.count()
            except Exception:
                # Silver has not been built yet — nothing to reconcile against.
                return nothing

            if open_count == 0:
                return nothing

            orphans      = open_keys.join(live_keys, on="rowkey", how="left_anti")
            orphan_count = orphans.count()

            if orphan_count == 0:
                return nothing

            # CIRCUIT BREAKER: blast radius.
            max_pct = float(rc.get("max_delete_pct", 5.0))
            pct     = 100.0 * orphan_count / open_count
            if pct > max_pct:
                msg = (
                    f"[{name}] Reconciliation would close {orphan_count:,} of "
                    f"{open_count:,} open records ({pct:.2f}%), exceeding "
                    f"reconciliation.max_delete_pct ({max_pct}%). Source returned "
                    f"{source_count:,} rows. Either the source genuinely purged a "
                    "large batch — in which case raise the threshold for one run — "
                    "or the read is incomplete."
                )
                if str(rc.get("on_breach", "fail")).lower() == "fail":
                    raise ValueError(msg + " No records were closed.")
                return nothing

            # ── 3. Last-known staging payload per orphan ─────────────────────
            # Tombstones must carry the staging schema, and Silver's copy is
            # renamed/normalized, so the payload comes from staging history.
            history = spark.sql(f"""
                SELECT * FROM {public_st}
                WHERE changetype  = 'upsert'
                  AND snapshotseq < {_RUN_SEQ}
            """)

            latest = Window.partitionBy("rowkey").orderBy(F.col("snapshotseq").desc())
            payload = (
                history
                .join(orphans, on="rowkey", how="left_semi")
                .withColumn("_rn", F.row_number().over(latest))
                .filter(F.col("_rn") == 1)
                .drop("_rn")
            )

            return _stamp_as_deleted(payload, "reconcile", ws, days).select(*schema)

        # ── Sweep audit trail ────────────────────────────────────────────────
        # One row per completed sweep. Doubles as the cadence state read by
        # _sweep_decision(), and stands on its own as evidence the control ran.
        # A sweep that aborts on a circuit breaker fails the update and is
        # deliberately NOT logged as completed, so the next run retries it.

        @dp.materialized_view(
            name    = stats_mv,
            comment = f"PRIVATE sweep result for {src['source_table']} — feeds {log_fqn}.",
            table_properties = {
                **_common_props,
                "quality":              "staging_internal",
                "ingestion.public_table": log_fqn,
            },
        )
        def reconcile_stats(src=src, reconcile_mv=reconcile_mv, log_fqn=log_fqn):
            due, trigger = _sweep_decision(src, log_fqn)
            if not due:
                return spark.createDataFrame([], _RECONCILE_LOG_SCHEMA)

            rc      = src.get("reconciliation") or {}
            max_age = rc.get("max_age_days")

            return spark.createDataFrame(
                [(
                    src["source_table"],
                    _RUN_SEQ,
                    _RUN_TS.replace(tzinfo=None),
                    trigger,
                    int(max_age) if max_age else None,
                    spark.read.table(reconcile_mv).count(),
                    "completed",
                )],
                _RECONCILE_LOG_SCHEMA,
            )

        dp.create_streaming_table(
            name    = log_fqn,
            comment = (
                f"Reconciliation sweep audit log for {src['source_table']}. One row "
                "per completed sweep: when it ran, what triggered it, how many "
                "records it closed."
            ),
            table_properties = {
                **_common_props,
                "quality":         "audit",
                "ingestion.grain": "one row per completed reconciliation sweep",
            },
        )

        @dp.append_flow(target=log_fqn, name=f"{src['source_table']}_reconcile_log_flow")
        def append_reconcile_log(stats_mv=stats_mv):
            return spark.readStream.option("ignoreChanges", "true").table(stats_mv)

    # ─────────────────────────────────────────────────────────────────────────
    # PUBLIC streaming table — the change stream bronze.py reads.
    #
    # Clustered on (snapshotseq, rowkey): snapshotseq serves the per-snapshot
    # scans of Control 1, rowkey serves the orphan payload lookup in Control 2.
    # Changing these keys on an existing table needs ALTER TABLE ... CLUSTER BY.
    # ─────────────────────────────────────────────────────────────────────────

    dp.create_streaming_table(
        name       = public_st,
        comment    = src.get(
            "description",
            f"Rolling-window federated change stream (append-only): {src['source_table']}",
        ),
        cluster_by = ["snapshotseq", "rowkey"],
        table_properties = {
            **_common_props,
            "quality":                "staging",
            "ingestion.snapshot_mv":  snapshot_mv,
            "ingestion.deletes_mv":   deletes_mv if detect_deletes else "",
            "ingestion.reconcile_mv": reconcile_mv if reconcile_on else "",
            "ingestion.grain":        "one row per source row per snapshot, plus soft-delete events",
        },
    )

    @dp.append_flow(target=public_st, name=f"{src['source_table']}_snapshot_append_flow")
    def append_snapshot(snapshot_mv=snapshot_mv):
        # ignoreChanges, not skipChangeCommits: the MV is rewritten in full on
        # every refresh and those rewrites ARE the new snapshot.
        # skipChangeCommits would discard them.
        return spark.readStream.option("ignoreChanges", "true").table(snapshot_mv)

    if detect_deletes:

        @dp.append_flow(target=public_st, name=f"{src['source_table']}_delete_append_flow")
        def append_deletes(deletes_mv=deletes_mv):
            return spark.readStream.option("ignoreChanges", "true").table(deletes_mv)

    if reconcile_on:

        @dp.append_flow(target=public_st, name=f"{src['source_table']}_reconcile_append_flow")
        def append_reconcile(reconcile_mv=reconcile_mv):
            return spark.readStream.option("ignoreChanges", "true").table(reconcile_mv)


# ─────────────────────────────────────────────────────────────────────────────
# Staging manifest — temporary view (pipeline-scoped, not persisted to UC)
# Query during a run to audit window boundaries, snapshot ids, sweep status, and
# the Silver settings each source depends on.
# ─────────────────────────────────────────────────────────────────────────────

@dp.temporary_view(name="_staging_rolling_manifest")
def staging_rolling_manifest():
    rows = []
    for s, f in zip(_SOURCES, _YAML_FILES):
        rc          = s.get("reconciliation") or {}
        log_fqn     = _reconcile_log_fqn(s)
        due, trig   = _sweep_decision(s, log_fqn)
        last_sweep  = _read_last_sweep(log_fqn) if rc.get("enabled", False) else None

        rows.append({
            "source_table":        s["source_table"],
            "foreign_fqn":         s["federated_source"]["foreign_fqn"],
            "window_column":       s["federated_source"]["window_column"],
            "rolling_window_days": str(s["federated_source"].get("rolling_window_days", _DEFAULT_WINDOW_DAYS)),
            "window_start":        str(_window_start(s["federated_source"])),
            "snapshotseq":         str(_RUN_SEQ),
            "snapshotts":          str(_RUN_TS),
            "prior_snapshotseq":   str(_read_prior_snapshot_seq(_public_staging_fqn(s))),
            "identity_mode":       str((s.get("row_identity") or {}).get("mode", "hash_all")),
            "identity_keys":       str((s.get("row_identity") or {}).get("key_columns") or []),
            "delete_detection":    str(bool((s.get("delete_detection") or {}).get("enabled", True))).lower(),
            "reconcile_enabled":   str(bool(rc.get("enabled", False))).lower(),
            "reconcile_due":       str(due).lower(),
            "reconcile_trigger":   trig,
            "reconcile_last_run":  str(last_sweep),
            "reconcile_scope":     str(rc.get("max_age_days") or "full"),
            "reconcile_guards":    f"min_rows={rc.get('min_source_rows', 1)}, "
                                   f"max_pct={rc.get('max_delete_pct', 5.0)}, "
                                   f"on_breach={rc.get('on_breach', 'fail')}",
            "staging_table":       _public_staging_fqn(s),
            "silver_table":        _silver_fqn(s),
            "silver_key":          str(s.get("scd_2_key_list") or []),
            "silver_sequence_by":  str(s.get("history_timestamp_source") or ""),
            "silver_not_tracked":  str(s.get("scd_2_exclude_list") or []),
            "config_file":         os.path.basename(f),
            "tags":                str(s.get("tags", {})),
        })
    return spark.createDataFrame(rows)
