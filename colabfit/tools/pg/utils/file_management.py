"""Export/import ColabFit PostgreSQL datasets to and from Parquet.

Parquet (zstd) is the on-disk format: it preserves the nested array columns
(positions, atomic_forces, cell), the TIMESTAMP columns, and compresses the
columnar numeric data well. Each dataset is written to its own directory::

    <out_dir>/<dataset_id>/configurations.parquet
    <out_dir>/<dataset_id>/property_objects.parquet
    <out_dir>/<dataset_id>/dataset.parquet

The normalized tables are written separately (rather than a joined view) so no
columns are lost to name collisions, and the files reload directly into the
same tables. ``metadata`` is a JSONB column in Postgres (a dict in Python); it
is stored as a JSON string in Parquet (matching the VAST backend's ``metadata``
string column) and decoded back to a dict on import.

Each file is stamped with the export-format version, the colabfit version, the
source table name, and the column list. :func:`import_dataset` checks these and
logs warnings when an older/foreign file is loaded, when the file's columns no
longer match the current table schema, when data would be dropped, or when a
file is missing/corrupt -- so schema drift introduced later is surfaced rather
than silently mangling data.
"""

import ast
import json
import logging
from pathlib import Path

import psycopg
import pyarrow as pa
import pyarrow.parquet as pq

from colabfit import __version__ as COLABFIT_VERSION
from colabfit.tools.pg.schema import (
    config_schema,
    dataset_schema,
    property_object_schema,
)

logger = logging.getLogger(__name__)

# Bump when the on-disk layout/encoding changes in a non-backward-compatible way.
EXPORT_FORMAT_VERSION = "1"

CONFIG_FILE = "configurations.parquet"
PROP_FILE = "property_objects.parquet"
DATASET_FILE = "dataset.parquet"

# (table name, file name, schema) in load order: configs, then property objects,
# then the dataset row.
_TABLE_SPECS = [
    ("configurations", CONFIG_FILE, config_schema),
    ("property_objects", PROP_FILE, property_object_schema),
    ("datasets", DATASET_FILE, dataset_schema),
]

_META_VERSION = b"colabfit_export_format_version"
_META_COLABFIT = b"colabfit_version"
_META_TABLE = b"colabfit_table"
_META_COLUMNS = b"colabfit_columns"


def _conn_params(dm):
    return {
        "dbname": dm.dbname,
        "user": dm.user,
        "port": dm.port,
        "host": dm.host,
        "password": dm.password,
    }


def _encode_metadata(rows):
    """JSONB dict -> JSON string for Parquet storage (in place)."""
    for row in rows:
        md = row.get("metadata")
        if isinstance(md, (dict, list)):
            row["metadata"] = json.dumps(md)
    return rows


def _decode_metadata(rows):
    """JSON string -> dict on import (in place)."""
    for i, row in enumerate(rows):
        md = row.get("metadata")
        if isinstance(md, str):
            try:
                row["metadata"] = json.loads(md)
            except (json.JSONDecodeError, TypeError):
                logger.warning(
                    "property_objects row %d: metadata is not valid JSON; "
                    "loading it as a raw string.",
                    i,
                )
    return rows


def _coerce_dataset_row(row):
    """Normalize old-schema dataset fields to match the current schema in place.

    Handles two fields that changed representation between schema versions:
    - ``dimension_types``: stored as ``VARCHAR[]`` of stringified inner lists in
      old databases (e.g. ``['[1, 1, 1]']``); current schema is ``INT[][]``.
    - ``links``: stored as ``VARCHAR[]`` of ``"key: value"`` fragments in old
      databases; current schema is a single ``VARCHAR`` str-of-dict.
    """
    dt = row.get("dimension_types")
    if isinstance(dt, list) and dt and isinstance(dt[0], str):
        try:
            row["dimension_types"] = [ast.literal_eval(v) for v in dt]
        except (ValueError, SyntaxError):
            logger.warning("dimension_types: could not parse %r; leaving as-is.", dt)

    links = row.get("links")
    if isinstance(links, list):
        row["links"] = "{" + ", ".join(str(v) for v in links) + "}"

    return row


def _stamp(table, table_name, columns):
    """Attach colabfit/version provenance to a pyarrow table's schema metadata."""
    meta = dict(table.schema.metadata or {})
    meta.update(
        {
            _META_VERSION: EXPORT_FORMAT_VERSION.encode(),
            _META_COLABFIT: str(COLABFIT_VERSION).encode(),
            _META_TABLE: table_name.encode(),
            _META_COLUMNS: json.dumps(columns).encode(),
        }
    )
    return table.replace_schema_metadata(meta)


def _write_table(rows, path, table_name, compression):
    if not rows:
        logger.warning("No rows for %s; skipping %s.", table_name, path.name)
        return False
    table = _stamp(pa.Table.from_pylist(rows), table_name, list(rows[0].keys()))
    pq.write_table(table, path, compression=compression)
    return True


def _read_parquet(path, table_name):
    """Read a parquet file into a pyarrow Table, or None if missing.

    Raises RuntimeError (with context) if the file exists but cannot be read.
    """
    path = Path(path)
    if not path.exists():
        logger.warning("Missing %s file: %s", table_name, path)
        return None
    try:
        return pq.read_table(path)
    except Exception as e:  # corrupt/truncated/non-parquet file
        raise RuntimeError(f"Failed to read {table_name} parquet at {path}: {e}") from e


def _validate_table(table, table_name, schema):
    """Log warnings for version drift, schema-column drift, and dropped data."""
    meta = table.schema.metadata or {}

    file_version = meta.get(_META_VERSION)
    if file_version is None:
        logger.warning(
            "%s: no colabfit export-format version stamp; file may predate "
            "versioned exports or come from another tool.",
            table_name,
        )
    elif file_version.decode() != EXPORT_FORMAT_VERSION:
        logger.warning(
            "%s: exported with format version %s but importing with %s; "
            "column handling may differ.",
            table_name,
            file_version.decode(),
            EXPORT_FORMAT_VERSION,
        )

    file_colabfit = meta.get(_META_COLABFIT)
    if file_colabfit is not None and file_colabfit.decode() != str(COLABFIT_VERSION):
        logger.warning(
            "%s: file written by colabfit %s, importing with %s.",
            table_name,
            file_colabfit.decode(),
            COLABFIT_VERSION,
        )

    stamped_table = meta.get(_META_TABLE)
    if stamped_table is not None and stamped_table.decode() != table_name:
        logger.warning(
            "%s: file is stamped as table '%s' -- wrong file in this slot?",
            table_name,
            stamped_table.decode(),
        )

    expected = set(schema.column_names)
    actual = set(table.column_names)

    missing = [c for c in schema.column_names if c not in actual]
    if missing:
        logger.warning(
            "%s: file is missing columns %s; they will be inserted as NULL.",
            table_name,
            missing,
        )

    # Extra columns are dropped on insert (the INSERT uses the fixed schema).
    # Only warn loudly when the dropped column actually carries data.
    for col in table.column_names:
        if col in expected:
            continue
        chunk = table.column(col)
        if chunk.null_count < len(chunk):
            logger.warning(
                "%s: column '%s' is not in the current schema and holds "
                "non-null data that will be DROPPED on import.",
                table_name,
                col,
            )
        else:
            logger.debug(
                "%s: dropping empty unknown column '%s'.", table_name, col
            )


def export_dataset(dm, dataset_id, out_dir="export", compression="zstd"):
    """Export one dataset's configurations, property objects, and dataset row.

    Returns the directory the files were written to.
    """
    ds_rows = dm.get_dataset(dataset_id)
    if not ds_rows:
        raise ValueError(f"No dataset found with id '{dataset_id}'")

    out = Path(out_dir) / dataset_id
    out.mkdir(parents=True, exist_ok=True)

    co_rows = dm.dataset_query(dataset_id, "configurations")
    po_rows = _encode_metadata(dm.dataset_query(dataset_id, "property_objects"))

    _write_table(co_rows, out / CONFIG_FILE, "configurations", compression)
    _write_table(po_rows, out / PROP_FILE, "property_objects", compression)
    _write_table(ds_rows, out / DATASET_FILE, "datasets", compression)
    logger.info(
        "Exported %s: %d configurations, %d property objects -> %s",
        dataset_id,
        len(co_rows),
        len(po_rows),
        out,
    )
    return out


def export_datasets(dm, dataset_ids=None, out_dir="export", compression="zstd"):
    """Export several datasets (or all of them when ``dataset_ids`` is None).

    Returns the list of directories written.
    """
    if dataset_ids is None:
        dataset_ids = [r["id"] for r in dm.general_query("SELECT id FROM datasets;")]
    return [
        export_dataset(dm, ds_id, out_dir=out_dir, compression=compression)
        for ds_id in dataset_ids
    ]


def import_dataset(dm, dataset_dir):
    """Load a dataset directory produced by :func:`export_dataset` back into the
    database, reusing the DataManager's INSERT statements (ON CONFLICT applies).

    Validates each file (version/schema/columns) and logs warnings before
    loading. Returns a dict of row counts loaded per table.
    """
    d = Path(dataset_dir)
    if not d.is_dir():
        raise NotADirectoryError(f"Not a dataset directory: {d}")

    tables = {}
    for table_name, file_name, schema in _TABLE_SPECS:
        table = _read_parquet(d / file_name, table_name)
        if table is not None:
            _validate_table(table, table_name, schema)
        tables[table_name] = table

    co_rows = tables["configurations"].to_pylist() if tables["configurations"] else []
    po_rows = tables["property_objects"].to_pylist() if tables["property_objects"] else []
    ds_rows = tables["datasets"].to_pylist() if tables["datasets"] else []
    po_rows = _decode_metadata(po_rows)
    ds_rows = [_coerce_dataset_row(r) for r in ds_rows]

    if not co_rows:
        logger.warning("%s: no configurations to import.", d)
    if not ds_rows:
        logger.warning("%s: no dataset row to import.", d)

    # Dangling reference check: property objects pointing at configs not present.
    co_ids = {r.get("id") for r in co_rows}
    dangling = {
        r.get("configuration_id")
        for r in po_rows
        if r.get("configuration_id") not in co_ids
    }
    if dangling:
        logger.warning(
            "%s: %d property objects reference configuration_ids not in the "
            "configurations file (export may be incomplete).",
            d,
            len(dangling),
        )

    try:
        with psycopg.connect(**_conn_params(dm)) as conn:
            with conn.cursor() as curs:
                if co_rows:
                    curs.executemany(
                        dm.get_co_sql(), [dm.co_row_to_values(r) for r in co_rows]
                    )
                if po_rows:
                    curs.executemany(
                        dm.get_po_sql(), [dm.po_row_to_values(r) for r in po_rows]
                    )
                for r in ds_rows:
                    curs.execute(dm.get_ds_sql(), dm.ds_row_to_values(r))
    except psycopg.Error as e:
        logger.error("Import of %s failed and was rolled back: %s", d, e)
        raise

    counts = {
        "configurations": len(co_rows),
        "property_objects": len(po_rows),
        "datasets": len(ds_rows),
    }
    logger.info("Imported %s: %s", d, counts)
    return counts
