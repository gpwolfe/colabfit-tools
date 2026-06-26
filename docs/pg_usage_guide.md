# ColabFit-Tools — PostgreSQL Usage Guide

---

## 1. Installation

```bash
python -m venv pg_env
source pg_env/bin/activate
pip install -e ".[pg]"
```

The `pg` optional dependency group installs `psycopg[binary]` and `pyarrow`. The base install
provides `ase`, `kim_property`, `numpy`, `tqdm`, `periodictable`, `unidecode`,
and `python-dotenv`.

A running PostgreSQL instance is required. Connection parameters are passed
directly to `DataManager` (below); keep credentials in environment variables,
not in source.

---

## 2. Core objects

| Object | Module | Role |
|--------|--------|------|
| `AtomicConfiguration` | `pg.configuration` | An ASE `Atoms` subclass; one atomic structure. Produces a config row dict and a `structure_hash`. |
| `Property` | `pg.property` | One property record (energy, forces, stress, …) tied to a configuration. |
| `PropertyMap` | `pg.property_map` | Maps raw data keys → property-definition fields, units, and metadata. |
| `Dataset` | `pg.dataset` | Aggregates statistics over a dataset's configs + properties. |
| `ConfigurationSet` | `pg.configuration_set` | A named grouping of configurations within a dataset. |
| `DataManager` | `pg.database` | Orchestrates table creation, ingestion, dataset/CS creation, and queries against PostgreSQL. |

### Identifiers and hashes

- Configuration: `CO_{hex[:25]}` — first 25 chars of the SHA-512 hexdigest.
- Property: `PO_{hex[:25]}`.
- ConfigurationSet: `CS_{name}_{dataset_id}` (SHA-512 hash of that string).
- Dataset: `DS_{random}_0` via `generate_ds_id()`.
- `hash` columns store the full 128-char SHA-512 hexdigest.
- `structure_hash` identifies geometry independent of labels/metadata.

---

## 3. Connecting

```python
import os
from colabfit.tools.pg.database import DataManager

dm = DataManager(
    dbname="colabfit",
    user="cf_user",
    port=5432,
    host="localhost",
    password=os.environ["CF_PG_PASSWORD"],
    nprocs=4,                  # multiprocessing for reading/parsing data files
    standardize_energy=True,   # normalize energies during ingest
    read_write_batch_size=10000,
)
```

---

## 4. Creating tables

```python
dm.create_co_table()   # configurations
dm.create_po_table()   # property_objects (carries the metadata column)
dm.create_ds_table()   # datasets
dm.create_pd_table()   # property_definitions
```

All tables are created `IF NOT EXISTS` from the schemas in `pg/schema.py`.

Metadata is attached to property objects only — the `configurations` table has
no metadata column. The configurations table carries a `structure_hash` column
for geometry-based deduplication.

---

## 5. Registering property definitions

A property definition is a KIM-style dict describing a property's fields, types,
units, and extents. Inserting one records the definition and (for PG) creates a
backing column per field on the `property_objects` table.

```python
energy_def = {
    "property-id": "energy",
    "property-name": "energy",
    "property-title": "Potential energy",
    "property-description": "...",
    "energy": {"type": "float", "has-unit": True, "extent": [], "required": True,
               "description": "..."},
}
dm.insert_property_definition(energy_def)

defs = dm.get_property_definitions()   # list[dict]
```

---

## 6. Ingesting data and creating a dataset

The one-call path: convert configurations, write CO/PO rows in batches, and
create the dataset record.

```python
from ase.io import read
from colabfit.tools.pg.configuration import AtomicConfiguration
from colabfit.tools.pg.property_map import PropertyMap

# Build configurations (ASE Atoms or AtomicConfiguration both accepted)
configs = [AtomicConfiguration.from_ase(a) for a in read("data.xyz", index=":")]

# Map raw keys -> property-definition fields
prop_map = PropertyMap(...)   # see PropertyMap docstring / docs/source/md_file_input.rst

dataset_id = dm.insert_data_and_create_dataset(
    configs=configs,
    name="my_dataset",
    authors=["A. Researcher", "B. Scientist"],
    description="DFT energies and forces for ...",
    publication_link="https://doi.org/...",
    data_link="https://...",
    publication_year="2026",
    doi="10.xxxx/xxxxx",
    data_license="CC-BY-4.0",
    date_requested="2026-01-21",   # required: 'YYYY-MM-DD'
    equilibrium=False,
    prop_map=prop_map,
)
```

Under the hood:
1. `gather_co_po_in_batches` parses configs (multiprocessing across `nprocs`) and
   builds `(config_row, property_row)` pairs via `Property.from_definition`.
2. `load_data_in_batches` writes each batch with `ON CONFLICT (hash)`:
   - Configurations: append the new `dataset_id`/`name` to the row's arrays.
   - Property objects: increment `multiplicity`.
3. `create_dataset` queries the dataset's configs + properties and writes the
   aggregated `datasets` row.

To ingest without creating a dataset, call `dm.load_data_in_batches(configs,
dataset_id, prop_map)` directly.

---

## 7. Filtering columns on property objects

Each property object row carries:

| Column | Meaning |
|--------|---------|
| `method` | Computational method |
| `software` | Software package |
| `max_force_norm` | Max per-atom L2 force norm (computed at ingest if forces present) |
| `mean_force_norm` | Mean per-atom L2 force norm (computed at ingest if forces present) |
| `energy_above_hull` | Energy above the convex hull (populated if provided in property map) |
| `metadata` | JSON blob for arbitrary supplementary fields |

These enable filter queries such as:

```sql
SELECT id, energy, max_force_norm
FROM property_objects
WHERE dataset_id = %s
  AND method = 'DFT-PBE'
  AND max_force_norm < 50.0;
```

---

## 8. Configuration sets

Configuration set membership is recorded in a separate join table
(`configuration_set_configuration_map`) — configuration rows are never modified.

```python
dm.create_configuration_sets(
    dataset_id=dataset_id,
    name_label_match=[
        # (names_pattern, labels_pattern, cs_name, cs_description)
        ("%bulk%",  None,       "bulk",  "Bulk structures"),
        (None,      "%surface%","surface","Surface slabs"),
    ],
)
```

Each tuple entry is `(names_match, label_match, cs_name, cs_desc)` where
`names_match`/`label_match` are SQL `LIKE` patterns (or `None` to skip that
filter). The method:

1. calls `create_cs_table()` and `create_co_cs_map_table()` if needed,
2. queries configs matching the patterns via `config_set_query`,
3. builds a `ConfigurationSet` row and inserts it into `configuration_sets`, and
4. inserts `(configuration_set_id, configuration_id)` pairs into
   `configuration_set_configuration_map`.

---

## 9. Dataset-level aggregation

`Dataset.to_row_dict` aggregates over the dataset's configurations and property
objects. In addition to element statistics and property counts it produces:

| Column | Meaning |
|--------|---------|
| `methods` | Sorted distinct list of methods in the dataset |
| `software` | Sorted distinct list of software packages in the dataset |
| `energy_above_hull_count` | Count of POs with a non-null `energy_above_hull` |
| `equilibrium` | Whether the dataset represents equilibrium structures |
| `authors` | List of authors |
| `date_added_to_colabfit` / `date_requested` | Provenance dates |

Energy variance is computed with Welford's online algorithm for numerical
stability on large datasets.

---

## 10. Querying

```python
# Full dataset record
ds = dm.get_dataset(dataset_id)

# Raw config / property rows for a dataset
configs_df = dm.dataset_query(dataset_id, "configurations")
props_df   = dm.dataset_query(dataset_id, "property_objects")

# Arbitrary SQL with parameterized values
rows = dm.general_query(
    "SELECT id, energy FROM property_objects WHERE dataset_id = %s",
    params=(dataset_id,),
)
```

`dataset_query` and `get_dataset` use parameterized statements internally.
`general_query` accepts an optional `params` tuple passed directly to
`curs.execute` — always use `%s` placeholders rather than f-strings.
`insert_new_column` whitelists identifiers via `psycopg.sql.Identifier`.

---

## 11. Maintenance utilities

- Audit duplicate short IDs (hash collisions on the 25-char prefix):
  `dm.find_duplicate_ids("property_objects")` or `dm.find_duplicate_ids("configurations")`
- Reset multiplicity before re-ingest if an ingest fails partway through with committed data:
  `dm.zero_multiplicity(dataset_id)`
- Raw SQL audit:
  `SELECT hash, COUNT(*) FROM property_objects GROUP BY hash HAVING COUNT(*) > 1;`
