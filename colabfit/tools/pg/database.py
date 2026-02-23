import hashlib
import itertools
import json
import logging
import secrets
import string
from functools import partial
from itertools import islice
from multiprocessing import Pool
from types import GeneratorType

import psycopg
from ase import Atoms
from psycopg import sql
from psycopg.rows import dict_row
from tqdm import tqdm

from colabfit import ID_FORMAT_STRING
from colabfit.tools.pg.configuration import AtomicConfiguration
from colabfit.tools.pg.configuration_set import ConfigurationSet
from colabfit.tools.pg.dataset import Dataset
from colabfit.tools.pg.property import Property
from colabfit.tools.pg.schema import (
    config_md_schema,
    configuration_set_schema,
    dataset_schema,
    property_definition_schema,
    property_object_md_schema,
)
from colabfit.tools.pg.utilities import get_last_modified

VAST_BUCKET_DIR = "colabfit-data"
VAST_METADATA_DIR = "data/MD"
NSITES_COL_SPLITS = 20


def generate_string():
    alphabet = string.ascii_lowercase + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(12))


def generate_ds_id():
    ds_id = ID_FORMAT_STRING.format("DS", generate_string(), 0)
    return ds_id


def batched(configs, n):
    "Batch data into tuples of length n. The last batch may be shorter."
    if not isinstance(configs, GeneratorType):
        configs = iter(configs)
    while True:
        batch = list(islice(configs, n))
        if len(batch) == 0:
            break
        yield batch


class DataManager:
    def __init__(
        self,
        dbname: str,
        user: str,
        port: int,
        host: str,
        password: str = None,
        nprocs: int = 1,
        standardize_energy: bool = False,
        read_write_batch_size: int = 10000,
    ):
        """
        Args:
            dbname (str): Name of the database.
            user (str): User name.
            port (int): Port number.
            host (str): Host name.
            password (str): Password.
            nprocs (int): Number of processes to use for multiprocessing.
            standardize_energy (bool): Whether to standardize energy.
            read_write_batch_size (int): Batch size for reading and writing data.
        """
        if not isinstance(port, int):
            raise TypeError(f"port must be an int, got {type(port)}")
        if not isinstance(read_write_batch_size, int) or read_write_batch_size <= 0:
            raise ValueError(
                f"read_write_batch_size must be a positive int, got {read_write_batch_size!r}"
            )
        self.dbname = dbname
        self.user = user
        self.port = port
        self.password = password
        self.host = host
        self.read_write_batch_size = read_write_batch_size
        self.nprocs = nprocs
        self.standardize_energy = standardize_energy

    @staticmethod
    def _gather_co_po_rows(
        configs: list[AtomicConfiguration],
        prop_defs: list[dict],
        prop_map: dict,
        dataset_id,
        standardize_energy: bool = True,
    ):
        """Convert COs and POs to row dicts."""
        co_po_rows = []
        for config in configs:
            config.set_dataset_id(dataset_id)
            property_ = Property.from_definition(
                definitions=prop_defs,
                configuration=config,
                property_map=prop_map,
                standardize_energy=standardize_energy,
            )
            co_po_rows.append(
                (
                    config.row_dict,
                    property_.row_dict,
                )
            )
        return co_po_rows

    def gather_co_po_rows_pool(
        self,
        config_chunks: list[list[AtomicConfiguration]],
        pool,
        dataset_id=None,
        prop_map=None,
    ):
        """Wrapper for _gather_co_po_rows using multiprocessing Pool."""
        if dataset_id is None:
            dataset_id = generate_ds_id()

        part_gather = partial(
            self._gather_co_po_rows,
            prop_defs=self.get_property_definitions(),
            prop_map=prop_map,
            dataset_id=dataset_id,
            standardize_energy=self.standardize_energy,
        )
        return itertools.chain.from_iterable(pool.map(part_gather, list(config_chunks)))

    def gather_co_po_in_batches(self, configs, dataset_id=None, prop_map=None):
        """Yields batches of CO-PO row dicts, preventing configs from being consumed
        all at once."""
        chunk_size = 1000
        config_chunks = batched(configs, chunk_size)
        with Pool(self.nprocs) as pool:
            while True:
                config_batches = list(islice(config_chunks, self.nprocs))
                if not config_batches:
                    break
                else:
                    yield list(
                        self.gather_co_po_rows_pool(
                            config_batches, pool, dataset_id, prop_map
                        )
                    )

    @staticmethod
    def get_co_sql():
        columns = config_md_schema.column_names
        sql_compose = sql.SQL(" ").join(
            [
                sql.SQL("INSERT INTO"),
                sql.Identifier(config_md_schema.name),
                sql.SQL("("),
                sql.SQL(",").join(map(sql.Identifier, columns)),
                sql.SQL(") VALUES ("),
                sql.SQL(",").join([sql.Placeholder()] * len(columns)),
                sql.SQL(")"),
                sql.SQL("ON CONFLICT (hash) DO UPDATE SET"),
                sql.Identifier("dataset_ids"),
                sql.SQL("= array_append("),
                sql.Identifier("configurations.dataset_ids"),
                sql.SQL(","),
                sql.Placeholder(),
                sql.SQL(") ,"),
                sql.Identifier("names"),
                sql.SQL("= array_append("),
                sql.Identifier("configurations.names"),
                sql.SQL(","),
                sql.Placeholder(),
                sql.SQL(");"),
            ]
        )
        return sql_compose

    @staticmethod
    def get_po_sql():
        columns = property_object_md_schema.column_names
        sql_compose = sql.SQL(" ").join(
            [
                sql.SQL("INSERT INTO"),
                sql.Identifier(property_object_md_schema.name),
                sql.SQL("("),
                sql.SQL(",").join(map(sql.Identifier, columns)),
                sql.SQL(") VALUES ("),
                sql.SQL(",").join([sql.Placeholder()] * len(columns)),
                sql.SQL(")"),
                sql.SQL(
                    "ON CONFLICT (hash) DO UPDATE SET multiplicity = {}.multiplicity + 1;"
                ).format(sql.Identifier(property_object_md_schema.name)),
            ]
        )
        return sql_compose

    @staticmethod
    def co_row_to_values(row_dict):
        name = row_dict["names"][0]
        dataset_id = row_dict["dataset_ids"][0]
        vals = [row_dict.get(k) for k in config_md_schema.column_names]
        assert len(vals) == len(config_md_schema.column_names), (
            f"CO column count mismatch: {len(vals)} vals for "
            f"{len(config_md_schema.column_names)} columns"
        )
        vals.append(dataset_id)
        vals.append(name)
        return vals

    @staticmethod
    def po_row_to_values(row_dict):
        vals = [row_dict.get(k) for k in property_object_md_schema.column_names]
        assert len(vals) == len(property_object_md_schema.column_names), (
            f"PO column count mismatch: {len(vals)} vals for "
            f"{len(property_object_md_schema.column_names)} columns"
        )
        return vals

    def load_data_in_batches(self, configs, dataset_id=None, prop_map=None):
        """Load data to PostgreSQL in batches."""
        co_po_rows = self.gather_co_po_in_batches(configs, dataset_id, prop_map)
        co_sql = self.get_co_sql()
        po_sql = self.get_po_sql()
        for co_po_batch in tqdm(
            co_po_rows,
            desc="Loading data to database: ",
            unit="batch",
        ):
            co_rows, po_rows = list(zip(*co_po_batch))
            if len(co_rows) == 0:
                continue
            co_values = list(map(self.co_row_to_values, co_rows))
            po_values = list(map(self.po_row_to_values, po_rows))
            with psycopg.connect(
                dbname=self.dbname,
                user=self.user,
                port=self.port,
                host=self.host,
                password=self.password,
            ) as conn:
                with conn.cursor() as curs:
                    curs.executemany(co_sql, co_values)
                    curs.executemany(po_sql, po_values)

    def create_table(self, schema):
        name_type = [
            (sql.Identifier(column.name), sql.SQL(column.type))
            for column in schema.columns
        ]
        query = sql.SQL(" ").join(
            [
                sql.SQL("CREATE TABLE IF NOT EXISTS"),
                sql.Identifier(schema.name),
                sql.SQL("("),
                sql.SQL(",").join(
                    [sql.SQL(" ").join([name, type_]) for name, type_ in name_type]
                ),
                sql.SQL(")"),
            ]
        )
        self.execute_sql(query)

    def create_ds_table(self):
        self.create_table(dataset_schema)

    def create_po_table(self):
        self.create_table(property_object_md_schema)

    def create_co_table(self):
        self.create_table(config_md_schema)

    def create_pd_table(self):
        self.create_table(property_definition_schema)

    def insert_property_definition(self, property_dict):
        json_pd = json.dumps(property_dict)
        last_modified = get_last_modified()
        md5_hash = hashlib.md5(json_pd.encode()).hexdigest()
        insert_sql = """
            INSERT INTO property_definitions (hash, last_modified, definition)
            VALUES (%s, %s, %s)
            ON CONFLICT (hash)
            DO NOTHING
        """
        with psycopg.connect(
            dbname=self.dbname,
            user=self.user,
            port=self.port,
            host=self.host,
            password=self.password,
        ) as conn:
            with conn.cursor() as curs:
                curs.execute(insert_sql, (md5_hash, last_modified, json_pd))
        for key, v in property_dict.items():
            if key in [
                "property-id",
                "property-name",
                "property-title",
                "property-description",
            ]:
                continue
            column_name = property_dict["property-name"].replace(
                "-", "_"
            ) + f"_{key}".replace("-", "_")
            if v["type"] == "float":
                data_type = "DOUBLE PRECISION"
            elif v["type"] == "int":
                data_type = "INT"
            elif v["type"] == "bool":
                data_type = "BOOL"
            else:
                data_type = "VARCHAR (10000)"
            for _ in range(len(v["extent"])):
                data_type += "[]"
            try:
                self.insert_new_column("property_objects", column_name, data_type)
            except Exception as e:
                logging.warning("Could not add column %s: %s", column_name, e)

    def get_property_definitions(self):
        query = "SELECT definition FROM property_definitions;"
        defs = self.general_query(query) or []
        return [json.loads(d["definition"]) for d in defs]

    def insert_data_and_create_dataset(
        self,
        configs,
        name: str,
        authors: list[str],
        description: str,
        publication_link: str = None,
        data_link: str = None,
        dataset_id: str = None,
        other_links: list[str] = None,
        publication_year: str = None,
        doi: str = None,
        labels: list[str] = None,
        data_license: str = "CC-BY-4.0",
        prop_map=None,
    ):
        if dataset_id is None:
            dataset_id = generate_ds_id()

        converted_configs = []
        for c in configs:
            if isinstance(c, Atoms):
                converted_configs.append(AtomicConfiguration.from_ase(c))
            elif isinstance(c, AtomicConfiguration):
                converted_configs.append(c)
            else:
                raise TypeError(
                    "Configs must be an instance of either ase.Atoms or AtomicConfiguration"
                )

        self.load_data_in_batches(converted_configs, dataset_id, prop_map)
        self.create_dataset(
            name,
            dataset_id,
            authors,
            publication_link,
            data_link,
            description,
            other_links,
            publication_year,
            doi,
            labels,
            data_license,
        )
        return dataset_id

    def get_table_schema(self, table_name):
        query = """
        SELECT
            column_name,
            data_type,
            character_maximum_length,
            is_nullable
        FROM information_schema.columns
        WHERE table_name = %s
        ORDER BY ordinal_position;
        """
        with psycopg.connect(
            dbname=self.dbname,
            user=self.user,
            port=self.port,
            host=self.host,
            password=self.password,
        ) as conn:
            with conn.cursor() as curs:
                curs.execute(query, (table_name,))
                return curs.fetchall()

    def _insert_dataset_row(self, row: dict):
        """Insert a dataset row; skip silently on hash conflict."""
        columns = dataset_schema.column_names
        query = sql.SQL(
            "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT (hash) DO NOTHING"
        ).format(
            sql.Identifier(dataset_schema.name),
            sql.SQL(",").join(map(sql.Identifier, columns)),
            sql.SQL(",").join([sql.Placeholder()] * len(columns)),
        )
        vals = [row.get(c) for c in columns]
        assert len(vals) == len(columns), (
            f"Dataset column count mismatch: {len(vals)} vals for {len(columns)} columns"
        )
        self.execute_sql(query, vals)

    def create_dataset(
        self,
        name: str,
        dataset_id: str,
        authors: list[str],
        publication_link: str,
        data_link: str,
        description: str,
        other_links: list[str] = None,
        publication_year: str = None,
        doi: str = None,
        labels: list[str] = None,
        data_license: str = "CC-BY-4.0",
    ):
        config_df = self.dataset_query(dataset_id, "configurations") or []
        prop_df = self.dataset_query(dataset_id, "property_objects") or []

        if isinstance(authors, str):
            authors = [authors]
        ds = Dataset(
            name=name,
            authors=authors,
            config_df=config_df,
            prop_df=prop_df,
            publication_link=publication_link,
            data_link=data_link,
            description=description,
            other_links=other_links,
            dataset_id=dataset_id,
            labels=labels,
            doi=doi,
            data_license=data_license,
            configuration_set_ids=None,
            publication_year=publication_year,
        )
        self._insert_dataset_row(ds.row_dict)

    def insert_new_column(self, table: str, column_name: str, data_type: str):
        ALLOWED_TYPES = {
            "DOUBLE PRECISION",
            "INT",
            "BOOL",
            "VARCHAR (10000)",
            "DOUBLE PRECISION[]",
            "INT[]",
            "BOOL[]",
            "VARCHAR (10000)[]",
        }
        norm_type = data_type.replace(" ", "").upper()
        if not any(norm_type.startswith(t.replace(" ", "").upper()) for t in ALLOWED_TYPES):
            raise ValueError(f"Disallowed column type: {data_type!r}")
        query = sql.SQL("ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} {}").format(
            sql.Identifier(table),
            sql.Identifier(column_name),
            sql.SQL(data_type),
        )
        self.execute_sql(query)

    def update_dataset(self, configs, dataset_id, prop_map):
        converted_configs = []
        for c in configs:
            if isinstance(c, Atoms):
                converted_configs.append(AtomicConfiguration.from_ase(c))
            elif isinstance(c, AtomicConfiguration):
                converted_configs.append(c)
            else:
                raise TypeError(
                    "Configs must be an instance of either ase.Atoms or AtomicConfiguration"
                )
        v_no = dataset_id.split("_")[-1]
        new_v_no = int(v_no) + 1
        new_dataset_id = (
            dataset_id.split("_")[0]
            + "_"
            + dataset_id.split("_")[1]
            + "_"
            + str(new_v_no)
        )

        self.load_data_in_batches(converted_configs, new_dataset_id, prop_map=prop_map)

        config_df_2 = self.dataset_query(new_dataset_id, "configurations") or []
        prop_df_2 = self.dataset_query(new_dataset_id, "property_objects") or []

        old_ds = self.get_dataset(dataset_id)[0]

        s = old_ds["links"][0].split(" ")[-1].replace("'", "")
        d = old_ds["links"][1].split(" ")[-1].replace("'", "")
        o = old_ds["links"][2].split(" ")[-1].replace("'", "")

        ds = Dataset(
            name=old_ds["name"],
            authors=old_ds["authors"],
            config_df=config_df_2,
            prop_df=prop_df_2,
            publication_link=s,
            data_link=d,
            description=old_ds["description"],
            other_links=o,
            dataset_id=new_dataset_id,
            labels=old_ds.get("labels"),
            doi=old_ds["doi"],
            data_license=old_ds["license"],
            configuration_set_ids=None,
            publication_year=old_ds["publication_year"],
        )
        self._insert_dataset_row(ds.row_dict)
        return new_dataset_id

    def get_dataset_data(self, dataset_id: str):
        query = (
            "SELECT c.*, po.* "
            "FROM (SELECT * FROM configurations WHERE %s = ANY(dataset_ids)) c "
            "INNER JOIN (SELECT * FROM property_objects WHERE dataset_id = %s) po "
            "ON c.id = po.configuration_id"
        )
        return self.general_query(query, (dataset_id, dataset_id))

    def general_query(self, query, params=None):
        try:
            with psycopg.connect(
                dbname=self.dbname,
                user=self.user,
                port=self.port,
                host=self.host,
                password=self.password,
                row_factory=dict_row,
            ) as conn:
                with conn.cursor() as curs:
                    curs.execute(query, params)
                    return curs.fetchall()
        except Exception:
            logging.exception("Query failed: %s", query)
            return None

    def execute_sql(self, query, params=None):
        with psycopg.connect(
            dbname=self.dbname,
            user=self.user,
            port=self.port,
            host=self.host,
            password=self.password,
        ) as conn:
            with conn.cursor() as curs:
                curs.execute(query, params)

    def dataset_query(self, dataset_id: str, table_name: str):
        allowed_tables = {"configurations", "property_objects"}
        if table_name not in allowed_tables:
            raise ValueError(f"table_name must be one of {allowed_tables}")
        if table_name == "configurations":
            query = sql.SQL("SELECT * FROM {} WHERE {} = ANY({})").format(
                sql.Identifier(table_name),
                sql.Placeholder(),
                sql.Identifier("dataset_ids"),
            )
        else:
            query = sql.SQL("SELECT * FROM {} WHERE {} = {}").format(
                sql.Identifier(table_name),
                sql.Identifier("dataset_id"),
                sql.Placeholder(),
            )
        return self.general_query(query, (dataset_id,))

    def get_dataset(self, dataset_id: str):
        query = sql.SQL("SELECT * FROM {} WHERE {} = {}").format(
            sql.Identifier("datasets"),
            sql.Identifier("id"),
            sql.Placeholder(),
        )
        return self.general_query(query, (dataset_id,))

    def create_configuration_sets(
        self,
        dataset_id: str,
        name_label_match: list[tuple],
    ):
        """Create configuration sets for a dataset by matching configuration names/labels.

        Args:
            dataset_id: Dataset ID to create configuration sets for.
            name_label_match: List of tuples, each containing:
                1. String pattern for matching configuration names (None = match all)
                2. String pattern for matching configuration labels (None = match all)
                3. Name for the configuration set
                4. Description for the configuration set
        """
        config_rows = self.dataset_query(dataset_id, "configurations") or []
        config_set_rows = []
        for names_match, label_match, cs_name, cs_desc in tqdm(
            name_label_match, desc="Creating Configuration Sets"
        ):
            matched = []
            for c in config_rows:
                co_names = c.get("names") or []
                co_labels = c.get("labels") or []
                name_ok = names_match is None or any(
                    names_match in n for n in co_names
                )
                label_ok = label_match is None or any(
                    label_match in lb for lb in co_labels
                )
                if name_ok and label_ok:
                    matched.append(c)
            if not matched:
                continue
            config_set = ConfigurationSet(
                name=cs_name,
                description=cs_desc,
                config_df=matched,
                dataset_id=dataset_id,
            )
            co_ids = [c["id"] for c in matched]
            self._append_cs_id_to_configs(co_ids, config_set.id)
            self._insert_configuration_set(config_set.row_dict)
            config_set_rows.append(config_set.row_dict)
        return config_set_rows

    def _append_cs_id_to_configs(self, co_ids: list, cs_id: str):
        """Append a configuration set ID to configuration_set_ids for each CO."""
        query = sql.SQL(
            "UPDATE {table} SET {col} = array_append({col}, {val}) "
            "WHERE {id_col} = ANY({ids})"
        ).format(
            table=sql.Identifier("configurations"),
            col=sql.Identifier("configuration_set_ids"),
            val=sql.Placeholder(),
            id_col=sql.Identifier("id"),
            ids=sql.Placeholder(),
        )
        with psycopg.connect(
            dbname=self.dbname,
            user=self.user,
            port=self.port,
            host=self.host,
            password=self.password,
        ) as conn:
            with conn.cursor() as curs:
                curs.execute(query, (cs_id, co_ids))

    def _insert_configuration_set(self, cs_row: dict):
        """Insert a configuration set row; skip on hash conflict."""
        columns = configuration_set_schema.column_names
        query = sql.SQL(
            "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT (hash) DO NOTHING"
        ).format(
            sql.Identifier(configuration_set_schema.name),
            sql.SQL(",").join(map(sql.Identifier, columns)),
            sql.SQL(",").join([sql.Placeholder()] * len(columns)),
        )
        vals = [cs_row.get(c) for c in columns]
        self.execute_sql(query, vals)

    def delete_dataset(self, dataset_id: str):
        delete_sql = "DELETE FROM datasets WHERE id = %s;"
        with psycopg.connect(
            dbname=self.dbname,
            user=self.user,
            port=self.port,
            host=self.host,
            password=self.password,
        ) as conn:
            with conn.cursor() as curs:
                curs.execute(delete_sql, (dataset_id,))
