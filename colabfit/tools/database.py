import datetime
import hashlib
import json
import itertools
import os
import string
from ast import literal_eval
from functools import partial
from itertools import islice
from multiprocessing import Pool
from pathlib import Path
from time import time
from types import GeneratorType
from typing import List

import boto3
import dateutil.parser
import psycopg
from psycopg.rows import dict_row
#import pyarrow as pa
#import pyspark.sql.functions as sf
from botocore.exceptions import ClientError
from django.utils.crypto import get_random_string
#from dotenv import load_dotenv
#from ibis import _
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from tqdm import tqdm
#from vastdb.session import Session
from ase import Atoms

from colabfit import (
    ID_FORMAT_STRING,
)  # ATOMS_NAME_FIELD,; EXTENDED_ID_STRING_NAME,; MAX_STRING_LENGTH,; SHORT_ID_STRING_NAME,; _CONFIGS_COLLECTION,; _CONFIGSETS_COLLECTION,; _DATASETS_COLLECTION,; _PROPOBJECT_COLLECTION, # noqa
from colabfit.tools.configuration import AtomicConfiguration
from colabfit.tools.configuration_set import ConfigurationSet
from colabfit.tools.dataset import Dataset
from colabfit.tools.property import Property
from colabfit.tools.property_definitions import atomic_forces_pd, energy_pd, cauchy_stress_pd, quests_descriptor_pd 
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from colabfit.tools.schema import (
    config_df_schema,
    config_md_schema,
    config_schema,
    configuration_set_df_schema,
    configuration_set_schema,
    dataset_df_schema,
    dataset_schema,
    property_object_df_schema,
    property_object_md_schema,
    property_object_schema,
    co_cs_mapping_schema,
)
from colabfit.tools.utilities import (
    _hash,
    get_spark_field_type,
    spark_schema_to_arrow_schema,
    split_long_string_cols,
    stringify_df_val,
    unstring_df_val,
)

VAST_BUCKET_DIR = "colabfit-data"
VAST_METADATA_DIR = "data/MD"
NSITES_COL_SPLITS = 20
_CONFIGS_COLLECTION = "test_configs"
_CONFIGSETS_COLLECTION = "test_config_sets"
_DATASETS_COLLECTION = "test_datasets"
_PROPOBJECT_COLLECTION = "test_prop_objects"
_CO_CS_MAP_COLLECTION = "test_co_cs_map"
_MAX_STRING_LEN = 60000

# from kim_property.definition import PROPERTY_ID as VALID_KIM_ID

from kim_property.definition import check_property_definition


def generate_string():
    return get_random_string(12, allowed_chars=string.ascii_lowercase + "1234567890")

'''
class VastDataLoader:
    def __init__(
        self,
        table_prefix: str = "ndb.colabfit.dev",
        endpoint=None,
        access_key=None,
        access_secret=None,
    ):
        self.table_prefix = table_prefix
        self.spark = (
            SparkSession.builder.appName("ColabFitDataLoader")
            # .config("spark.dynamicAllocation.enabled", "true")
            # .config("spark.dynamicAllocation.minExecutors", "1")
            # .config("spark.dynamicAllocation.maxExecutors", "10")
            # .config("spark.task.maxFailures", "4")
            # .config("spark.network.timeout", "300s")
            # .config("spark.speculation", "true")
            # .config("spark.executor.memory", "4g")
            .getOrCreate()
        )
        self.spark.sparkContext.setLogLevel("ERROR")
        if endpoint and access_key and access_secret:
            self.endpoint = endpoint
            self.access_key = access_key
            self.access_secret = access_secret
            self.session = self.get_vastdb_session(
                endpoint=self.endpoint,
                access_key=self.access_key,
                access_secret=self.access_secret,
            )
        self.config_table = f"{self.table_prefix}.{_CONFIGS_COLLECTION}"
        self.config_set_table = f"{self.table_prefix}.{_CONFIGSETS_COLLECTION}"
        self.dataset_table = f"{self.table_prefix}.{_DATASETS_COLLECTION}"
        self.prop_object_table = f"{self.table_prefix}.{_PROPOBJECT_COLLECTION}"
        self.co_cs_map_table = f"{self.table_prefix}.{_CO_CS_MAP_COLLECTION}"

        self.bucket_dir = VAST_BUCKET_DIR
        self.metadata_dir = VAST_METADATA_DIR

    def get_vastdb_session(self, endpoint, access_key: str, access_secret: str):
        return Session(endpoint=endpoint, access=access_key, secret=access_secret)

    def set_vastdb_session(self, endpoint, access_key: str, access_secret: str):
        self.session = self.get_vastdb_session(endpoint, access_key, access_secret)
        self.access_key = access_key
        self.access_secret = access_secret
        self.endpoint = endpoint

    def _get_table_split(self, table_name_str: str):
        """Get bucket, schema and table names for VastDB SDK, with no backticks"""
        table_split = table_name_str.split(".")
        bucket_name = table_split[1].replace("`", "")
        schema_name = table_split[2].replace("`", "")
        table_name = table_split[3].replace("`", "")
        return (bucket_name, schema_name, table_name)

    def add_elem_to_col(df, col_name: str, elem: str):
        df_added_elem = df.withColumn(
            col_name,
            sf.when(
                sf.col(col_name).isNull(), sf.array().cast(ArrayType(StringType()))
            ).otherwise(sf.col(col_name)),
        )
        df_added_elem = df_added_elem.withColumn(
            col_name, sf.array_union(sf.col(col_name), sf.array(sf.lit(elem)))
        )
        return df_added_elem

    def delete_from_table(self, table_name: str, ids: list[str]):
        if isinstance(ids, str):
            ids = [ids]
        bucket_name, schema_name, table_n = self._get_table_split(table_name)
        with self.session.transaction() as tx:
            table = tx.bucket(bucket_name).schema(schema_name).table(table_n)
            rec_batch = table.select(
                predicate=table["id"].isin(ids), internal_row_id=True
            )
            for batch in rec_batch:
                table.delete(rows=batch)

    def check_unique_ids(self, table_name: str, df):
        if not self.spark.catalog.tableExists(table_name):
            print(f"Table {table_name} does not yet exist.")
            return True
        ids = [x["id"] for x in df.select("id").collect()]
        # table_df = self.read_table(table_name)
        # table_df = table_df.select("id")
        bucket_name, schema_name, table_n = self._get_table_split(table_name)
        with self.session.transaction() as tx:
            table = tx.bucket(bucket_name).schema(schema_name).table(table_n)
            for id_batch in tqdm(
                batched(ids, 10000), desc=f"Checking for duplicates in {table_name}"
            ):
                rec_batch_reader = table.select(
                    predicate=table["id"].isin(id_batch), columns=["id"]
                )
                for batch in rec_batch_reader:
                    if batch.num_rows > 0:
                        print(f"Duplicate IDs found in table {table_name}")
                        return False
        return True

        # dupes_exist = id_df.join(table_df, on="id", how="inner")
        # if not dupes_exist.rdd.isEmpty():
        #     # if len(dupes_exist.take(1)) > 0:
        #     print(f"Duplicate IDs found in table {table_name}")
        #     return False
        # return True

    def write_table(
        self,
        spark_df,
        table_name: str,
        ids_filter: list[str] = None,
        check_length_col: str = None,
        check_unique: bool = True,
    ):
        # print(spark_df.first())
        """Include self.table_prefix in the table name when passed to this function"""
        string_schema_dict = {
            self.config_table: config_schema,
            self.config_set_table: configuration_set_schema,
            self.dataset_table: dataset_schema,
            self.prop_object_table: property_object_schema,
            self.co_cs_map_table: co_cs_mapping_schema,
        }
        table_schema = string_schema_dict[table_name]
        if ids_filter is not None:
            spark_df = spark_df.filter(sf.col("id").isin(ids_filter))
        if check_unique:
            all_unique = self.check_unique_ids(table_name, spark_df)
            if not all_unique:
                raise ValueError("Duplicate IDs found in table. Not writing.")
        bucket_name, schema_name, table_n = self._get_table_split(table_name)
        string_cols = [
            f.name for f in spark_df.schema if f.dataType.typeName() == "array"
        ]
        string_col_udf = sf.udf(stringify_df_val, StringType())
        for col in string_cols:
            spark_df = spark_df.withColumn(col, string_col_udf(sf.col(col)))
        if check_length_col is not None:
            spark_df = split_long_string_cols(
                spark_df, check_length_col, _MAX_STRING_LEN
            )
        arrow_schema = spark_schema_to_arrow_schema(table_schema)
        # print(arrow_schema)
        for field in arrow_schema:
            field = field.with_nullable(True)
        if not self.spark.catalog.tableExists(table_name):
            print(f"Creating table {table_name}")

            with self.session.transaction() as tx:
                schema = tx.bucket(bucket_name).schema(schema_name)
                schema.create_table(table_n, arrow_schema)
        arrow_rec_batch = pa.table(
            [pa.array(col) for col in zip(*spark_df.collect())],
            # names=spark_df.columns,
            schema=arrow_schema,
        ).to_batches()
        total_rows = 0
        with self.session.transaction() as tx:
            table = tx.bucket(bucket_name).schema(schema_name).table(table_n)
            for rec_batch in arrow_rec_batch:
                len_batch = rec_batch.num_rows
                table.insert(rec_batch)
                total_rows += len_batch
        print(f"Inserted {total_rows} rows into table {table_name}")

    def write_metadata(self, df):
        """Writes metadata to files using boto3 for VastDB
        Returns a DataFrame without metadata column. The returned DataFrame should
        match table schema (from schema.py)
        """
        if df.filter(sf.col("metadata").isNotNull()).count() == 0:
            df = df.drop("metadata")
            return df
        config = {
            "bucket_dir": self.bucket_dir,
            "access_key": self.access_key,
            "access_secret": self.access_secret,
            "endpoint": self.endpoint,
            "metadata_dir": self.metadata_dir,
        }
        beg = time()
        distinct_metadata = df.select("metadata", "metadata_path").distinct()
        distinct_metadata.foreachPartition(
            lambda partition: write_md_partition(partition, config)
        )
        print(f"Time to write metadata: {time() - beg}")
        df = df.drop("metadata")
        # file_base = f"/vdev/{VAST_BUCKET_DIR}/{VAST_METADATA_DIR}/"
        file_base = f"{self.metadata_dir}/"
        df = df.withColumn(
            "metadata_path",
            prepend_path_udf(sf.lit(str(Path(file_base))), sf.col("metadata_path")),
        )
        return df

    def update_existing_co_po_rows(
        self,
        df,
        table_name,
        cols: list[str],
        elems: list[str],
        str_schema,
        unstr_schema,
        arrow_schema,
        update_cols,
        arr_cols,
    ):
        """
        Updates existing rows in CO or PO table with data from new ingest.

        Parameters:
        -----------
        df : DataFrame
            The DataFrame containing the new data to be updated.
        table_name : str
            The name of the table to be updated.
        cols : list[str]
            List of column names to be updated.
        elems : list[str]
            List of elements corresponding to the columns to be updated.
        str_schema : Schema
            The stringed schema of the table.
        unstr_schema : Schema
            The unstringed schema of the table.
        arrow_schema : Schema
            The Arrow schema of the columns to be updated.
        update_cols : list[str]
            List of columns to be updated.
        arr_cols : list[str]
            List of columns that contain array data.

        Returns:
        --------
        tuple
            A tuple containing two lists:
            - new_ids: List of IDs that were newly added.
            - existing_ids: List of IDs that were updated.
        """
        if isinstance(cols, str):
            cols = [cols]
        if isinstance(elems, str):
            elems = [elems]

        str_col_types = {
            col: get_spark_field_type(str_schema, col) for col in update_cols
        }
        unstr_col_types = {col: get_spark_field_type(unstr_schema, col) for col in cols}
        addtl_fields = {
            "id": StringType(),
            "last_modified": TimestampType(),
            "$row_id": LongType(),
        }
        str_col_types.update(addtl_fields)
        unstr_col_types.update(addtl_fields)
        str_spark_schema = StructType(
            [StructField(col, str_col_types[col], True) for col in update_cols]
            + [
                StructField("id", StringType(), False),
                StructField("$row_id", IntegerType(), False),
            ]
        )
        total_write_cols = update_cols + ["$row_id"]
        ids = [x["id"] for x in df.select("id").collect()]
        batched_ids = batched(ids, 10000)
        new_ids = []
        existing_ids = []
        bucket_name, schema_name, table_n = self._get_table_split(table_name)
        for id_batch in batched_ids:
            id_batch = list(set(id_batch))
            with self.session.transaction() as tx:
                table = tx.bucket(bucket_name).schema(schema_name).table(table_n)
                rec_batch = table.select(
                    predicate=table["id"].isin(id_batch),
                    columns=update_cols + ["id"],
                    internal_row_id=True,
                )
                rec_batch = rec_batch.read_all()
                duplicate_df = self.spark.createDataFrame(
                    rec_batch.to_struct_array().to_pandas(), schema=str_spark_schema
                )
            if duplicate_df.count() == 0:
                new_ids.extend(id_batch)
                continue
            for col_name in arr_cols:
                unstring_udf = sf.udf(unstring_df_val, unstr_col_types[col_name])
                duplicate_df = duplicate_df.withColumn(
                    col_name, unstring_udf(sf.col(col_name))
                )
            for col, elem in zip(cols, elems):
                if col in ["labels", "names"]:
                    if (
                        col == "labels"
                        and df.filter(sf.col("labels").isNotNull()).count() == 0
                    ):
                        continue
                    df_add = df.select("id", col)
                    duplicate_df = (
                        duplicate_df.withColumnRenamed(col, f"{col}_dup")
                        .join(df_add, on="id")
                        .withColumn(
                            col, sf.array_distinct(sf.array_union(f"{col}_dup", col))
                        )
                        .drop(f"{col}_dup")
                    )
                elif col == "multiplicity":
                    df_add = df.select(
                        "id", sf.col("multiplicity").alias("multiplicity_add")
                    )
                    duplicate_df = duplicate_df.join(df_add, on="id", how="left")
                    duplicate_df = duplicate_df.withColumn(
                        "multiplicity",
                        sf.col("multiplicity") + sf.col("multiplicity_add"),
                    ).drop("multiplicity_add")
                else:
                    print(col, unstr_col_types[col])
                    duplicate_df = duplicate_df.withColumn(
                        col, sf.coalesce(sf.col(col), sf.array())
                    )
                    duplicate_df = duplicate_df.withColumn(
                        col,
                        sf.array_distinct(
                            sf.array_union(sf.col(col), sf.array(sf.lit(elem)))
                        ),
                    )
            existing_ids_batch = [x["id"] for x in duplicate_df.select("id").collect()]
            new_ids_batch = [id for id in id_batch if id not in existing_ids_batch]
            string_udf = sf.udf(stringify_df_val, StringType())
            for col_name in arr_cols:
                duplicate_df = duplicate_df.withColumn(
                    col_name, string_udf(sf.col(col_name))
                )
            update_time = dateutil.parser.parse(
                datetime.datetime.now(tz=datetime.timezone.utc).strftime(
                    "%Y-%m-%dT%H:%M:%SZ"
                )
            )
            duplicate_df = duplicate_df.withColumn(
                "last_modified", sf.lit(update_time).cast("timestamp")
            )
            arrow_schema = pa.schema(
                [arrow_schema.field(col) for col in total_write_cols]
            )
            update_table = pa.table(
                [
                    pa.array(col)
                    for col in zip(*duplicate_df.select(total_write_cols).collect())
                ],
                schema=arrow_schema,
            )
            with self.session.transaction() as tx:
                table = tx.bucket(bucket_name).schema(schema_name).table(table_n)
                table.update(rows=update_table, columns=update_cols)
            new_ids.extend(new_ids_batch)
            existing_ids.extend(existing_ids_batch)
        return (new_ids, list(set(existing_ids)))

    def update_existing_co_rows(self, co_df, cols: list[str], elems: list[str]):
        update_cols = [
            col for col in config_schema.fieldNames() if col not in ["id", "$row_id"]
        ]
        # cols_types = [
        #     (col, dtype)
        #     for col, dtype in zip(
        #         cols, [get_spark_field_type(config_df_schema, col) for col in cols]
        #     )
        # ]
        # arr_cols = [
        #     (col, dtype) for col, dtype in cols_types if dtype.typeName() == "array"
        # ]
        arr_cols = [
            col
            for col in cols
            if get_spark_field_type(config_df_schema, col).typeName() == "array"
        ]
        arrow_schema = spark_schema_to_arrow_schema(config_schema)
        arrow_schema = arrow_schema.append(pa.field("$row_id", pa.uint64()))
        return self.update_existing_co_po_rows(
            df=co_df,
            table_name=self.config_table,
            cols=cols,
            elems=elems,
            str_schema=config_schema,
            unstr_schema=config_df_schema,
            arrow_schema=arrow_schema,
            update_cols=update_cols,
            arr_cols=arr_cols,
        )

    def update_existing_po_rows(self, po_df):
        update_cols = ["multiplicity", "last_modified"]
        arr_cols = []
        return self.update_existing_co_po_rows(
            df=po_df,
            table_name=self.prop_object_table,
            cols=["multiplicity"],
            elems=[None],
            str_schema=property_object_schema,
            unstr_schema=property_object_df_schema,
            arrow_schema=pa.schema(
                [
                    pa.field("id", pa.string()),
                    pa.field("multiplicity", pa.int32()),
                    pa.field("last_modified", pa.timestamp("us")),
                    pa.field("$row_id", pa.uint64()),
                ]
            ),
            update_cols=update_cols,
            arr_cols=arr_cols,
        )

    def read_table(
        self, table_name: str, unstring: bool = False, read_metadata: bool = False
    ):
        """
        Include self.table_prefix in the table name when passed to this function.
        Ex: loader.read_table(loader.config_table, unstring=True)
        Arguments:
            table_name {str} -- Name of the table to read from database
        Keyword Arguments:
            unstring {bool} -- Convert stringified lists to lists (default: {False})
            read_metadata {bool} -- Read metadata from files. If True,
            lists will be also converted from strings (default: {False})
        Returns:
            DataFrame -- Spark DataFrame
        """
        string_schema_dict = {
            self.config_table: config_schema,
            self.config_set_table: configuration_set_schema,
            self.dataset_table: dataset_schema,
            self.prop_object_table: property_object_schema,
            self.co_cs_map_table: co_cs_mapping_schema,
        }
        unstring_schema_dict = {
            self.config_table: config_df_schema,
            self.config_set_table: configuration_set_df_schema,
            self.dataset_table: dataset_df_schema,
            self.prop_object_table: property_object_df_schema,
        }
        md_schema_dict = {
            self.config_table: config_md_schema,
            self.config_set_table: configuration_set_df_schema,
            self.dataset_table: dataset_df_schema,
            self.prop_object_table: property_object_md_schema,
        }
        if table_name in [self.config_set_table, self.dataset_table]:
            read_metadata = False
        df = self.spark.read.table(table_name)
        if unstring or read_metadata:
            schema = unstring_schema_dict[table_name]
            schema_type_dict = {f.name: f.dataType for f in schema}
            string_cols = [f.name for f in schema if f.dataType.typeName() == "array"]
            for col in string_cols:
                string_col_udf = sf.udf(unstring_df_val, schema_type_dict[col])
                df = df.withColumn(col, string_col_udf(sf.col(col)))
        if read_metadata:
            schema = md_schema_dict[table_name]
            config = {
                "bucket_dir": self.bucket_dir,
                "access_key": self.access_key,
                "access_secret": self.access_secret,
                "endpoint": self.endpoint,
                "metadata_dir": self.metadata_dir,
            }
            df = df.rdd.mapPartitions(
                lambda partition: read_md_partition(partition, config)
            ).toDF(schema)
        if not read_metadata and not unstring:
            schema = string_schema_dict[table_name]
        mismatched_cols = [
            x
            for x in [(f.name, f.dataType.typeName()) for f in df.schema]
            if x not in [(f.name, f.dataType.typeName()) for f in schema]
        ]
        if len(mismatched_cols) == 0:
            return df
        else:
            raise ValueError(
                f"Schema mismatch for table {table_name}. "
                f"Mismatched column types in DataFrame: {mismatched_cols}"
            )

    def zero_multiplicity(self, dataset_id):
        """Use to return multiplicity of POs for a given dataset to zero"""
        table_exists = self.spark.catalog.tableExists(self.prop_object_table)
        if not table_exists:
            print(f"Table {self.prop_object_table} does not exist")
            return
        spark_schema = StructType(
            [
                StructField("id", StringType(), False),
                StructField("multiplicity", IntegerType(), True),
                StructField("last_modified", TimestampType(), False),
                StructField("$row_id", IntegerType(), False),
            ]
        )
        with self.session.transaction() as tx:
            table_name = self.prop_object_table
            bucket_name, schema_name, table_n = self._get_table_split(table_name)
            table = tx.bucket(bucket_name).schema(schema_name).table(table_n)
            rec_batches = table.select(
                predicate=(table["dataset_id"] == dataset_id)
                & (table["multiplicity"] > 0),
                columns=["id", "multiplicity", "last_modified"],
                internal_row_id=True,
            )
            for rec_batch in rec_batches:
                df = self.spark.createDataFrame(
                    rec_batch.to_struct_array().to_pandas(), schema=spark_schema
                )
                df = df.withColumn("multiplicity", sf.lit(0))
                print(f"Zeroed {df.count()} property objects")
                update_time = dateutil.parser.parse(
                    datetime.datetime.now(tz=datetime.timezone.utc).strftime(
                        "%Y-%m-%dT%H:%M:%SZ"
                    )
                )
                df = df.withColumn(
                    "last_modified", sf.lit(update_time).cast("timestamp")
                )
                arrow_schema = pa.schema(
                    [
                        pa.field("id", pa.string()),
                        pa.field("multiplicity", pa.int32()),
                        pa.field("last_modified", pa.timestamp("us")),
                        pa.field("$row_id", pa.uint64()),
                    ]
                )
                update_table = pa.table(
                    [pa.array(col) for col in zip(*df.collect())], schema=arrow_schema
                )
                table.update(
                    rows=update_table,
                    columns=["multiplicity", "last_modified"],
                )

    def get_pos_cos_by_filter(
        self,
        po_filter_conditions: list[tuple[str, str, str | int | float | list]] = None,
        co_filter_conditions: list[
            tuple[str, str, str | int | float | list | None]
        ] = None,
    ):
        """
        example filter conditions:
        po_filter_conditions = [("dataset_id", "=", "ds_id1"),
                                ("method", "like", "DFT%")]
        co_filter_conditions = [("nsites", ">", 15),
                                ('labels', 'array_contains', 'label1')]
        """
        po_df = self.read_table(self.prop_object_table, unstring=True)
        po_df = self.get_filtered_table(po_df, po_filter_conditions)
        po_df = po_df.drop("chemical_formula_hill")

        co_df = self.read_table(self.config_table, unstring=True)
        overlap_cols = [col for col in po_df.columns if col in co_df.columns]
        po_df = po_df.select(
            [
                (
                    col
                    if col not in overlap_cols
                    else sf.col(col).alias(f"prop_object_{col}")
                )
                for col in po_df.columns
            ]
        )
        co_df = co_df.select(
            [
                (
                    col
                    if col not in overlap_cols
                    else sf.col(col).alias(f"configuration_{col}")
                )
                for col in co_df.columns
            ]
        )
        co_df = self.get_filtered_table(co_df, co_filter_conditions)
        co_po_df = co_df.join(po_df, on="configuration_id", how="inner")
        return co_po_df

    def simple_sdk_query(self, query_table, predicate, schema, internal_row_id=False):
        bucket_name, schema_name, table_n = self._get_table_split(query_table)
        with self.session.transaction() as tx:
            table = tx.bucket(bucket_name).schema(schema_name).table(table_n)
            rec_batch_reader = table.select(
                predicate=predicate, internal_row_id=internal_row_id
            )
            rec_batch = rec_batch_reader.read_all()
            if rec_batch.num_rows == 0:
                print(f"No records found for given query {predicate}")
                return self.spark.createDataFrame([], schema=schema)
            spark_df = self.spark.createDataFrame(
                rec_batch.to_struct_array().to_pandas(), schema=schema
            )
        return spark_df

    def get_co_cs_mapping(self, cs_id: str):
        """
        Get configuration to configuration set mapping for a given ID.

        Args:
            cs_id (str): Configuration set ID.

        Returns:
            DataFrame or None: Mapping DataFrame if found, else None.

        Notes:
            - Prints message and returns None if mapping table doesn't exist.
            - Prints message and returns None if no records found for the given ID.
        """
        if not self.spark.catalog.tableExists(self.co_cs_map_table):
            print(f"Table {self.co_cs_map_table} does not exist")
            return None
        predicate = _.configuration_set_id == cs_id
        co_cs_map = self.simple_sdk_query(
            self.co_cs_map_table, predicate, co_cs_mapping_schema
        )
        if co_cs_map.count() == 0:
            print(f"No records found for given configuration set id {cs_id}")
            return None
        return co_cs_map

    def dataset_query(
        self,
        dataset_id=None,
        table_name=None,
    ):
        if dataset_id is None:
            raise ValueError("dataset_id must be provided")
        string_schema_dict = {
            self.config_table: config_schema,
            self.config_set_table: configuration_set_schema,
            self.dataset_table: dataset_schema,
            self.prop_object_table: property_object_schema,
        }
        df_schema = string_schema_dict[table_name]
        if table_name == self.config_table:
            predicate = _.dataset_ids.contains(dataset_id)
        elif table_name == self.prop_object_table or table_name == self.config_set_table:
            predicate = _.dataset_id == dataset_id
        spark_df = self.simple_sdk_query(table_name, predicate, df_schema)
        unstring_schema_dict = {
            self.config_table: config_df_schema,
            self.config_set_table: configuration_set_df_schema,
            self.dataset_table: dataset_df_schema,
            self.prop_object_table: property_object_df_schema,
        }
        schema = unstring_schema_dict[table_name]
        schema_type_dict = {f.name: f.dataType for f in schema}
        string_cols = [f.name for f in schema if f.dataType.typeName() == "array"]
        for col in string_cols:
            string_col_udf = sf.udf(unstring_df_val, schema_type_dict[col])
            spark_df = spark_df.withColumn(col, string_col_udf(sf.col(col)))
        return spark_df

    def config_set_query(
        self,
        query_table,
        dataset_id=None,
        name_match=None,
        label_match=None,
        configuration_ids=None,
    ):
        if dataset_id is None:
            raise ValueError("dataset_id must be provided")
        string_schema_dict = {
            self.config_table: config_schema,
            self.config_set_table: configuration_set_schema,
            self.dataset_table: dataset_schema,
            self.prop_object_table: property_object_schema,
        }
        df_schema = string_schema_dict[query_table]
        if query_table == self.config_table:
            if name_match is None and label_match is None:
                predicate = _.dataset_ids.contains(dataset_id)
            if name_match is not None and label_match is not None:
                predicate = (
                    (_.dataset_ids.contains(dataset_id))
                    & (_.names.contains(name_match))
                    & (_.labels.contains(label_match))
                )
            elif name_match is not None:
                predicate = (_.dataset_ids.contains(dataset_id)) & (
                    _.names.contains(name_match)
                )
            else:
                predicate = (_.dataset_ids.contains(dataset_id)) & (
                    _.labels.contains(label_match)
                )
            spark_df = self.simple_sdk_query(query_table, predicate, df_schema)
            return spark_df
        elif query_table == self.prop_object_table:
            if configuration_ids is None:
                predicate = _.dataset_id == dataset_id
                spark_df = self.simple_sdk_query(query_table, predicate, df_schema)
            if configuration_ids is not None and len(configuration_ids) < 10000:
                predicate = (_.dataset_id == dataset_id) & (
                    _.configuration_id.isin(configuration_ids)
                )
                spark_df = self.simple_sdk_query(query_table, predicate, df_schema)
            else:
                config_id_batches = batched(configuration_ids, 10000)
                spark_df = self.spark.createDataFrame([], schema=df_schema)
                for batch in config_id_batches:
                    predicate = (_.dataset_id == dataset_id) & (
                        _.configuration_id.isin(batch)
                    )
                    batch_spark_df = self.simple_sdk_query(
                        query_table, predicate, df_schema
                    )
                    spark_df = spark_df.union(batch_spark_df)
            return spark_df

    def get_filtered_table(
        self,
        df,
        filter_conditions: list[tuple[str, str, str | int | float | list]] | None = None,
    ):
        if filter_conditions is None:
            return df
        for i, (column, operand, condition) in enumerate(filter_conditions):
            if operand == "in":
                df = df.filter(sf.col(column).isin(condition))
            elif operand == "like":
                df = df.filter(sf.col(column).like(condition))
            elif operand == "rlike":
                df = df.filter(sf.col(column).rlike(condition))
            elif operand == "==":
                df = df.filter(sf.col(column) == condition)
            elif operand == "array_contains":
                df = df.filter(sf.array_contains(sf.col(column), condition))
            elif operand == ">":
                df = df.filter(sf.col(column) > condition)
            elif operand == "<":
                df = df.filter(sf.col(column) < condition)
            elif operand == ">=":
                df = df.filter(sf.col(column) >= condition)
            elif operand == "<=":
                df = df.filter(sf.col(column) <= condition)
            else:
                raise ValueError(
                    f"Operand {operand} not implemented in get_pos_cos_filter"
                )
        return df

    def rehash_property_objects(spark_row: Row):
        """
        Rehash property object row after changing values of one or
        more of the columns corresponding to hash_keys defined below.

        """
        hash_keys = [
            "adsorption_energy",
            "atomic_forces",
            "atomization_energy",
            "cauchy_stress",
            "cauchy_stress_volume_normalized",
            "chemical_formula_hill",
            "configuration_id",
            "dataset_id",
            "electronic_band_gap",
            "electronic_band_gap_type",
            "energy",
            "formation_energy",
            "metadata_id",
            "method",
            "software",
        ]
        spark_dict = spark_row.asDict()
        if spark_dict["atomic_forces_01"] is None:
            spark_dict["atomic_forces"] = literal_eval(spark_dict["atomic_forces_00"])
        else:
            spark_dict["atomic_forces"] = list(
                itertools.chain(
                    *[
                        literal_eval(spark_dict[f"atomic_forces_{i:02}"])
                        for i in range(1, 19)
                    ]
                )
            )
        if spark_dict["cauchy_stress"] is not None:
            spark_dict["cauchy_stress"] = literal_eval(spark_dict["cauchy_stress"])
        spark_dict["last_modified"] = dateutil.parser.parse(
            datetime.datetime.now(tz=datetime.timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )
        )
        spark_dict["hash"] = _hash(spark_dict, hash_keys, include_keys_in_hash=False)
        if spark_dict["cauchy_stress"] is not None:
            spark_dict["cauchy_stress"] = str(spark_dict["cauchy_stress"])
        id = f'PO_{spark_dict["hash"]}'
        if len(id) > 28:
            id = id[:28]
        spark_dict["id"] = id
        return Row(**{k: v for k, v in spark_dict.items() if k != "atomic_forces"})

    @udf(returnType=StringType())
    def config_structure_hash(spark_row: Row, hash_keys: list[str]):
        """
        Rehash configuration object row after changing values of one or
        more of the columns corresponding to hash_keys defined below.

        """
        spark_dict = spark_row.asDict()
        if spark_dict["positions_01"] is None:
            spark_dict["positions"] = literal_eval(spark_dict["positions_00"])
        else:
            spark_dict["positions"] = list(
                itertools.chain(
                    *[
                        literal_eval(spark_dict[f"positions_{i:02}"])
                        for i in range(1, 19)
                    ]
                )
            )
        spark_dict["last_modified"] = dateutil.parser.parse(
            datetime.datetime.now(tz=datetime.timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )
        )
        spark_dict["hash"] = _hash(spark_dict, hash_keys, include_keys_in_hash=False)
        return spark_dict["hash"]

    def stop_spark(self):
        self.spark.stop()


class PGDataLoader:
    """
    Class to load data from files to ColabFit PostgreSQL database
    """

    def __init__(
        self,
        appname="colabfit",
        url="jdbc:postgresql://localhost:5432/colabfit",
        database_name: str = None,
        env="./.env",
        table_prefix: str = None,
    ):
        # self.spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
        JARFILE = os.environ.get("CLASSPATH")
        self.spark = (
            SparkSession.builder.appName(appname)
            .config("spark.jars", JARFILE)
            .getOrCreate()
        )

        user = os.environ.get("PGS_USER")
        password = os.environ.get("PGS_PASS")
        driver = os.environ.get("PGS_DRIVER")
        self.properties = {
            "user": user,
            "password": password,
            "driver": driver,
        }
        self.url = url
        self.database_name = database_name
        self.table_prefix = table_prefix
        findspark.init()

        self.format = "jdbc"  # for postgres local
        load_dotenv(env)
        self.config_table = _CONFIGS_COLLECTION
        self.config_set_table = _CONFIGSETS_COLLECTION
        self.dataset_table = _DATASETS_COLLECTION
        self.prop_object_table = _PROPOBJECT_COLLECTION

    def read_table(
        self,
    ):
        pass

    def get_spark(self):
        return self.spark

    def get_spark_context(self):
        return self.spark.sparkContext

    def write_table(self, spark_rows: list[dict], table_name: str, schema: StructType):
        df = self.spark.createDataFrame(spark_rows, schema=schema)

        df.write.jdbc(
            url=self.url,
            table=table_name,
            mode="append",
            properties=self.properties,
        )

    def write_metadata(self, df):
        """Should accept a DataFrame with a metadata column,
        write metadata to files, return DataFrame without metadata column"""
        pass

    # def update_co_rows_cs_id(self, co_ids: list[str], cs_id: str):
    #     with psycopg.connect(
    #         """dbname=colabfit user=%s password=%s host=localhost port=5432"""
    #         % (
    #             self.user,
    #             self.password,
    #         )
    #     ) as conn:
    #         cur = conn.execute(
    #             """UPDATE configurations
    #                     SET configuration_set_ids = concat(%s::text, \
    #             rtrim(ltrim(replace(configuration_set_ids,%s,''), '['),']'), %s::text)
    #             """,
    #             (
    #                 "[",
    #                 f", {cs_id}",
    #                 f", {cs_id}]",
    #             ),
    #             # WHERE id = ANY(%s)""",
    #             # (cs_id, co_ids),
    #         )
    #         conn.commit()
'''

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
        dbname,
        user,
        port,
        host,
        password=None,
        nprocs: int = 1,
        # configs: list[AtomicConfiguration] = None,
        #prop_defs: list[dict] = [atomic_forces_pd, energy_pd, cauchy_stress_pd, quests_descriptor_pd, mask_selection_pd],
        #prop_map: dict = None,
        # dataset_id=None,
        standardize_energy: bool = False,
        read_write_batch_size=10000,
    ):
        self.dbname = dbname
        self.user = user
        self.port = port
        self.user = user
        self.password = password
        self.host = host
        # self.configs = configs
        #if isinstance(prop_defs, dict):
        #    prop_defs = [prop_defs]
        #self.prop_defs = prop_defs
        self.read_write_batch_size = read_write_batch_size
        #self.prop_map = prop_map
        self.nprocs = nprocs
        # self.dataset_id = dataset_id
        self.standardize_energy = standardize_energy
        #if self.dataset_id is None:
        #     self.dataset_id = generate_ds_id()
        #print("Dataset ID:", self.dataset_id)

    @staticmethod
    def _gather_co_po_rows(
        configs: list[AtomicConfiguration],
        prop_defs: list[dict],
        prop_map: dict,
        dataset_id,
        standardize_energy: bool = True,
        strict: bool = False,
    ):
        """Convert COs and DOs to Spark rows."""
        co_po_rows = []
        #po_schema = self.get_table_schema('property_objects')
        for config in configs:
            config.set_dataset_id(dataset_id)
            # TODO: Add PO schema as input to this method so to_spark_row works better
            property = Property.from_definition(
                definitions=prop_defs,
                configuration=config,
                property_map=prop_map,
                standardize_energy=standardize_energy,
                strict=strict,
            )
            co_po_rows.append(
                (
                    config.spark_row,
                    property.spark_row,
                )
            )
        return co_po_rows

    def gather_co_po_rows_pool(
        self, config_chunks: list[list[AtomicConfiguration]], pool, dataset_id=None, prop_map=None, strict=False,
    ):
        """
        Wrapper for _gather_co_po_rows.
        Convert COs and DOs to Spark rows using multiprocessing Pool.
        Returns a batch of tuples of (configuration_row, property_row).
        """

        if dataset_id is None:
            dataset_id = generate_ds_id()

        part_gather = partial(
            self._gather_co_po_rows,
            prop_defs=self.get_property_definitions(),
            prop_map=prop_map,
            dataset_id=dataset_id,
            standardize_energy=self.standardize_energy,
            strict=strict
        )
        return itertools.chain.from_iterable(pool.map(part_gather, list(config_chunks)))

    def gather_co_po_in_batches(self, configs, dataset_id=None, prop_map=None, strict=False):
        """
        Wrapper function for gather_co_po_rows_pool.
        Yields batches of CO-DO rows, preventing configuration iterator from
        being consumed all at once.
        """
        chunk_size = 1000
        config_chunks = batched(configs, chunk_size)
        with Pool(self.nprocs) as pool:
            while True:
                config_batches = list(islice(config_chunks, self.nprocs))
                if not config_batches:
                    break
                else:
                    yield list(self.gather_co_po_rows_pool(config_batches, pool, dataset_id, prop_map, strict))

    def gather_co_po_in_batches_no_pool(self, prop_map=None):
        """
        Wrapper function for gather_co_po_rows_pool.
        Yields batches of CO-DO rows, preventing configuration iterator from
        being consumed all at once.
        """
        chunk_size = self.read_write_batch_size
        config_chunks = batched(self.configs, chunk_size)
        for chunk in config_chunks:
            yield list(
                self._gather_co_po_rows(
                    self.get_property_definitions(),
                    prop_map,
                    self.dataset_id,
                    chunk,
                    standardize_energy=self.standardize_energy,
                )
            )

    def load_co_po_to_vastdb(self, loader, batching_ingest=False):
        if loader.spark.catalog.tableExists(loader.prop_object_table):
            print("loader.prop_object_table exists")
            if batching_ingest is False:
                pos_with_mult = loader.read_table(loader.prop_object_table)
                pos_with_mult = pos_with_mult.filter(
                    sf.col("dataset_id") == self.dataset_id
                )
                pos_with_mult = pos_with_mult.filter(sf.col("multiplicity") > 0).limit(1)
                if pos_with_mult.count() > 0:
                    raise ValueError(
                        f"POs for dataset with ID {self.dataset_id} already exist in "
                        "database with multiplicity > 0.\nTo continue, set "
                        "multiplicities to 0 with "
                        f'loader.zero_multiplicity("{self.dataset_id}")'
                    )
        if loader.spark.catalog.tableExists(loader.dataset_table):
            dataset_exists = loader.read_table(loader.dataset_table).filter(
                sf.col("id") == self.dataset_id
            )
            if dataset_exists.count() > 0:
                raise ValueError(f"Dataset with ID {self.dataset_id} already exists.")
        co_po_rows = self.gather_co_po_in_batches_no_pool()
        for co_po_batch in tqdm(
            co_po_rows,
            desc="Loading data to database: ",
            unit="batch",
        ):
            co_rows, po_rows = list(zip(*co_po_batch))
            if len(co_rows) == 0:
                continue
            else:
                co_df = loader.spark.createDataFrame(co_rows, schema=config_md_schema)
                po_df = loader.spark.createDataFrame(
                    po_rows, schema=property_object_md_schema
                )
                first_count = co_df.count()
                print("Dropping duplicates from CO dataframe")
                merged_names = co_df.groupBy("id").agg(
                    sf.array_distinct(sf.flatten(sf.collect_list("names"))).alias(
                        "names"
                    )
                )
                co_df = co_df.dropDuplicates(["id"])
                second_count = co_df.count()
                if second_count < first_count:
                    co_df = (
                        co_df.drop("names")
                        .join(merged_names, on="id", how="inner")
                        .select(config_md_schema.fieldNames())
                    )
                print(f"{first_count -second_count} duplicates found in CO dataframe")
                count = po_df.count()
                count_distinct = po_df.select("id").distinct().count()
                if count_distinct < count:
                    print(f"{count - count_distinct} duplicates found in PO dataframe")
                    multiplicity = po_df.groupBy("id").agg(sf.count("*").alias("count"))
                    po_df = po_df.dropDuplicates(["id"])
                    po_df = (
                        po_df.join(multiplicity, on="id", how="inner")
                        .withColumn("multiplicity", sf.col("count"))
                        .drop("count")
                    )
                all_unique_co = loader.check_unique_ids(loader.config_table, co_df)
                all_unique_po = loader.check_unique_ids(loader.prop_object_table, po_df)
                if not all_unique_co:
                    new_co_ids, update_co_ids = loader.update_existing_co_rows(
                        co_df=co_df,
                        cols=["dataset_ids", "names", "labels"],
                        elems=[self.dataset_id, None, None],
                    )
                    print(f"Updated {len(update_co_ids)} rows in {loader.config_table}")
                    if len(new_co_ids) > 0:
                        print(f"Writing {len(new_co_ids)} new rows to table")
                        co_df = loader.write_metadata(co_df)
                        loader.write_table(
                            co_df,
                            loader.config_table,
                            ids_filter=new_co_ids,
                            check_length_col="positions_00",
                            check_unique=False,
                        )
                else:
                    co_df = loader.write_metadata(co_df)
                    loader.write_table(
                        co_df,
                        loader.config_table,
                        check_length_col="positions_00",
                        check_unique=False,
                    )
                    # print(f"Inserted {co_df.count()} rows into {loader.config_table}")

                if not all_unique_po:
                    # print("Sending to update_existing_po_rows")
                    new_po_ids, update_po_ids = loader.update_existing_po_rows(
                        po_df=po_df,
                    )
                    print(
                        f"Updated {len(update_po_ids)} rows in "
                        f"{loader.prop_object_table}"
                    )
                    if len(new_po_ids) > 0:
                        print("Remaining POs unique. Writing new rows to table...")
                        po_df = loader.write_metadata(po_df)
                        loader.write_table(
                            po_df,
                            loader.prop_object_table,
                            ids_filter=new_po_ids,
                            check_length_col="atomic_forces_00",
                            check_unique=False,
                        )
                    # print(
                    #     f"Inserted {len(new_po_ids)} rows into "
                    #     f"{loader.prop_object_table}"
                    # )
                else:
                    print("All POs unique: writing to table...")
                    po_df = loader.write_metadata(po_df)
                    # print("finished writing metadata")
                    loader.write_table(
                        po_df,
                        loader.prop_object_table,
                        check_length_col="atomic_forces_00",
                        check_unique=False,
                    )
                    print(
                        f"Inserted {len(po_rows)} rows into {loader.prop_object_table}"
                    )


    def load_data_to_pg_in_batches(self, loader):
        """Load data to PostgreSQL in batches."""
        co_po_rows = self.gather_co_po_in_batches()

        for co_po_batch in tqdm(
            co_po_rows,
            desc="Loading data to database: ",
            unit="batch",
        ):
            co_rows, po_rows = list(zip(*co_po_batch))
            if len(co_rows) == 0:
                continue
            else:
                loader.write_table(
                    co_rows,
                    loader.config_table,
                    config_schema,
                )
                loader.write_table(
                    po_rows,
                    loader.prop_object_table,
                    property_object_schema,
                )

    def load_data_to_pg_in_batches_no_spark(
        self, 
        configs, 
        dataset_id=None, 
        config_table=None, 
        prop_object_table=None, 
        prop_map=None, 
        parameters: dict=None,
        strict=False
    ):
        """
        Load data to PostgreSQL in batches.
        
        :param configs:
        :param datset_id:
        :param config_table:
        :param prop_object_table:
        :param prop_map:
        :param parameters: Dictionary containing two nested dictionaries called
            `universal` and `code` to represent universal values in the 
            database and code specific inputs for the dataset.
        :param strict:
        """

        co_po_rows = self.gather_co_po_in_batches(configs, dataset_id, prop_map, strict)
        for co_po_batch in tqdm(
            co_po_rows,
            desc="Loading data to database: ",
            unit="batch",
        ):
            co_rows, po_rows = list(zip(*co_po_batch))

            if len(co_rows) == 0:
                continue
            
            # make tuple of tuples for data
            column_headers = tuple(co_rows[0].keys())
            co_values = []
            for co_row in co_rows:
                t = []
                for column in column_headers:
                    val = co_row[column]
                    if column == 'last_modified':
                        val = val.strftime("%Y-%m-%dT%H:%M:%SZ")
                    #if isinstance(val, (list, tuple, dict)):
                    #    print (column, type(val[0]))
                    #    val = str(val)
                    t.append(val)
                t.append(co_row['dataset_ids'][0])
                # t.append(co_row['dataset_ids'][0])
                co_values.append(t)
            sql_co = "INSERT INTO configurations (id, hash, last_modified, dataset_ids, configuration_set_ids, chemical_formula_hill, chemical_formula_reduced, chemical_formula_anonymous, elements, elements_ratios, atomic_numbers, nsites, nelements, nperiodic_dimensions, cell, dimension_types, pbc, names, labels, positions) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (hash) DO UPDATE SET dataset_ids = array_append(configurations.dataset_ids, %s);"
# TODO: Need to modify dataset.from_pg to properly aggregate values and get data to get two copie

#SET dataset_ids = CASE WHEN NOT (%s = ANY(configurations.dataset_ids)) THEN array_append(configurations.dataset_ids, %s) ELSE configurations.dataset_ids END;"
          
            # TODO: Ensure all columns are present here
            # TODO: get column names from query and ensure len matches values
            columns = list(zip(*self.get_table_schema('property_objects')))[0]
            column_string = ', '.join(list(columns))
            val_string = ', '.join(['%s'] * len(columns))
            po_values = []
            for po_row in po_rows:
                t = []
                for column in columns:
                    #print (column)
                    try:
                        val = po_row[column]
                    except:
                        val = None
                    if column == 'last_modified':
                        val = val.strftime("%Y-%m-%dT%H:%M:%SZ")
                    #if isinstance(val, (list, tuple, dict)):
                    #    print (column, type(val[0]))
                    #    val = str(val)
                    t.append(val)
                po_values.append(t)
            # TODO: get column names from query and ensure len matches values
            sql_po = f"""
                INSERT INTO property_objects ({column_string})
                VALUES ({val_string})
                ON CONFLICT (hash)
                DO UPDATE SET multiplicity = property_objects.multiplicity + 1;

            """

            with psycopg.connect(dbname=self.dbname, user=self.user, port=self.port, host=self.host, password=self.password) as conn:
                with conn.cursor() as curs:
                    curs.executemany(sql_co,co_values)
                    curs.executemany(sql_po,po_values)

        if parameters:
            universal = parameters.get('universal', {})
            code = parameters.get('code', {})
            code = json.dumps(code)

            sql_code = f"""
                INSERT INTO dataset_code_specific_parameters (dataset_id, code_specific_inputs)
                VALUES (%s, %s);
                """
            sql_universal = f"""
                INSERT INTO dataset_universal_parameters (dataset_id, parameter_name, parameter_value)
                VALUES (%s, %s, %s)
                """
            
            family_id = dataset_id.split('_')[1]
            universal_values = []
            for key, value in universal.items():
                universal_values.append([family_id, key, value])

            with psycopg.connect(dbname=self.dbname, user=self.user, port=self.port, host=self.host, password=self.password) as conn:
                with conn.cursor() as curs:
                    curs.execute(sql_code, [family_id, code])
                    curs.executemany(sql_universal, universal_values)

    def create_pg_ds_table(self):
        sql = """
        CREATE TABLE datasets (
        id VARCHAR (256),
        hash VARCHAR (256) PRIMARY KEY,
        name VARCHAR (256),
        last_modified VARCHAR (256),
        nconfigurations INT,
        nproperty_objects INT,
        nsites INT,
        elements VARCHAR (1000) [],
        labels VARCHAR (1000) [],
        nelements INT,
        total_elements_ratio DOUBLE PRECISION [],
        nperiodic_dimensions INT [],
        dimension_types VARCHAR (1000) [],
        energy_count INT,
        energy_mean DOUBLE PRECISION,
        energy_variance DOUBLE PRECISION,
        atomic_forces_count INT,
        cauchy_stress_count INT,
        authors VARCHAR (256) [],
        description VARCHAR (10000),
        extended_id VARCHAR (1000),
        license VARCHAR (256),
        links VARCHAR (1000) [],
        publication_year VARCHAR (256),
        doi VARCHAR (256),
        uploader  VARCHAR (256),
        property_map jsonb
        )
        """
        with psycopg.connect(dbname=self.dbname, user=self.user, port=self.port, host=self.host, password=self.password) as conn:
            with conn.cursor() as curs:
                curs.execute(sql)

    # currently cf-kit table with some properties removed
    def create_pg_po_table(self):
        sql = """
        CREATE TABLE property_objects (
        id VARCHAR (256),
        hash VARCHAR (256) PRIMARY KEY,
        last_modified VARCHAR (256),
        configuration_id VARCHAR (256),
        dataset_id VARCHAR (256),
        multiplicity INT,
        metadata VARCHAR (10000)
        )
        """
        # Don't need anymore
        '''
        chemical_formula_hill VARCHAR (256),
        energy DOUBLE PRECISION,
        atomic_forces_00 DOUBLE PRECISION [] [],
        atomic_forces_01 DOUBLE PRECISION [] [],
        atomic_forces_02 DOUBLE PRECISION [] [],
        atomic_forces_03 DOUBLE PRECISION [] [],
        atomic_forces_04 DOUBLE PRECISION [] [],
        atomic_forces_05 DOUBLE PRECISION [] [],
        atomic_forces_06 DOUBLE PRECISION [] [],
        atomic_forces_07 DOUBLE PRECISION [] [],
        atomic_forces_08 DOUBLE PRECISION [] [],
        atomic_forces_09 DOUBLE PRECISION [] [],
        atomic_forces_10 DOUBLE PRECISION [] [],
        atomic_forces_11 DOUBLE PRECISION [] [],
        atomic_forces_12 DOUBLE PRECISION [] [],
        atomic_forces_13 DOUBLE PRECISION [] [],
        atomic_forces_14 DOUBLE PRECISION [] [],
        atomic_forces_15 DOUBLE PRECISION [] [],
        atomic_forces_16 DOUBLE PRECISION [] [],
        atomic_forces_17 DOUBLE PRECISION [] [],
        atomic_forces_18 DOUBLE PRECISION [] [],
        atomic_forces_19 DOUBLE PRECISION [] [],
        cauchy_stress DOUBLE PRECISION [] []
        )
        '''

        with psycopg.connect(dbname=self.dbname, user=self.user, port=self.port, host=self.host, password=self.password) as conn:
            with conn.cursor() as curs:
                curs.execute(sql)

    def create_pg_co_table(self):
        # TODO: Metadata
        sql = """
        CREATE TABLE configurations (
        id VARCHAR (256),
        hash VARCHAR (256) PRIMARY KEY,
        last_modified VARCHAR (256),
        dataset_ids VARCHAR (256) [],
        configuration_set_ids VARCHAR (256) [],
        chemical_formula_hill VARCHAR (256),
        chemical_formula_reduced VARCHAR (256),
        chemical_formula_anonymous VARCHAR (256),
        elements VARCHAR (256) [],
        elements_ratios DOUBLE PRECISION [],
        atomic_numbers INT [],
        nsites INT,
        nelements INT,
        nperiodic_dimensions INT,
        cell DOUBLE PRECISION [] [],
        dimension_types INT [],
        pbc BOOL[],
        names VARCHAR (256) [],
        labels VARCHAR (256) [],
        positions DOUBLE PRECISION [][]
        )
        """
        with psycopg.connect(dbname=self.dbname, user=self.user, port=self.port, host=self.host, password=self.password) as conn:
            with conn.cursor() as curs:
                curs.execute(sql)

    def create_pg_pd_table(self):
        sql = """
        CREATE TABLE property_definitions (
        hash VARCHAR (256) PRIMARY KEY,
        last_modified VARCHAR (256),
        definition VARCHAR (10000)
        )
        """
        with psycopg.connect(dbname=self.dbname, user=self.user, port=self.port, host=self.host, password=self.password) as conn:
            with conn.cursor() as curs:
                curs.execute(sql)

    def create_pg_code_table(self):
        sql = """
        CREATE TABLE dataset_code_specific_parameters (
            id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            dataset_id character varying NOT NULL,
            code_specific_inputs jsonb NOT NULL,
            "timestamp" timestamp without time zone DEFAULT now() NOT NULL
        );
        """
        with psycopg.connect(dbname=self.dbname, user=self.user, port=self.port, host=self.host, password=self.password) as conn:
            with conn.cursor() as curs:
                curs.execute(sql)

    def create_pg_univ_table(self):
        sql = """
        CREATE TABLE parameter_definition (
            id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            name text NOT NULL UNIQUE,
            description text,
            data_type integer NOT NULL REFERENCES data_type(datatype_id)
        );

        INSERT INTO parameter_definition (id, name, description, data_type) VALUES
            (1, 'code', 'Which software is being used for the calculation', 3),
            (2, 'version', 'Version of the code used', 3),
            (3, 'executable_path', 'Absolute path to the binary', 3),
            (4, 'xc', 'Which exchange correlation functional used', 3),
            (5, 'planewave_cutoff', 'Planewave cutoff for the code', 1),
            (7, 'kspacing', 'Kspacing in reciprocal space (Not multiplied by 2pi)', 2),
            (10, 'smearing_value', 'Value for smearing', 2),
            (11, 'energy_convergence', 'Energy convergence value', 2),
            (12, 'force_convergence', 'Force convergence', 2),
            (13, 'spin_mode', 'Type of spin calculation', 3),
            (15, 'pseudopotential_library', 'Pseudopotential library being used in simulation', 3),
            (16, 'ion_relax', 'Ionic optimization', 4),
            (17, 'cell_relax', 'Cell optimization', 4),
            (18, 'mixing_mode', 'Electronic mixing method', 3),
            (19, 'mixing_value', 'Electronic mixing value', 2),
            (20, 'vdw_correction', 'Type of Van der Waals correction', 3),
            (21, 'hubbard_method', 'Methodology for applying Hubbard U corrections (Default to Dudarev)', 3),
            (22, 'hubbard_orbitals', 'Set which l-quantum number the correction is applied', 1),
            (23, 'hubbard_u', 'Hubbard U correction value', 2),
            (24, 'hubbard_j', 'Hubbard J correction value', 2),
            (14, 'diagonalization_method', 'Diagonalization method', 3),
            (26, 'magnetic_moments', 'Magnetic moments for each atom', 3),
            (27, 'ion_optimization_method', 'Method for optimizing ions', 3),
            (9, 'smearing_method', 'Type of smearing', 3);
        """
        with psycopg.connect(dbname=self.dbname, user=self.user, port=self.port, host=self.host, password=self.password) as conn:
            with conn.cursor() as curs:
                curs.execute(sql)

    def create_pg_param_def_table(self):
        sql = """
        CREATE TABLE parameter_definition (
        id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        name text NOT NULL UNIQUE,
        description text,
        data_type integer NOT NULL,
        CONSTRAINT parameter_definition_data_type_fk FOREIGN KEY (data_type)
            REFERENCES data_type(datatype_id)
        );
        """
        with psycopg.connect(dbname=self.dbname, user=self.user, port=self.port, host=self.host, password=self.password) as conn:
            with conn.cursor() as curs:
                curs.execute(sql)

    def create_pg_data_type_table(self):
        sql = """
        CREATE TABLE data_type (
            datatype_id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            datatype_name varchar NOT NULL UNIQUE,
            description text
        );

        INSERT INTO data_type (datatype_id, datatype_name, description) VALUES
            (1, 'Integer', 'Whole numbers'),
            (2, 'Float', 'Decimal numbers'),
            (3, 'String', 'Text or characters'),
            (4, 'Bool', 'Boolean (True or False)'),
            (5, 'JSON', 'JSON object');
        """
        with psycopg.connect(dbname=self.dbname, user=self.user, port=self.port, host=self.host, password=self.password) as conn:
            with conn.cursor() as curs:
                curs.execute(sql)

    def create_pg_tables(self):
        """
        Single function to create all the tables needed for the colabfit
        database.
        """

        self.create_pg_ds_table()
        self.create_pg_co_table()
        self.create_pg_po_table()
        self.create_pg_pd_table()
        self.create_pg_data_type_table()
        self.create_pg_param_def_table()
        self.create_pg_code_table()
        self.create_pg_univ_table()

    def insert_property_definition(self, property_dict):
        # TODO: try except that property_dict must be jsonable

        # check that property dict has correct form
        # hack to get around OpenKIM requiring the property-name be a dict
        tmp_prop = property_dict.copy()
        tmp_prop.pop("property-name")
        check_property_definition(tmp_prop)
        for key in ["property-id", "property-name", "property-title", "property-description"]:
            assert key in property_dict, f"{key} must be a part of the property definition format"
        json_pd = json.dumps(property_dict) 
        last_modified = dateutil.parser.parse(
            datetime.datetime.now(tz=datetime.timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )
        )
        md5_hash = hashlib.md5(json_pd.encode()).hexdigest()
        sql = """
            INSERT INTO property_definitions (hash, last_modified, definition)
            VALUES (%s, %s, %s)
            ON CONFLICT (hash)
            DO NOTHING
        """
        with psycopg.connect(dbname=self.dbname, user=self.user, port=self.port, host=self.host, password=self.password) as conn:
            with conn.cursor() as curs:
                curs.execute(sql, (md5_hash, last_modified, json_pd))
        # TODO: insert columns into po table
        for key, v in property_dict.items():
            if key in ['property-id', 'property-name', 'property-title', 'property-description',]:
                continue
            else:
                column_name = property_dict['property-name'].replace('-', '_') + f'_{key}'.replace('-', '_')
                if v['type'] == 'float':
                    data_type = "DOUBLE PRECISION"
                elif v['type'] == 'int':
                    data_type = "INT"
                elif v['type'] == 'bool':
                    data_type = "BOOL"
                else:
                    data_type = "VARCHAR (10000)"
                for i in range(len(v['extent'])):
                    data_type += '[]' 
            try:
                self.insert_new_column('property_objects', column_name, data_type)

            except Exception as e:
                print(f"An error occurred: {e}")

    def get_property_definitions(self):
        sql = """
             SELECT definition
             FROM property_definitions;
        """ 
        with psycopg.connect(dbname=self.dbname, user=self.user, port=self.port, host=self.host, password=self.password, row_factory=dict_row) as conn:
            with conn.cursor() as curs:
                curs.execute(sql)
                defs = curs.fetchall()
                dict_defs = []
                for d in defs:
                    dict_defs.append(json.loads(d['definition']))
                return (dict_defs)

    def insert_data_and_create_datset(self,        
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
        config_table=None,
        prop_object_table=None,
        prop_map=None,
        parameters: dict=None,
        strict=False,
        ):

        if dataset_id is None:
            dataset_id = generate_ds_id()

        # convert to CF AtomicConfiguration if not already
        converted_configs = []
        for c in configs:
            if isinstance(c, Atoms):
                converted_configs.append(AtomicConfiguration.from_ase(c))
            elif isinstance(c, AtomicConfiguration):
                converted_configs.append(c)
            else:
                raise Exception("Configs must be an instance of either ase.Atoms or AtomicConfiguration")

        self.load_data_to_pg_in_batches_no_spark(
            converted_configs, 
            dataset_id, 
            config_table, 
            prop_object_table, 
            prop_map,
            parameters,
            strict
        )
        self.create_dataset_pg_no_spark(
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
            property_map = prop_map,
        )
        return dataset_id

    def get_table_schema(self, table_name):
        
        # Query to get the table schema
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
        with psycopg.connect(dbname=self.dbname, user=self.user, port=self.port, host=self.host, password=self.password) as conn:
            with conn.cursor() as curs:
                curs.execute(query, (table_name,))
                schema = curs.fetchall()
                return schema
    
    def get_dataset_property_map(self, dataset_id):
        query = """
             SELECT property_map
             FROM datasets
             WHERE id = %s;
        """
        with psycopg.connect(dbname=self.dbname, user=self.user, port=self.port, host=self.host, password=self.password) as conn:
            with conn.cursor() as curs:
                curs.execute(query, (dataset_id,))
                result = curs.fetchall()
                return result[0][0] 

    def get_dataset_name_from_id(self, dataset_id):
        query = """
             SELECT name
             FROM datasets
             WHERE id = %s;
        """
        with psycopg.connect(dbname=self.dbname, user=self.user, port=self.port, host=self.host, password=self.password) as conn:
            with conn.cursor() as curs:
                curs.execute(query, (dataset_id,))
                result = curs.fetchall()
                return result[0][0]

    def create_dataset_pg_no_spark(self,
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
        property_map: dict = None,
    ):
        # find cs_ids, co_ids, and pi_ids
        config_df = self.dataset_query_pg(dataset_id, 'configurations')
        prop_df = self.dataset_query_pg(dataset_id, 'property_objects')

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
            use_pg = True,
        )
        row = ds.spark_row
        user = os.getlogin()
        sql = """
            INSERT INTO datasets (last_modified, nconfigurations, nproperty_objects, nsites, nelements, elements, total_elements_ratio, nperiodic_dimensions, dimension_types, energy_mean, energy_variance, atomic_forces_count, cauchy_stress_count, energy_count, authors, description, license, links, name, publication_year, doi, id, extended_id, hash, labels, property_map, uploader)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s)
            ON CONFLICT (hash)
            DO NOTHING
        """
        
        column_headers = tuple(row.keys())
        values = []
        for column in column_headers:
            if column in ['nconfiguration_sets']:
                pass
            else:
                val = row[column]
                if column == 'last_modified':
                    val = val.strftime("%Y-%m-%dT%H:%M:%SZ")
                values.append(val)
                #print ('val here',val)
                #print ('t here',t)
                

        values.append(json.dumps(property_map))
        values.append(user)

        with psycopg.connect(dbname=self.dbname, user=self.user, port=self.port, host=self.host, password=self.password) as conn:
            with conn.cursor() as curs:
                curs.executemany(sql, [values])

    def insert_new_column(self, table, column_name, data_type):
        sql = f"""
            ALTER TABLE {table}
            ADD COLUMN {column_name} {data_type};
        """
        with psycopg.connect(dbname=self.dbname, user=self.user, port=self.port, host=self.host, password=self.password) as conn:
            with conn.cursor() as curs:
                curs.execute(sql)


    def update_dataset_pg_no_spark(
        self, 
        configs: List[Atoms], 
        dataset_id: str, 
        parameters: dict,
        prop_map: dict, 
        strict=False
    ):
        # Get family id
        family = dataset_id.split('_')[1]
        # Check if the provided parameters match with the stored values.
        if parameters:
            sql_param = f"""SELECT code_specific_inputs 
                FROM dataset_code_specific_parameters 
                WHERE dataset_id = '{family}';
                """
            params = self.general_query(sql_param)
            params = params[0]['code_specific_inputs']
            code = parameters.get('code', None)
            if params != code:
                raise ValueError(
                    f"The provided parameters['code'] does not match what is "
                    f"stored in the database. All data being uploaded must have "
                    f"matching input parameters."
                )
        # convert to CF AtomicConfiguration if not already
        converted_configs = []
        for c in configs:
            if isinstance(c, Atoms):
                converted_configs.append(AtomicConfiguration.from_ase(c))
            elif isinstance(c, AtomicConfiguration):
                converted_configs.append(c)
            else:
                raise Exception("Configs must be an instance of either ase.Atoms or AtomicConfiguration")
        # update dataset_id
        q = f"SELECT id FROM datasets where id LIKE '%{family}%'"
        res = self.general_query(q)
        largest_version = sorted([r['id'].split('_')[-1] for r in res])[-1]
        new_v_no = int(largest_version) + 1
        new_dataset_id = dataset_id.split('_')[0] + '_' + dataset_id.split('_')[1] + '_' + str(new_v_no)
        
        self.load_data_to_pg_in_batches_no_spark(converted_configs, new_dataset_id, prop_map=prop_map, strict=strict)

        #config_df_1 = self.dataset_query_pg(dataset_id, 'configurations')
        #prop_df_1 = self.dataset_query_pg(dataset_id, 'property_objects')
        
        config_df_2 = self.dataset_query_pg(new_dataset_id, 'configurations')
        prop_df_2 = self.dataset_query_pg(new_dataset_id, 'property_objects')

        #config_df_1.extend(config_df_2)
        #prop_df_1.extend(prop_df_2)
        
        old_ds = self.get_dataset_pg(dataset_id)[0]

        # format links
        s = old_ds['links'][0].split(' ')[-1].replace("'","")
        d = old_ds['links'][1].split(' ')[-1].replace("'","")
        o = old_ds['links'][2].split(' ')[-1].replace("'","")


        ds = Dataset(
            name=old_ds['name'],
            authors=old_ds['authors'],
            config_df=config_df_2,
            prop_df=prop_df_2,
            publication_link=s,
            data_link=d,
            description=old_ds['description'],
            other_links=o,
            dataset_id=new_dataset_id,
            labels=old_ds['labels'],
            doi=old_ds['doi'],
            data_license=old_ds['license'],
            # TODO handle cs later
            configuration_set_ids=None,
            publication_year=old_ds['publication_year'],
            use_pg = True,
        )
        row = ds.spark_row

        sql = """
            INSERT INTO datasets (last_modified, nconfigurations, nproperty_objects, nsites, nelements, elements, total_elements_ratio, nperiodic_dimensions, dimension_types, energy_mean, energy_variance, atomic_forces_count, cauchy_stress_count, energy_count, authors, description, license, links, name, publication_year, doi, id, extended_id, hash, labels, property_map, uploader)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s)
            ON CONFLICT (hash)
            DO NOTHING
        """

        column_headers = tuple(row.keys())
        values = []
        t = []
        for column in column_headers:
            if column in ['nconfiguration_sets']:
                pass
            else:
                val = row[column]
                if column == 'last_modified':
                    val = val.strftime("%Y-%m-%dT%H:%M:%SZ")
                values.append(val)
        values.append(json.dumps(prop_map))
        values.append(os.getlogin())
       
        with psycopg.connect(dbname=self.dbname, user=self.user, port=self.port, host=self.host, password=self.password) as conn:
            with conn.cursor() as curs:
                curs.executemany(sql, [values])
                return new_dataset_id

    def get_dataset_data(self, dataset_id):
        sql=f"""
        SELECT
            c.*,  
            po.* 
        FROM
            (SELECT * FROM configurations WHERE '{dataset_id}' = ANY(dataset_ids)) c
        INNER JOIN
            (SELECT * FROM property_objects WHERE dataset_id = '{dataset_id}') po
        ON
            c.id = po.configuration_id
        ORDER by configuration_id;
        """
        with psycopg.connect(dbname=self.dbname, user=self.user, port=self.port, host=self.host, password=self.password, row_factory=dict_row) as conn:
            with conn.cursor() as curs:
                curs.execute(sql)
                table = curs.fetchall()
                return table

    def general_query(self, sql):
        with psycopg.connect(dbname=self.dbname, user=self.user, port=self.port, host=self.host, password=self.password, row_factory=dict_row) as conn:
            with conn.cursor() as curs:
                curs.execute(sql)
                try:
                    return curs.fetchall()
                except:
                    return

    def dataset_query_pg(self,
        dataset_id=None,
        table_name=None,
    ):      
        if table_name == 'configurations':
            sql = f"""
                SELECT *
                FROM {table_name}
                WHERE '{dataset_id}' = ANY(dataset_ids);
            """
        elif table_name == 'property_objects':
            sql = f"""
                SELECT *
                FROM {table_name}
                WHERE dataset_id = '{dataset_id}';
            """
        else:
            raise Exception('Only configurations and property_objects tables are supported')
            
        with psycopg.connect(dbname=self.dbname, user=self.user, port=self.port, host=self.host, password=self.password, row_factory=dict_row) as conn:
            with conn.cursor() as curs:
                r = curs.execute(sql)
                return curs.fetchall()
            
    def get_dataset_pg(self, dataset_id):
        sql = f"""
                SELECT *
                FROM datasets
                WHERE id = '{dataset_id}';
            """ 
        print (dataset_id)    
        with psycopg.connect(dbname=self.dbname, user=self.user, port=self.port, host=self.host, password=self.password, row_factory=dict_row) as conn:
            with conn.cursor() as curs:
                r = curs.execute(sql)
                return curs.fetchall()

    def create_configuration_sets(
        self,
        loader,
        name_label_match: list[tuple],
    ):
        """
        Args for name_label_match in order:
        1. String pattern for matching CONFIGURATION NAMES
        2. String pattern for matching CONFIGURATION LABELS
        3. Name for configuration set
        4. Description for configuration set
        """
        dataset_id = self.dataset_id
        config_set_rows = []
        # config_df = loader.read_table(table_name=loader.config_table, unstring=True)
        # config_df = config_df.filter(
        #     sf.array_contains(sf.col("dataset_ids"), self.dataset_id)
        # )
        # .cache()
        # prop_df = loader.read_table(loader.prop_object_table, unstring=True)
        # prop_df = prop_df.filter(sf.col("dataset_id") == self.dataset_id)
        # .cache()
        for i, (names_match, label_match, cs_name, cs_desc) in tqdm(
            enumerate(name_label_match), desc="Creating Configuration Sets"
        ):
            print(
                f"names match: {names_match}, label: {label_match}, "
                f"cs_name: {cs_name}, cs_desc: {cs_desc}"
            )
            if names_match and not label_match:
                # config_set_query = config_df.withColumn(
                #     "names_exploded", sf.explode(sf.col("names"))
                # ).filter(sf.col("names_exploded").rlike(names_match))
                config_set_query_df = loader.config_set_query(
                    query_table=loader.config_table,
                    dataset_id=dataset_id,
                    name_match=names_match,
                )
            # Currently an AND operation on labels: labels col contains x AND y
            if label_match and not names_match:
                # if isinstance(label_match, str):
                #     label_match = [label_match]
                # for label in label_match:
                #     config_set_query = config_set_query.filter(
                #         sf.array_contains(sf.col("labels"), label)
                #     )
                config_set_query_df = loader.config_set_query(
                    query_table=loader.config_table,
                    dataset_id=dataset_id,
                    label_match=label_match,
                )
            if names_match and label_match:
                config_set_query_df = loader.config_set_query(
                    query_table=loader.config_table,
                    dataset_id=dataset_id,
                    name_match=names_match,
                    label_match=label_match,
                )
            co_id_df = (
                config_set_query_df.select("id")
                .distinct()
                .withColumnRenamed("id", "configuration_id")
            )
            # prop_df_cs = loader.config_set_query(
            #     query_table=loader.prop_object_table,
            #     dataset_id=dataset_id,
            #     configuration_ids=co_ids,
            # )
            # prop_df_cs = prop_df_cs.select(
            #     "configuration_id", "multiplicity"
            # ).withColumnRenamed("configuration_id", "id")
            # config_set_query_df = config_set_query_df.join(
            #     prop_df_cs, on="id", how="inner"
            # )
            string_cols = [
                "elements",
            ]
            unstring_col_udf = sf.udf(unstring_df_val, ArrayType(StringType()))
            for col in string_cols:
                config_set_query_df = config_set_query_df.withColumn(
                    col, unstring_col_udf(sf.col(col))
                )
            unstring_col_udf = sf.udf(unstring_df_val, ArrayType(IntegerType()))
            int_cols = [
                "atomic_numbers",
                "dimension_types",
            ]
            for col in int_cols:
                config_set_query_df = config_set_query_df.withColumn(
                    col, unstring_col_udf(sf.col(col))
                )
            t = time()
            prelim_cs_id = f"CS_{cs_name}_{self.dataset_id}"
            co_cs_df = loader.get_co_cs_mapping(prelim_cs_id)
            if co_cs_df is not None:
                print(
                    f"Configuration Set {cs_name} already exists.\nRemove rows matching "  # noqa E501
                    f"'configuration_set_id == {prelim_cs_id} from table {loader.co_cs_map_table} to recreate.\n"  # noqa E501
                )
                continue
            config_set = ConfigurationSet(
                name=cs_name,
                description=cs_desc,
                config_df=config_set_query_df,
                dataset_id=self.dataset_id,
            )
            co_cs_df = co_id_df.withColumn("configuration_set_id", sf.lit(config_set.id))
            loader.write_table(co_cs_df, loader.co_cs_map_table, check_unique=False)
            loader.update_existing_co_rows(
                co_df=config_set_query_df,
                cols=["configuration_set_ids"],
                elems=config_set.id,
            )
            t_end = time() - t
            print(f"Time to create CS and update COs with CS-ID: {t_end}")

            config_set_rows.append(config_set.spark_row)
        config_set_df = loader.spark.createDataFrame(
            config_set_rows, schema=configuration_set_df_schema
        )
        loader.write_table(config_set_df, loader.config_set_table)
        return config_set_rows

    def create_dataset(
        self,
        loader,
        name: str,
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
        if loader.spark.catalog.tableExists(loader.config_set_table):
            cs_ids = (
                loader.dataset_query(
                    dataset_id=self.dataset_id, table_name=loader.config_set_table
                )
                .select("id")
                .collect()
            )
            # cs_ids = (
            #     loader.read_table(loader.config_set_table)
            #     .filter(sf.col("dataset_id") == self.dataset_id)
            #     .select("id")
            #     .collect()
            # )
            if len(cs_ids) == 0:
                cs_ids = None
            else:
                cs_ids = [x["id"] for x in cs_ids]
        else:
            cs_ids = None
        config_df = loader.dataset_query(
            dataset_id=self.dataset_id, table_name=loader.config_table
        )
        # config_df = loader.read_table(loader.config_table, unstring=True)
        # config_df = config_df.filter(
        #     sf.array_contains(sf.col("dataset_ids"), self.dataset_id)
        # )
        prop_df = loader.dataset_query(
            dataset_id=self.dataset_id, table_name=loader.prop_object_table
        )
        # prop_df = loader.read_table(loader.prop_object_table, unstring=True)
        # prop_df = prop_df.filter(sf.col("dataset_id") == self.dataset_id)
        ds = Dataset(
            name=name,
            authors=authors,
            config_df=config_df,
            prop_df=prop_df,
            publication_link=publication_link,
            data_link=data_link,
            description=description,
            other_links=other_links,
            dataset_id=self.dataset_id,
            labels=labels,
            doi=doi,
            data_license=data_license,
            configuration_set_ids=cs_ids,
            publication_year=publication_year,
        )
        ds_df = loader.spark.createDataFrame([ds.spark_row], schema=dataset_df_schema)
        loader.write_table(ds_df, loader.dataset_table)
    
    def delete_dataset(self, dataset_id, delete_children=False):
        # check if user matches original uploader
        sql = """
            SELECT uploader
            FROM datasets
            WHERE id = %s;
        """
        with psycopg.connect(dbname=self.dbname, user=self.user, port=self.port, host=self.host, password=self.password) as conn:
            with conn.cursor() as curs:
                curs.execute(sql, (dataset_id,))
                uploader = curs.fetchall()[0][0]

        if uploader == os.getlogin():
            print (f'Deleting {dataset_id}')
            sql = """
                DELETE
                FROM datasets
                WHERE id = %s;
            """
            # TODO: delete children as well
            with psycopg.connect(dbname=self.dbname, user=self.user, port=self.port, host=self.host, password=self.password) as conn:
                with conn.cursor() as curs:
                    curs.execute(sql, (dataset_id,))
            if delete_children:
                sql1 = '''
                    DELETE FROM property_objects
                    WHERE dataset_id = %s;
                    '''
                sql2 = '''
                    DELETE FROM configurations
                    WHERE dataset_ids = ARRAY[%s]::varchar[];
                    '''
                sql3 = '''
                    UPDATE configurations
                    SET dataset_ids = array_remove(dataset_ids, %s)
                    WHERE dataset_ids @> ARRAY[%s]::varchar[]  
                    AND cardinality(dataset_ids) > 1;
                '''
                with psycopg.connect(dbname=self.dbname, user=self.user, port=self.port, host=self.host, password=self.password) as conn:
                    with conn.cursor() as curs:
                        curs.execute(sql1, (dataset_id,))
                        curs.execute(sql2, (dataset_id,))
                        curs.execute(sql3, (dataset_id, dataset_id))
        else:
            raise Exception(f'Cannot delete dataset. User must match the original uploader, {uploader}.')


class S3BatchManager:
    def __init__(self, bucket_name, access_id, secret_key, endpoint_url=None):
        self.bucket_name = bucket_name
        self.access_id = access_id
        self.secret_key = secret_key
        self.endpoint_url = endpoint_url
        self.client = self.get_client()
        self.MAX_BATCH_SIZE = 100

    def get_client(self):
        return boto3.client(
            "s3",
            use_ssl=False,
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_id,
            aws_secret_access_key=self.secret_key,
            region_name="fake-region",
            config=boto3.session.Config(
                signature_version="s3v4", s3={"addressing_style": "path"}
            ),
        )

    def batch_write(self, file_batch):
        results = []
        for key, content in file_batch:
            try:
                self.client.put_object(Bucket=self.bucket_name, Key=key, Body=content)
                results.append((key, None))
            except Exception as e:
                results.append((key, str(e)))
        return results


def write_md_partition(partition, config):
    s3_mgr = S3BatchManager(
        bucket_name=config["bucket_dir"],
        access_id=config["access_key"],
        secret_key=config["access_secret"],
        endpoint_url=config["endpoint"],
    )
    file_batch = []
    for row in partition:
        md_path = Path(config["metadata_dir"]) / row["metadata_path"]
        file_batch.append((str(md_path), row["metadata"]))

        if len(file_batch) >= s3_mgr.MAX_BATCH_SIZE:
            _ = s3_mgr.batch_write(file_batch)
            file_batch = []
    if file_batch:
        _ = s3_mgr.batch_write(file_batch)
    return iter([])


class S3FileManager:
    def __init__(self, bucket_name, access_id, secret_key, endpoint_url=None):
        self.bucket_name = bucket_name
        self.access_id = access_id
        self.secret_key = secret_key
        self.endpoint_url = endpoint_url

    def get_client(self):
        return boto3.client(
            "s3",
            use_ssl=False,
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_id,
            aws_secret_access_key=self.secret_key,
            region_name="fake-region",
            config=boto3.session.Config(
                signature_version="s3v4", s3={"addressing_style": "path"}
            ),
        )

    def write_file(self, content, file_key):
        try:
            client = self.get_client()
            client.put_object(Bucket=self.bucket_name, Key=file_key, Body=content)
            # return (f"/vdev/{self.bucket_name}/{file_key}", sys.getsizeof(content))
        except Exception as e:
            return f"Error: {str(e)}"

    def read_file(self, file_key):
        try:
            client = self.get_client()
            # key = file_key.replace(str(Path("/vdev/colabfit-data")) + "/", "")
            response = client.get_object(Bucket=self.bucket_name, Key=file_key)
            return response["Body"].read().decode("utf-8")
        except Exception as e:
            return f"Error: {str(e)}"


def generate_ds_id():
    # Maybe check to see whether the DS ID already exists?
    ds_id = ID_FORMAT_STRING.format("DS", generate_string(), 0)
    #print("Generated new DS ID:", ds_id)
    return ds_id

'''
@sf.udf(returnType=StringType())
def prepend_path_udf(prefix, md_path):
    try:
        full_path = Path(prefix) / Path(md_path).relative_to("/")
        return str(full_path)
    except ValueError:
        full_path = Path(prefix) / md_path
        return str(full_path)
'''

# def write_md_partition(partition, config):
#     s3_mgr = S3FileManager(
#         bucket_name=config["bucket_dir"],
#         access_id=config["access_key"],
#         secret_key=config["access_secret"],
#         endpoint_url=config["endpoint"],
#     )
#     for row in partition:
#         md_path = Path(config["metadata_dir"]) / row["metadata_path"]
#         if not md_path.exists():
#             s3_mgr.write_file(
#                 row["metadata"],
#                 str(md_path),
#             )
#     return iter([])


def read_md_partition(partition, config):
    s3_mgr = S3FileManager(
        bucket_name=config["bucket_dir"],
        access_id=config["access_key"],
        secret_key=config["access_secret"],
        endpoint_url=config["endpoint"],
    )

    def process_row(row):
        rowdict = row.asDict()
        try:
            rowdict["metadata"] = s3_mgr.read_file(row["metadata_path"])
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                rowdict["metadata"] = None
            else:
                print(f"Error reading {row['metadata_path']}: {str(e)}")
                rowdict["metadata"] = None
        return Row(**rowdict)

    return map(process_row, partition)

'''
def dataset_query_pg(
    dataset_id=None,
    table_name=None,
):
    if table_name == 'configurations':
        sql = f"""
            SELECT *
            FROM {table_name}
            WHERE '{dataset_id}' = ANY(dataset_ids);
        """
    elif table_name == 'property_objects':
        sql = f"""
            SELECT *
            FROM {table_name}
            WHERE dataset_id = '{dataset_id}';
        """
    else:
        raise Exception('Only configurations and property_objects tables are supported')

    with psycopg.connect(dbname=self.dbname, user=self.user, port=self.port, host=self.host, password=self.password,row_factory=dict_row) as conn:
        with conn.cursor() as curs:
            r = curs.execute(sql)
            return curs.fetchall()

def get_dataset_pg(dataset_id):
    sql = f"""
            SELECT *
            FROM datasets
            WHERE id = '{dataset_id}';
        """

    with psycopg.connect(dbname=self.dbname, user=self.user, port=self.port, host=self.host, password=self.password,row_factory=dict_row) as conn:
        with conn.cursor() as curs:
            r = curs.execute(sql)
            return curs.fetchall()
'''
