"""Tests verifying PyArrow schema column alignment for the vast backend."""

import pyarrow as pa

from colabfit.tools.vast.schema import (
    config_schema,
    configuration_set_schema,
    dataset_schema,
    property_object_schema,
)

_ALL_SCHEMAS = [
    config_schema,
    property_object_schema,
    dataset_schema,
    configuration_set_schema,
]


def test_config_schema_has_structure_hash():
    assert "structure_hash" in config_schema.names


def test_config_schema_geometry_columns():
    for col in ["positions", "cell", "pbc", "atomic_numbers", "elements"]:
        assert col in config_schema.names, f"config_schema missing column: {col}"


def test_property_object_schema_columns():
    names = property_object_schema.names
    for col in [
        "energy_above_hull",
        "max_force_norm",
        "mean_force_norm",
        "software",
        "method",
        "metadata",
    ]:
        assert col in names, f"property_object_schema missing column: {col}"


def test_dataset_schema_columns():
    names = dataset_schema.names
    for col in [
        "energy_above_hull_count",
        "equilibrium",
        "methods",
        "software",
        "date_added_to_colabfit",
    ]:
        assert col in names, f"dataset_schema missing column: {col}"


def test_configuration_set_schema_columns():
    names = configuration_set_schema.names
    for col in [
        "id",
        "hash",
        "nconfigurations",
        "elements",
        "nelements",
        "nsites",
        "total_elements_ratios",
    ]:
        assert col in names, f"configuration_set_schema missing column: {col}"


def test_all_schemas_are_pyarrow():
    for schema in _ALL_SCHEMAS:
        assert isinstance(schema, pa.Schema)


def test_all_field_names_are_strings():
    for schema in _ALL_SCHEMAS:
        for name in schema.names:
            assert isinstance(name, str), f"field name is not str: {name!r}"


def test_field_count_matches_names():
    for schema in _ALL_SCHEMAS:
        assert len(schema.names) == len(schema)
