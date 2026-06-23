"""Tests verifying schema column alignment."""

from colabfit.tools.pg.schema import (
    config_schema,
    configuration_set_schema,
    dataset_schema,
    property_definition_schema,
    property_object_schema,
)


def test_config_schema_has_structure_hash():
    assert "structure_hash" in config_schema.column_names


def test_config_schema_geometry_columns():
    cols = config_schema.column_names
    for expected in ["positions", "cell", "pbc", "atomic_numbers", "elements"]:
        assert expected in cols, f"config_schema missing expected column: {expected}"


def test_property_object_schema_columns():
    cols = property_object_schema.column_names
    assert "energy_above_hull" in cols
    assert "max_force_norm" in cols
    assert "mean_force_norm" in cols
    assert "software" in cols
    assert "method" in cols
    assert "metadata" in cols


def test_dataset_schema_columns():
    cols = dataset_schema.column_names
    assert "energy_above_hull_count" in cols
    assert "equilibrium" in cols
    assert "methods" in cols
    assert "software" in cols
    assert "date_added_to_colabfit" in cols


def test_column_names_are_strings():
    for schema in [
        config_schema,
        property_object_schema,
        dataset_schema,
        configuration_set_schema,
        property_definition_schema,
    ]:
        for name in schema.column_names:
            assert isinstance(
                name, str
            ), f"column_name in {schema.name} is not a str: {name!r}"


def test_column_names_len_matches_columns_len():
    for schema in [
        config_schema,
        property_object_schema,
        dataset_schema,
        configuration_set_schema,
        property_definition_schema,
    ]:
        assert len(schema.column_names) == len(schema.columns), (
            f"Mismatch in {schema.name}: "
            f"{len(schema.column_names)} names vs {len(schema.columns)} columns"
        )
