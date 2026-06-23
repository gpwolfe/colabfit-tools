"""Tests for AtomicConfiguration in colabfit.tools.pg.configuration."""
import pytest
from ase import Atoms

from colabfit import ATOMS_NAME_FIELD
from colabfit.tools.pg.configuration import AtomicConfiguration
from colabfit.tools.pg.schema import config_schema


def _make_config(name="test_config", positions=None, numbers=None):
    if positions is None:
        positions = [[0.0, 0.0, 0.0], [1.5, 0.0, 0.0]]
    if numbers is None:
        numbers = [6, 6]
    return AtomicConfiguration(
        info={ATOMS_NAME_FIELD: name},
        positions=positions,
        numbers=numbers,
        cell=[5.0, 5.0, 5.0],
        pbc=False,
    )


def test_construction():
    co = _make_config()
    assert co.id.startswith("CO_")
    assert len(co.id) == 28  # "CO_" + 25 hex chars


def test_hash_is_hex_string():
    co = _make_config()
    assert isinstance(co._hash, str)
    assert len(co._hash) == 128
    assert all(c in "0123456789abcdef" for c in co._hash)


def test_row_dict_has_all_schema_keys():
    co = _make_config()
    schema_keys = set(config_schema.column_names)
    row_keys = set(co.row_dict.keys())
    missing = schema_keys - row_keys
    assert not missing, f"row_dict missing schema keys: {missing}"


def test_row_dict_hash_is_hex_string():
    co = _make_config()
    assert isinstance(co.row_dict["hash"], str)
    assert len(co.row_dict["hash"]) == 128


def test_structure_hash_present():
    co = _make_config()
    assert co.row_dict["structure_hash"] is not None
    assert len(co.row_dict["structure_hash"]) == 128


def test_from_ase():
    atoms = Atoms(
        symbols="CH4",
        positions=[[0, 0, 0], [1, 0, 0], [-1, 0, 0], [0, 1, 0], [0, -1, 0]],
        cell=[5, 5, 5],
    )
    atoms.info[ATOMS_NAME_FIELD] = "methane"
    co = AtomicConfiguration.from_ase(atoms)
    assert co.id.startswith("CO_")
    assert co.row_dict["chemical_formula_hill"] == "CH4"


def test_missing_atoms_name_field_raises():
    with pytest.raises(ValueError, match=ATOMS_NAME_FIELD):
        AtomicConfiguration(
            info={"some_key": "value"},
            positions=[[0.0, 0.0, 0.0]],
            numbers=[6],
            cell=[5.0, 5.0, 5.0],
        )


def test_info_not_dict_raises():
    with pytest.raises(TypeError, match="dict"):
        AtomicConfiguration(
            info="not_a_dict",
            positions=[[0.0, 0.0, 0.0]],
            numbers=[6],
            cell=[5.0, 5.0, 5.0],
        )


def test_deterministic_hash():
    co1 = _make_config()
    co2 = _make_config()
    assert co1._hash == co2._hash


def test_different_structures_different_hash():
    co1 = _make_config(positions=[[0, 0, 0], [1.5, 0, 0]])
    co2 = _make_config(positions=[[0, 0, 0], [2.0, 0, 0]])
    assert co1._hash != co2._hash


def test_aggregate_configuration_summaries_raises():
    from colabfit.tools.pg.configuration import AtomicConfiguration

    with pytest.raises(NotImplementedError):
        AtomicConfiguration.aggregate_configuration_summaries(None, [])
