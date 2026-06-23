"""Tests for Property helpers in colabfit.tools.pg.property (Phase 3)."""
import pytest

from colabfit.tools.pg.property import (
    PropertyInfo,
    check_split_units,
    energy_to_schema,
    property_info,
)


def test_check_split_units_valid():
    assert check_split_units("eV")
    assert check_split_units("eV/angstrom")
    assert check_split_units("eV/angstrom^3")


def test_check_split_units_invalid():
    assert not check_split_units("notaunit")
    assert not check_split_units("eV/notaunit")


def test_property_info_valid_get_info():
    pi = PropertyInfo("atomization-energy", "atom_en", "eV", "atomization_energy")
    info = pi.get_info()
    assert isinstance(info, property_info)
    assert info.property_name == "atomization-energy"
    assert info.units == "eV"
    assert info.additional == []


def test_property_info_rejects_underscore_in_name():
    with pytest.raises(ValueError, match="underscore"):
        PropertyInfo("atomization_energy", "field", "eV", "key")


def test_property_info_rejects_invalid_units():
    with pytest.raises(ValueError, match="Invalid unit"):
        PropertyInfo("energy", "field", "notaunit", "key")


def test_property_info_requires_field():
    with pytest.raises(ValueError, match="Field is required"):
        PropertyInfo("energy", "", "eV", "key")


def test_energy_to_schema_generic_energy_above_hull():
    """Any energy-typed property routes through energy_to_schema generically."""
    en_prop = {
        "energy": {"source-value": 0.1, "source-unit": "eV"},
        "per-atom": {"source-value": False},
    }
    out = energy_to_schema("energy-above-hull", en_prop)
    assert out["energy_above_hull"] == 0.1
    assert out["energy_above_hull_unit"] == "eV"
    assert out["energy_above_hull_per_atom"] is False


def test_energy_to_schema_none_returns_empty():
    assert energy_to_schema("formation-energy", {"energy": None}) == {}
