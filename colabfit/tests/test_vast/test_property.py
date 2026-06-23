"""Tests for Property helpers in colabfit.tools.vast.property."""

import pytest

from colabfit.tools.vast.property import PropertyInfo, check_split_units


def test_check_split_units_valid():
    assert check_split_units("eV")
    assert check_split_units("eV/angstrom")
    assert check_split_units("eV/angstrom^3")


def test_check_split_units_invalid():
    assert not check_split_units("notaunit")
    assert not check_split_units("eV/notaunit")


def test_property_info_valid_string_units():
    pi = PropertyInfo("atomization-energy", "atom_en", "eV", "atomization_energy")
    info = pi.get_info()
    assert info.property_name == "atomization-energy"
    assert info.original_file_key == "atomization_energy"
    assert info.additional == []


def test_property_info_rejects_underscore_in_name():
    with pytest.raises(ValueError, match="underscore"):
        PropertyInfo("atomization_energy", "field", "eV", "key")


def test_property_info_rejects_invalid_units():
    with pytest.raises(ValueError, match="[Ii]nvalid unit"):
        PropertyInfo("energy", "field", "notaunit", "key")


def test_property_info_requires_field():
    with pytest.raises(ValueError, match="[Ff]ield"):
        PropertyInfo("energy", "", "eV", "key")


def test_property_info_requires_original_file_key():
    with pytest.raises(ValueError):
        PropertyInfo("energy", "field", "eV", "")


def test_property_info_units_stored_as_nested_dict():
    pi = PropertyInfo("energy", "energy_field", "eV", "energy_key")
    assert isinstance(pi.units, dict)
    assert "source-unit" in pi.units


def test_property_info_dict_units_accepted():
    pi = PropertyInfo("energy", "energy_field", {"value": "eV"}, "energy_key")
    assert isinstance(pi.units, dict)


def test_property_info_additional_stored():
    pi = PropertyInfo(
        "energy", "field", "eV", "key", additional=[("per-atom", {"value": False})]
    )
    info = pi.get_info()
    assert len(info.additional) == 1
