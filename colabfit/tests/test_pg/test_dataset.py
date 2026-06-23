"""Tests for Dataset in colabfit.tools.pg.dataset."""
import pytest

from colabfit.tools.pg.dataset import Dataset


def _make_mock_configs():
    return [
        {
            "nsites": 2,
            "atomic_numbers": [6, 1],
            "nperiodic_dimensions": 0,
            "dimension_types": [0, 0, 0],
        },
        {
            "nsites": 3,
            "atomic_numbers": [6, 1, 1],
            "nperiodic_dimensions": 0,
            "dimension_types": [0, 0, 0],
        },
    ]


def _make_mock_props():
    return [
        {
            "energy": -1.5,
            "atomic_forces": None,
            "cauchy_stress": None,
            "energy_above_hull": None,
            "atomization_energy": None,
            "adsorption_energy": None,
            "formation_energy": None,
            "electronic_band_gap": None,
            "method": "DFT",
            "software": "VASP",
        },
        {
            "energy": -2.0,
            "atomic_forces": [[0.1, 0.2, 0.3], [0.4, 0.5, 0.6]],
            "cauchy_stress": None,
            "energy_above_hull": None,
            "atomization_energy": None,
            "adsorption_energy": None,
            "formation_energy": None,
            "electronic_band_gap": None,
            "method": "DFT",
            "software": "VASP",
        },
    ]


def _make_dataset(**kwargs):
    defaults = {
        "name": "TestDataset",
        "authors": ["Smith", "Jones"],
        "publication_link": "https://doi.org/example",
        "data_link": "https://github.com/example",
        "description": "A test dataset",
        "config_df": _make_mock_configs(),
        "prop_df": _make_mock_props(),
        "dataset_id": "DS_teststring12_0",
        "publication_year": "2023",
        "date_requested": "2023-01-15",
    }
    defaults.update(kwargs)
    return Dataset(**defaults)


def test_construction():
    ds = _make_dataset()
    assert ds.row_dict["name"] == "TestDataset"
    assert ds.row_dict["nconfigurations"] == 2
    assert ds.row_dict["nproperty_objects"] == 2


def test_hash_is_hex_string():
    ds = _make_dataset()
    assert isinstance(ds._hash, str)
    assert len(ds._hash) == 128
    assert all(c in "0123456789abcdef" for c in ds._hash)


def test_row_dict_hash_stored_correctly():
    ds = _make_dataset()
    assert ds.row_dict["hash"] == ds._hash


def test_correct_key_names_energy():
    ds = _make_dataset()
    assert ds.row_dict["energy_count"] == 2
    assert ds.row_dict["atomic_forces_count"] == 1
    assert ds.row_dict["cauchy_stress_count"] == 0


def test_total_elements_ratios_present():
    ds = _make_dataset()
    assert "total_elements_ratios" in ds.row_dict


def test_methods_software_aggregation():
    ds = _make_dataset()
    assert ds.row_dict["methods"] == ["DFT"]
    assert ds.row_dict["software"] == ["VASP"]


def test_authors_must_be_list():
    with pytest.raises(TypeError, match="list"):
        _make_dataset(authors="Smith")


def test_publication_year_validation():
    with pytest.raises(ValueError, match="4-digit"):
        _make_dataset(publication_year="23")


def test_str_no_key_error():
    ds = _make_dataset()
    result = str(ds)
    assert "Dataset" in result


def test_energy_above_hull_count():
    props = _make_mock_props()
    props[0]["energy_above_hull"] = 0.1
    ds = _make_dataset(prop_df=props)
    assert ds.row_dict["energy_above_hull_count"] == 1
