"""Tests for Dataset in colabfit.tools.vast.dataset."""

import pytest
import pyarrow as pa

from colabfit.tools.vast.dataset import Dataset


def _make_mock_batches(energy_above_hull=None):
    hull_vals = (
        [energy_above_hull, None] if energy_above_hull is not None else [None, None]
    )
    return [
        pa.table(
            {
                "property_id": pa.array(["PO_001", "PO_002"], pa.string()),
                "configuration_id": pa.array(["CO_001", "CO_002"], pa.string()),
                "nsites": pa.array([2, 3], pa.int32()),
                "nperiodic_dimensions": pa.array([0, 0], pa.int32()),
                "dimension_types": pa.array(
                    [[0, 0, 0], [0, 0, 0]], pa.list_(pa.int32())
                ),
                "elements": pa.array([["C", "H"], ["C", "H"]], pa.list_(pa.string())),
                "atomic_numbers": pa.array([[6, 1], [6, 1, 1]], pa.list_(pa.int32())),
                "method": pa.array(["DFT", "DFT"], pa.string()),
                "software": pa.array(["VASP", "VASP"], pa.string()),
                "energy": pa.array([-1.5, -2.0], pa.float64()),
                "atomic_forces": pa.array(
                    [None, None], pa.list_(pa.list_(pa.float64()))
                ),
                "atomization_energy": pa.array([None, None], pa.float64()),
                "adsorption_energy": pa.array([None, None], pa.float64()),
                "electronic_band_gap": pa.array([None, None], pa.float64()),
                "cauchy_stress": pa.array(
                    [None, None], pa.list_(pa.list_(pa.float64()))
                ),
                "energy_above_hull": pa.array(hull_vals, pa.float64()),
                "formation_energy": pa.array([None, None], pa.float64()),
            }
        )
    ]


def _make_dataset(**kwargs):
    defaults = dict(
        name="TestDataset",
        authors=["Smith", "Jones"],
        publication_link="https://doi.org/example",
        data_link="https://github.com/example",
        description="A test dataset",
        config_batches=_make_mock_batches(),
        dataset_id="DS_teststring12_0",
        publication_year="2023",
        date_requested="2023-01-15",
        data_license="CC-BY-4.0",
    )
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


def test_energy_count():
    ds = _make_dataset()
    assert ds.row_dict["energy_count"] == 2
    assert ds.row_dict["atomic_forces_count"] == 0
    assert ds.row_dict["cauchy_stress_count"] == 0


def test_methods_software_aggregation():
    ds = _make_dataset()
    assert ds.row_dict["methods"] == ["DFT"]
    assert ds.row_dict["software"] == ["VASP"]


def test_total_elements_ratios_present():
    ds = _make_dataset()
    assert "total_elements_ratios" in ds.row_dict
    ratios = ds.row_dict["total_elements_ratios"]
    assert abs(sum(ratios) - 1.0) < 1e-9


def test_energy_above_hull_count():
    ds = _make_dataset(config_batches=_make_mock_batches(energy_above_hull=0.05))
    assert ds.row_dict["energy_above_hull_count"] == 1


def test_energy_mean_and_variance():
    ds = _make_dataset()
    assert ds.row_dict["energy_mean"] is not None
    assert ds.row_dict["energy_variance"] is not None


def test_missing_dataset_id_raises():
    with pytest.raises(RuntimeError):
        _make_dataset(dataset_id=None)


def test_missing_publication_year_raises():
    with pytest.raises(RuntimeError):
        _make_dataset(publication_year=None)


def test_missing_date_requested_raises():
    with pytest.raises(RuntimeError):
        _make_dataset(date_requested=None)


def test_missing_data_license_raises():
    with pytest.raises(RuntimeError):
        _make_dataset(data_license=None)


def test_bad_publication_year_raises():
    with pytest.raises(Exception):
        _make_dataset(publication_year="23")


def test_bad_date_requested_raises():
    with pytest.raises(Exception):
        _make_dataset(date_requested="Jan 15 2023")


def test_bad_author_name_raises():
    with pytest.raises(RuntimeError):
        _make_dataset(authors=["Smith123"])


def test_equilibrium_flag():
    ds = _make_dataset(equilibrium=True)
    assert ds.row_dict["equilibrium"] is True


def test_extended_id_contains_dataset_id():
    ds = _make_dataset()
    assert "DS_teststring12_0" in ds.row_dict["extended_id"]
