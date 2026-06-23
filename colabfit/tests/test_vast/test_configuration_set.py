"""Tests for ConfigurationSet in colabfit.tools.vast.configuration_set."""

import pyarrow as pa
import pytest

from colabfit.tools.vast.configuration_set import ConfigurationSet, ConfigurationSetInfo
from colabfit.tools.vast.schema import configuration_set_schema


def _make_mock_batches():
    """Two rows: nsites=[2,3], atomic_numbers=[[6,1],[6,1,1]] so totals match."""
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
            }
        )
    ]


def _make_cs(**kwargs):
    defaults = dict(
        config_batches=_make_mock_batches(),
        name="test_cs",
        description="Test CS",
        dataset_id="DS_teststring12_0",
    )
    defaults.update(kwargs)
    return ConfigurationSet(**defaults)


def test_construction():
    cs = _make_cs()
    assert cs.id == "CS_test_cs_DS_teststring12_0"
    assert cs.row_dict["nconfigurations"] == 2


def test_hash_is_hex_string():
    cs = _make_cs()
    assert isinstance(cs._hash, str)
    assert len(cs._hash) == 128
    assert all(c in "0123456789abcdef" for c in cs._hash)


def test_row_dict_hash_stored_correctly():
    cs = _make_cs()
    assert cs.row_dict["hash"] == cs._hash


def test_row_dict_has_all_schema_keys():
    cs = _make_cs()
    missing = set(configuration_set_schema.names) - set(cs.row_dict.keys())
    assert not missing, f"row_dict missing schema keys: {missing}"


def test_element_aggregation():
    cs = _make_cs()
    assert cs.row_dict["elements"] == ["C", "H"]
    assert cs.row_dict["nelements"] == 2


def test_nsites_sum():
    cs = _make_cs()
    assert cs.row_dict["nsites"] == 5


def test_total_elements_ratios():
    cs = _make_cs()
    ratios = cs.row_dict["total_elements_ratios"]
    # 2 C atoms, 3 H atoms out of 5 total
    assert abs(ratios[0] - 2 / 5) < 1e-9  # C
    assert abs(ratios[1] - 3 / 5) < 1e-9  # H


def test_configuration_ids_populated():
    cs = _make_cs()
    assert sorted(cs.configuration_ids) == ["CO_001", "CO_002"]


def test_ordered_flag():
    cs = _make_cs(ordered=True)
    assert cs.row_dict["ordered"] is True


def test_duplicate_property_ids_deduplicated():
    """Duplicate property_id rows should only be counted once."""
    batch = pa.table(
        {
            "property_id": pa.array(["PO_001", "PO_001"], pa.string()),
            "configuration_id": pa.array(["CO_001", "CO_001"], pa.string()),
            "nsites": pa.array([2, 2], pa.int32()),
            "nperiodic_dimensions": pa.array([0, 0], pa.int32()),
            "dimension_types": pa.array([[0, 0, 0], [0, 0, 0]], pa.list_(pa.int32())),
            "elements": pa.array([["C", "H"], ["C", "H"]], pa.list_(pa.string())),
            "atomic_numbers": pa.array([[6, 1], [6, 1]], pa.list_(pa.int32())),
        }
    )
    cs = ConfigurationSet(
        config_batches=[batch],
        name="dedup_cs",
        description="dedup test",
        dataset_id="DS_test_0",
    )
    assert cs.row_dict["nconfigurations"] == 1


# --- ConfigurationSetInfo ---


def test_cs_info_construction():
    csi = ConfigurationSetInfo(
        co_name_match="%bulk%", cs_name="bulk", cs_description="Bulk structures"
    )
    info = csi.get_info()
    assert info.co_name_match == "%bulk%"
    assert info.cs_name == "bulk"


def test_cs_info_requires_name_or_label():
    with pytest.raises(ValueError):
        ConfigurationSetInfo(cs_name="bulk", cs_description="no match provided")


def test_cs_info_label_match():
    csi = ConfigurationSetInfo(
        co_label_match="%surface%", cs_name="surface", cs_description="Surfaces"
    )
    info = csi.get_info()
    assert info.co_label_match == "%surface%"
    assert info.co_name_match is None
