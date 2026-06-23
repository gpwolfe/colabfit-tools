"""Tests for ConfigurationSet in colabfit.tools.pg.configuration_set."""
from colabfit.tools.pg.configuration_set import ConfigurationSet
from colabfit.tools.pg.schema import configuration_set_schema


def _make_mock_configs():
    return [
        {
            "id": "CO_abc123",
            "nsites": 2,
            "elements": ["C", "H"],
            "atomic_numbers": [6, 1],
            "nperiodic_dimensions": 0,
            "dimension_types": [0, 0, 0],
        },
        {
            "id": "CO_def456",
            "nsites": 3,
            "elements": ["C", "H"],
            "atomic_numbers": [6, 1, 1],
            "nperiodic_dimensions": 0,
            "dimension_types": [0, 0, 0],
        },
    ]


def test_construction():
    configs = _make_mock_configs()
    cs = ConfigurationSet(
        config_df=configs,
        name="test_cs",
        description="Test configuration set",
        dataset_id="DS_testdataset_0",
    )
    assert cs.id == "CS_test_cs_DS_testdataset_0"
    assert cs.row_dict["nconfigurations"] == 2


def test_hash_is_hex_string():
    configs = _make_mock_configs()
    cs = ConfigurationSet(
        config_df=configs,
        name="test_cs",
        description="Test",
        dataset_id="DS_test_0",
    )
    assert isinstance(cs._hash, str)
    assert len(cs._hash) == 128
    assert all(c in "0123456789abcdef" for c in cs._hash)


def test_row_dict_hash_is_stored_correctly():
    configs = _make_mock_configs()
    cs = ConfigurationSet(
        config_df=configs,
        name="test_cs",
        description="Test",
        dataset_id="DS_test_0",
    )
    assert cs.row_dict["hash"] == cs._hash


def test_element_aggregation():
    configs = _make_mock_configs()
    cs = ConfigurationSet(
        config_df=configs,
        name="test_cs",
        description="Test",
        dataset_id="DS_test_0",
    )
    assert cs.row_dict["elements"] == ["C", "H"]
    assert cs.row_dict["nelements"] == 2


def test_nsites_sum():
    configs = _make_mock_configs()
    cs = ConfigurationSet(
        config_df=configs,
        name="test_cs",
        description="Test",
        dataset_id="DS_test_0",
    )
    assert cs.row_dict["nsites"] == 5


def test_total_elements_ratios():
    configs = _make_mock_configs()
    cs = ConfigurationSet(
        config_df=configs,
        name="test_cs",
        description="Test",
        dataset_id="DS_test_0",
    )
    ratios = cs.row_dict["total_elements_ratios"]
    # 2 C atoms (1 from first, 1 from second) and 3 H atoms out of 5 total
    assert abs(ratios[0] - 2 / 5) < 1e-9  # C ratio
    assert abs(ratios[1] - 3 / 5) < 1e-9  # H ratio


def test_row_dict_has_all_schema_keys():
    configs = _make_mock_configs()
    cs = ConfigurationSet(
        config_df=configs,
        name="test_cs",
        description="Test",
        dataset_id="DS_test_0",
    )
    schema_keys = set(configuration_set_schema.column_names)
    # extended_id and id/hash are set after to_row_dict, so check full row_dict
    row_keys = set(cs.row_dict.keys())
    missing = schema_keys - row_keys
    assert not missing, f"row_dict missing schema keys: {missing}"
