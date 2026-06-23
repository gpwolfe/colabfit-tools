"""Tests for hash functions in colabfit.tools.vast.utils."""

from colabfit.tools.vast.utils import _hash, config_struct_hash


def test_hash_returns_hex_string():
    result = _hash({"a": 1, "b": 2}, ["a", "b"])
    assert isinstance(result, str)
    assert len(result) == 128
    assert all(c in "0123456789abcdef" for c in result)


def test_hash_consistent():
    row = {"x": [1.0, 2.0], "y": "hello", "z": 3}
    keys = ["x", "y", "z"]
    assert _hash(row, keys) == _hash(row, keys)


def test_hash_key_order_independent():
    row = {"a": 1, "b": 2}
    assert _hash(row, ["a", "b"]) == _hash(row, ["b", "a"])


def test_hash_skips_none_values():
    h_with_none = _hash({"a": 1, "b": None}, ["a", "b"])
    h_without = _hash({"a": 1}, ["a"])
    assert h_with_none == h_without


def test_config_struct_hash_returns_hex_string():
    result = config_struct_hash(
        atomic_numbers=[1, 1, 8],
        cell=[[5.0, 0, 0], [0, 5.0, 0], [0, 0, 5.0]],
        pbc=[True, True, True],
        positions=[[0.0, 0.0, 0.0], [1.0, 0.0, 0.0], [0.5, 0.5, 0.0]],
    )
    assert isinstance(result, str)
    assert len(result) == 128
    assert all(c in "0123456789abcdef" for c in result)


def test_config_struct_hash_consistent():
    kwargs = dict(
        atomic_numbers=[6, 6],
        cell=[[4.0, 0, 0], [0, 4.0, 0], [0, 0, 4.0]],
        pbc=[True, True, True],
        positions=[[0.0, 0.0, 0.0], [2.0, 2.0, 2.0]],
    )
    assert config_struct_hash(**kwargs) == config_struct_hash(**kwargs)


def test_config_struct_hash_independent_of_atom_order():
    cell = [[5.0, 0, 0], [0, 5.0, 0], [0, 0, 5.0]]
    pbc = [True, True, True]
    h1 = config_struct_hash([6, 1], cell, pbc, [[0.0, 0.0, 0.0], [1.0, 0.0, 0.0]])
    h2 = config_struct_hash([1, 6], cell, pbc, [[1.0, 0.0, 0.0], [0.0, 0.0, 0.0]])
    assert h1 == h2
