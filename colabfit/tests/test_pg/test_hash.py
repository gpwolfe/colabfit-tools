"""Tests for hash functions in colabfit.tools.pg.utilities."""

from colabfit.tools.pg.utilities import _hash, config_struct_hash


def test_hash_returns_hex_string():
    result = _hash({"a": 1, "b": 2}, ["a", "b"])
    assert isinstance(result, str), f"Expected str, got {type(result)}"
    assert len(result) == 128, f"Expected 128-char hex, got length {len(result)}"
    assert all(c in "0123456789abcdef" for c in result), "Not a valid hex string"


def test_hash_consistent():
    row = {"x": [1.0, 2.0], "y": "hello", "z": 3}
    keys = ["x", "y", "z"]
    h1 = _hash(row, keys)
    h2 = _hash(row, keys)
    assert h1 == h2, "Hash should be deterministic"


def test_hash_key_order_independent():
    row = {"a": 1, "b": 2}
    h1 = _hash(row, ["a", "b"])
    h2 = _hash(row, ["b", "a"])
    assert h1 == h2, "Hash should be order-independent (sorted key list)"


def test_hash_skips_none_values():
    row = {"a": 1, "b": None}
    h_with_none = _hash(row, ["a", "b"])
    row2 = {"a": 1}
    h_without = _hash(row2, ["a"])
    assert h_with_none == h_without, "None values should be skipped in hash"


def test_config_struct_hash_returns_hex_string():
    atomic_numbers = [1, 1, 8]
    cell = [[5.0, 0, 0], [0, 5.0, 0], [0, 0, 5.0]]
    pbc = [False, False, False]
    positions = [[0.0, 0.0, 0.0], [1.0, 0.0, 0.0], [0.5, 0.5, 0.0]]
    result = config_struct_hash(atomic_numbers, cell, pbc, positions)
    assert isinstance(result, str)
    assert len(result) == 128
    assert all(c in "0123456789abcdef" for c in result)


def test_config_struct_hash_consistent():
    atomic_numbers = [6, 6]
    cell = [[4.0, 0, 0], [0, 4.0, 0], [0, 0, 4.0]]
    pbc = [True, True, True]
    positions = [[0.0, 0.0, 0.0], [2.0, 2.0, 2.0]]
    h1 = config_struct_hash(atomic_numbers, cell, pbc, positions)
    h2 = config_struct_hash(atomic_numbers, cell, pbc, positions)
    assert h1 == h2


def test_config_struct_hash_independent_of_atom_order():
    """Structures with atoms in different order but same geometry should match."""
    cell = [[5.0, 0, 0], [0, 5.0, 0], [0, 0, 5.0]]
    pbc = [False, False, False]
    atomic_numbers_1 = [6, 1]
    positions_1 = [[0.0, 0.0, 0.0], [1.0, 0.0, 0.0]]
    atomic_numbers_2 = [1, 6]
    positions_2 = [[1.0, 0.0, 0.0], [0.0, 0.0, 0.0]]
    h1 = config_struct_hash(atomic_numbers_1, cell, pbc, positions_1)
    h2 = config_struct_hash(atomic_numbers_2, cell, pbc, positions_2)
    assert h1 == h2, "Structure hash should be independent of atom ordering"
