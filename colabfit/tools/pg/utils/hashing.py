from hashlib import sha512

import numpy as np


def _format_for_hash(v):
    if isinstance(v, (list, tuple)):
        v = np.array(v)
    if isinstance(v, np.ndarray):
        if np.issubdtype(v.dtype, np.floating):
            return np.round(v.astype(np.float64), decimals=16)
        elif np.issubdtype(v.dtype, np.integer):
            return v.astype(np.int64)
        elif np.issubdtype(v.dtype, np.bool_):
            return v.astype(np.int64)
        else:
            return v
    elif isinstance(v, dict):
        return str(v).encode("utf-8")
    elif isinstance(v, str):
        return v.encode("utf-8")
    elif isinstance(v, (int, float)):
        return np.array(v).data.tobytes()
    else:
        return v


def _hash(row, identifying_key_list, include_keys_in_hash=False):
    """Return a 128-char SHA-512 hex digest of the identifying values in ``row``.

    Callers that need an int (e.g. a Python ``__hash__`` dunder) should wrap
    the result in ``int(result, 16)``.
    """
    identifying_key_list = sorted(identifying_key_list)
    identifiers = [row[k] for k in identifying_key_list]
    h = sha512()
    for k, v in zip(identifying_key_list, identifiers):
        if v is None or v == "[]":
            continue
        if include_keys_in_hash:
            h.update(_format_for_hash(k))
        h.update(_format_for_hash(v))
    return h.hexdigest()


def _sorted_struct_hash(atomic_numbers, cell, pbc, positions):
    """Shared structure hashing logic; returns a sha512 digest object.

    Atoms are sorted lexicographically by position so that the digest is
    independent of atom ordering for an otherwise identical geometry.
    """
    h = sha512()
    positions = np.array(positions)
    sort_ixs = np.lexsort(
        (
            positions[:, 2],
            positions[:, 1],
            positions[:, 0],
        )
    )
    sorted_positions = positions[sort_ixs]
    atomic_numbers = np.array(atomic_numbers)
    sorted_atomic_numbers = atomic_numbers[sort_ixs]
    h.update(bytes(_format_for_hash(sorted_atomic_numbers)))
    h.update(bytes(_format_for_hash(cell)))
    h.update(bytes(_format_for_hash(pbc)))
    h.update(bytes(_format_for_hash(sorted_positions)))
    return h


def config_struct_hash(atomic_numbers, cell, pbc, positions):
    """Structure hash for configuration creation. Returns a 128-char hex string."""
    return _sorted_struct_hash(atomic_numbers, cell, pbc, positions).hexdigest()
