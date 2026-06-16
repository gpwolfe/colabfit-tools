import datetime
import json
from hashlib import sha512

import numpy as np

METADATA_MAX_CHARS = 10_000


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

    Replaces the legacy ``int(hexdigest, 16)`` form. Callers that need an int
    (e.g. a Python ``__hash__`` dunder) should wrap the result in
    ``int(result, 16)``.
    """
    identifying_key_list = sorted(identifying_key_list)
    identifiers = [row[k] for k in identifying_key_list]
    _hash = sha512()
    for k, v in zip(identifying_key_list, identifiers):
        if v is None or v == "[]":
            continue
        if include_keys_in_hash:
            _hash.update(_format_for_hash(k))
        _hash.update(_format_for_hash(v))
    return _hash.hexdigest()


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


def get_last_modified():
    return datetime.datetime.now(tz=datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _empty_dict_from_schema(schema):
    empty_dict = {}
    for field in schema.columns:
        empty_dict[field.name] = None
    return empty_dict


def _sort_dict(dictionary):
    keys = list(dictionary.keys())
    keys.sort()
    return {k: dictionary[k] for k in keys}


def _parse_unstructured_metadata(md_json):
    """Return ``{"metadata": <sorted dict>}`` for storage in a JSONB column.

    Value is kept as a dict with deterministic, recursively sorted keys.
    A size guard is enforced against the serialized form.
    """
    if not md_json:
        return {"metadata": None}
    md = {}
    for key, val in md_json.items():
        if key in ["_id", "hash", "colabfit-id", "last_modified", "software", "method"]:
            continue
        if isinstance(val, dict):
            if "source-value" in val.keys():
                val = val["source-value"]
        if isinstance(val, list) and len(val) == 1:
            val = val[0]
        if isinstance(val, np.ndarray):
            val = val.tolist()
        if isinstance(val, dict):
            val = _sort_dict(val)
        if isinstance(val, bytes):
            val = val.decode("utf-8")
        md[key] = val
    md = _sort_dict(md)
    serialized_len = len(json.dumps(md))
    if serialized_len > METADATA_MAX_CHARS:
        raise ValueError(
            f"Metadata exceeds maximum allowed size of {METADATA_MAX_CHARS} "
            f"characters ({serialized_len} chars). Reduce metadata content "
            "before ingesting."
        )
    return {"metadata": md}


def stringify_lists(row_dict):
    """
    Replace list/tuple fields with comma-separated strings.
    Spark and Vast both support array columns, but the connector does not,
    so keeping cell values in list format crashes the table.
    Use with dicts
    TODO: Remove when no longer necessary
    """
    for key, val in row_dict.items():
        if isinstance(val, (list, tuple, dict)):
            row_dict[key] = str(val)
        # Below would convert numpy arrays to comma-separated
        elif isinstance(val, np.ndarray):
            row_dict[key] = str(val.tolist())
    return row_dict


def append_ith_element_to_rdd_labels(row_elem):
    """
    row_elem: tuple created by joining two RDD.zipWithIndex
    new_labels: list of labels
    """
    index, (co_row, new_labels) = row_elem
    val = co_row.get("labels")
    if val is None:
        val = new_labels
    else:
        val.extend(new_labels)
        val = list(set(val))
    co_row["labels"] = val
    return co_row


def add_elem_to_row_dict(col, elem, row_dict):
    val = row_dict.get(col)
    if val is None:
        val = [elem]
    else:
        val.append(elem)
        val = list(set(val))
    row_dict[col] = val
    return row_dict


def convert_stress(keys, stress):
    """Convert a size-6 array of stress components to a 3x3 matrix

    In particular for VASP output. Assumes symmetric matrix.
    Check order of keys."""
    stresses = {k: s for k, s in zip(keys, stress)}
    return [
        [stresses["xx"], stresses["xy"], stresses["xz"]],
        [stresses["xy"], stresses["yy"], stresses["yz"]],
        [stresses["xz"], stresses["yz"], stresses["zz"]],
    ]


ELEMENT_MAP = {
    1: "H",
    2: "He",
    3: "Li",
    4: "Be",
    5: "B",
    6: "C",
    7: "N",
    8: "O",
    9: "F",
    10: "Ne",
    11: "Na",
    12: "Mg",
    13: "Al",
    14: "Si",
    15: "P",
    16: "S",
    17: "Cl",
    18: "Ar",
    19: "K",
    20: "Ca",
    21: "Sc",
    22: "Ti",
    23: "V",
    24: "Cr",
    25: "Mn",
    26: "Fe",
    27: "Co",
    28: "Ni",
    29: "Cu",
    30: "Zn",
    31: "Ga",
    32: "Ge",
    33: "As",
    34: "Se",
    35: "Br",
    36: "Kr",
    37: "Rb",
    38: "Sr",
    39: "Y",
    40: "Zr",
    41: "Nb",
    42: "Mo",
    43: "Tc",
    44: "Ru",
    45: "Rh",
    46: "Pd",
    47: "Ag",
    48: "Cd",
    49: "In",
    50: "Sn",
    51: "Sb",
    52: "Te",
    53: "I",
    54: "Xe",
    55: "Cs",
    56: "Ba",
    57: "La",
    58: "Ce",
    59: "Pr",
    60: "Nd",
    61: "Pm",
    62: "Sm",
    63: "Eu",
    64: "Gd",
    65: "Tb",
    66: "Dy",
    67: "Ho",
    68: "Er",
    69: "Tm",
    70: "Yb",
    71: "Lu",
    72: "Hf",
    73: "Ta",
    74: "W",
    75: "Re",
    76: "Os",
    77: "Ir",
    78: "Pt",
    79: "Au",
    80: "Hg",
    81: "Tl",
    82: "Pb",
    83: "Bi",
    84: "Po",
    85: "At",
    86: "Rn",
    87: "Fr",
    88: "Ra",
    89: "Ac",
    90: "Th",
    91: "Pa",
    92: "U",
    93: "Np",
    94: "Pu",
    95: "Am",
    96: "Cm",
    97: "Bk",
    98: "Cf",
    99: "Es",
    100: "Fm",
    101: "Md",
    102: "No",
    103: "Lr",
    104: "Rf",
    105: "Db",
    106: "Sg",
    107: "Bh",
    108: "Hs",
    109: "Mt",
    110: "Ds",
    111: "Rg",
    112: "Cn",
    113: "Nh",
    114: "Fl",
    115: "Mc",
    116: "Lv",
    117: "Ts",
    118: "Og",
}
