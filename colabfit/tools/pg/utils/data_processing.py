import datetime

import numpy as np


def get_last_modified():
    return datetime.datetime.now(tz=datetime.timezone.utc).replace(
        microsecond=0, tzinfo=None
    )


def get_date():
    now = datetime.datetime.now(tz=datetime.timezone.utc)
    return datetime.datetime(now.year, now.month, now.day)


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
    """Convert a size-6 array of stress components to a 3x3 matrix.

    Assumes a symmetric matrix. Check order of keys.
    """
    stresses = {k: s for k, s in zip(keys, stress)}
    return [
        [stresses["xx"], stresses["xy"], stresses["xz"]],
        [stresses["xy"], stresses["yy"], stresses["yz"]],
        [stresses["xz"], stresses["yz"], stresses["zz"]],
    ]


def stringify_lists(row_dict):
    """Replace list/tuple fields with comma-separated strings.

    TODO: Remove when no longer necessary.
    """
    for key, val in row_dict.items():
        if isinstance(val, (list, tuple, dict)):
            row_dict[key] = str(val)
        elif isinstance(val, np.ndarray):
            row_dict[key] = str(val.tolist())
    return row_dict


def append_ith_element_to_rdd_labels(row_elem):
    """row_elem: tuple created by joining two RDD.zipWithIndex."""
    index, (co_row, new_labels) = row_elem
    val = co_row.get("labels")
    if val is None:
        val = new_labels
    else:
        val.extend(new_labels)
        val = list(set(val))
    co_row["labels"] = val
    return co_row
