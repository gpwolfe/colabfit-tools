import json

import numpy as np

METADATA_MAX_CHARS = 10_000


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
