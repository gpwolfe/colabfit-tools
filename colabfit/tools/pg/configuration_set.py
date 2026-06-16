from hashlib import sha512

from colabfit.tools.pg.schema import configuration_set_schema
from colabfit.tools.pg.utilities import (
    ELEMENT_MAP,
    _empty_dict_from_schema,
    get_last_modified,
)


class ConfigurationSet:
    """
    A configuration set defines a group of configurations and aggregates
    information about those configurations to improve queries.

    Note that a configuration set should only be constructed by loading from an
    existing database (in order to aggregate the info)

    Attributes:

        configuration_ids (list):
            A list of all attached configuration IDs

        description (str):
            A human-readable description of the configuration set

        aggregated_info (dict):
            A dictionary of information that was aggregated from all of the
            attached configurations. Contains the following information:

                * nconfigurations: the total number of configurations
                * nsites: the total number of sites
                * nelements: the total number of unique element types
                * elements: the element types
                * total_elements_ratios: the ratio of the total count of atoms of
                  each element type over nsites
                * nperiodic_dimensions: the set of all numbers of periodic dimensions
                * dimension_types: the set of all periodic boundary choices

    """

    def __init__(self, config_df, name, description, dataset_id, ordered=False):
        self.name = name
        self.description = description
        self.dataset_id = dataset_id
        self.ordered = ordered
        self.row_dict = self.to_row_dict(config_df)
        self.id = f"CS_{self.name}_{self.dataset_id}"
        self._hash = sha512(self.id.encode("utf-8")).hexdigest()
        self.row_dict["id"] = self.id
        self.row_dict["extended_id"] = f"{self.name}__{self.id}"
        self.row_dict["hash"] = self._hash

    def to_row_dict(self, config_df):
        """Aggregate configuration statistics from a list of configuration rows.

        ``config_df`` is an iterable of dict-like configuration rows (as returned
        by ``DataManager.config_set_query``). Duplicate configuration ids are
        counted once.
        """
        row_dict = _empty_dict_from_schema(configuration_set_schema)
        row_dict["name"] = self.name
        row_dict["description"] = self.description
        row_dict["dataset_id"] = self.dataset_id
        row_dict["ordered"] = self.ordered
        row_dict["last_modified"] = get_last_modified()

        seen_ids = set()
        nsites = 0
        element_counts = {}
        nperiodic_dimensions = set()
        dimension_types = set()
        for c in config_df:
            cid = c.get("id")
            if cid is not None and cid in seen_ids:
                continue
            seen_ids.add(cid)
            nsites += c["nsites"]
            for e in c["atomic_numbers"]:
                el = ELEMENT_MAP[e]
                element_counts[el] = element_counts.get(el, 0) + 1
            nperiodic_dimensions.add(c["nperiodic_dimensions"])
            dimension_types.add(str(c["dimension_types"]))

        sorted_elements = sorted(element_counts.keys())
        row_dict["nconfigurations"] = len(seen_ids)
        row_dict["nsites"] = nsites
        row_dict["elements"] = sorted_elements
        row_dict["nelements"] = len(sorted_elements)
        row_dict["total_elements_ratios"] = (
            [element_counts[e] / nsites for e in sorted_elements] if nsites else []
        )
        row_dict["nperiodic_dimensions"] = list(nperiodic_dimensions)
        row_dict["dimension_types"] = list(dimension_types)
        return row_dict

    def __hash__(self):
        return int(sha512(self.id.encode("utf-8")).hexdigest(), 16)

    def __str__(self):
        return "ConfigurationSet(description='{}', nconfigurations={})".format(
            self.description,
            self.row_dict["nconfigurations"],
        )

    def __repr__(self):
        return str(self)
