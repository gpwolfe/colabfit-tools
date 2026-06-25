import datetime
import warnings

from unidecode import unidecode

from colabfit import MAX_STRING_LENGTH
from colabfit.tools.pg.schema import dataset_schema
from colabfit.tools.pg.utils import (
    ELEMENT_MAP,
    _empty_dict_from_schema,
    _hash,
    get_date,
    get_last_modified,
)

import numpy as np


class Dataset:
    """
    A dataset defines a group of configuration sets and computed properties, and
    aggregates information about those configuration sets and properties.

    Attributes:

        configuration_set_ids (list):
            A list of attached configuration sets

        property_ids (list):
            A list of attached properties

        name (str):
            The name of the dataset

        authors (list or str or None):
            The names of the authors of the dataset.

        links (list or str or None):
            External links (e.g., journal articles, Git repositories, ...)
            to be associated with the dataset.

        description (str or None):
            A human-readable description of the dataset.

        aggregated_info (dict):
            A dictionary of information that was aggregated rom all of the
            attached configuration sets and properties. Contains the following
            information:

                From the configuration sets:
                    nconfigurations
                    nsites
                    nelements
                    chemical_systems
                    elements
                    individual_elements_ratios
                    total_elements_ratios
                    configuration_labels
                    configuration_labels_counts
                    chemical_formula_reduced
                    chemical_formula_anonymous
                    chemical_formula_hill
                    nperiodic_dimensions
                    dimension_types

                From the properties:
                    property_types
                    property_fields
                    methods
                    methods_counts
                    property_labels
                    property_labels_counts

        data_license (str):
            License associated with the Dataset's data
    """

    def __init__(
        self,
        name: str,
        authors: list[str],
        publication_link: str,
        data_link: str,
        description: str,
        config_df,
        prop_df,
        other_links: list[str] = None,
        dataset_id: str = None,
        labels: list[str] = None,
        doi: str = None,
        configuration_set_ids: list[str] = [],
        data_license: str = "CC-BY-ND-4.0",
        publication_year: str = None,
        date_requested: str = None,
        equilibrium: bool = False,
    ):
        if not isinstance(authors, list):
            raise TypeError("authors must be a list of strings")
        if not date_requested:
            raise RuntimeError("Missing required field date_requested")
        if isinstance(date_requested, str):
            date_requested = datetime.datetime.strptime(date_requested, "%Y-%m-%d")
        elif isinstance(date_requested, datetime.datetime):
            pass
        elif isinstance(date_requested, datetime.date):
            date_requested = datetime.datetime(
                date_requested.year, date_requested.month, date_requested.day
            )
        else:
            raise ValueError("date_requested must be a 'YYYY-MM-DD' string or datetime")
        if publication_year is not None and not (
            isinstance(publication_year, str)
            and publication_year.isdigit()
            and len(publication_year) == 4
        ):
            raise ValueError("publication_year must be a 4-digit year string")
        for auth in authors:
            if not "".join(auth.split(" ")[-1].replace("-", "")).isalpha():
                raise RuntimeError(
                    f"Bad author name '{auth}'. Author names "
                    "can only contain [a-z][A-Z]"
                )

        self.name = name
        self.authors = authors
        self.publication_link = publication_link
        self.data_link = data_link
        self.other_links = other_links
        self.description = description
        self.data_license = data_license
        self.dataset_id = dataset_id
        self.doi = doi
        self.publication_year = publication_year
        self.date_requested = date_requested
        self.equilibrium = equilibrium
        self.configuration_set_ids = configuration_set_ids
        if self.configuration_set_ids is None:
            self.configuration_set_ids = []
        self.row_dict = self.to_row_dict(configs=config_df, props=prop_df)
        self.row_dict["id"] = self.dataset_id

        id_prefix = "__".join(
            [
                self.name,
                "-".join([unidecode(auth.split()[-1]) for auth in authors]),
            ]
        )
        if len(id_prefix) > (MAX_STRING_LENGTH - len(dataset_id) - 2):
            id_prefix = id_prefix[: MAX_STRING_LENGTH - len(dataset_id) - 2]
            warnings.warn(f"ID prefix is too long. Clipping to {id_prefix}")
        extended_id = f"{id_prefix}__{dataset_id}"
        self.row_dict["extended_id"] = extended_id
        self._hash = _hash(self.row_dict, ["extended_id"])
        self.row_dict["hash"] = self._hash
        self.row_dict["labels"] = labels

    # aggregate stuff
    def to_row_dict(self, configs, props):
        row_dict = _empty_dict_from_schema(dataset_schema)
        row_dict["last_modified"] = get_last_modified()
        row_dict["nconfigurations"] = len(configs)
        row_dict["nproperty_objects"] = len(props)
        nsites = 0
        nperiodic_dimensions = set()
        seen_dim_type_keys = set()
        unique_dim_types = []
        element_dict = {}

        for c in configs:
            nsites += c["nsites"]
            for e in c["atomic_numbers"]:
                el = ELEMENT_MAP[e]
                element_dict[el] = element_dict.get(el, 0) + 1
            nperiodic_dimensions.add(c["nperiodic_dimensions"])
            dt = c["dimension_types"]
            if dt is not None:
                key = tuple(dt)
                if key not in seen_dim_type_keys:
                    seen_dim_type_keys.add(key)
                    unique_dim_types.append(list(dt))

        sorted_elements = sorted(element_dict.keys())

        row_dict["nsites"] = nsites
        row_dict["nelements"] = len(sorted_elements)
        row_dict["elements"] = sorted_elements
        row_dict["total_elements_ratios"] = (
            [element_dict[e] / nsites for e in sorted_elements] if nsites else []
        )
        row_dict["nperiodic_dimensions"] = sorted(nperiodic_dimensions)
        row_dict["dimension_types"] = unique_dim_types

        # Per-property counts, keyed by the property-object column names.
        count_keys = [
            "energy",
            "atomic_forces",
            "cauchy_stress",
            "electronic_band_gap",
            "formation_energy",
            "adsorption_energy",
            "atomization_energy",
            "energy_above_hull",
        ]
        counts = {k: 0 for k in count_keys}
        energies = []
        methods = set()
        software = set()
        for p in props:
            for k in count_keys:
                if p.get(k) is not None:
                    counts[k] += 1
            if p.get("energy") is not None:
                energies.append(p["energy"])
            if p.get("method") is not None:
                methods.add(p["method"])
            if p.get("software") is not None:
                software.add(p["software"])

        row_dict["energy_count"] = counts["energy"]
        row_dict["atomic_forces_count"] = counts["atomic_forces"]
        row_dict["cauchy_stress_count"] = counts["cauchy_stress"]
        row_dict["electronic_band_gap_count"] = counts["electronic_band_gap"]
        row_dict["formation_energy_count"] = counts["formation_energy"]
        row_dict["adsorption_energy_count"] = counts["adsorption_energy"]
        row_dict["atomization_energy_count"] = counts["atomization_energy"]
        row_dict["energy_above_hull_count"] = counts["energy_above_hull"]
        row_dict["energy_mean"] = float(np.mean(energies)) if energies else None
        row_dict["energy_variance"] = float(np.var(energies)) if energies else None
        row_dict["methods"] = sorted(methods)
        row_dict["software"] = sorted(software)
        row_dict["authors"] = self.authors
        row_dict["description"] = self.description
        row_dict["license"] = self.data_license
        row_dict["links"] = {
            "source-publication": self.publication_link,
            "source-data": self.data_link,
            "other": self.other_links,
        }
        row_dict["name"] = self.name
        row_dict["publication_year"] = self.publication_year
        row_dict["doi"] = self.doi
        row_dict["date_added_to_colabfit"] = get_date()
        row_dict["date_requested"] = self.date_requested
        row_dict["equilibrium"] = self.equilibrium

        return row_dict

    def __str__(self):
        return (
            f"Dataset(description='{self.description}', "
            f"nconfiguration_sets={len(self.configuration_set_ids)}, "
            f"nproperty_objects={self.row_dict['nproperty_objects']}, "
            f"nconfigurations={self.row_dict['nconfigurations']})"
        )

    def __repr__(self):
        return str(self)
