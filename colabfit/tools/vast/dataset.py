import logging
from collections import Counter
from datetime import datetime

from unidecode import unidecode

from colabfit import MAX_STRING_LENGTH
from colabfit.tools.vast.schema import dataset_schema
from colabfit.tools.vast.data_object import DataObject
from colabfit.tools.vast.utils import (
    ELEMENT_MAP,
    _empty_dict_from_schema,
    get_date,
    get_last_modified,
)

logger = logging.getLogger(__name__)


class Dataset(DataObject):
    """
    Represents a dataset that aggregates configuration sets and computed properties,
    along with associated metadata and aggregated statistics.

    A Dataset object is used to organize and summarize information about a collection
    of configuration sets and their computed properties, including authorship,
    publication links, licensing, and various aggregated statistics derived from the
    data.

    Parameters
    ----------
    name : str
        The name of the dataset.
    authors : list of str
    publication_link : str
        Link to the publication associated with the dataset.
    data_link : str
        Link to the source data for the dataset.
    description : str
    config_df : pa.Table
        Table containing configuration/property data.
    other_links : list of str, optional
        Additional external links related to the dataset (default: None).
    dataset_id : str, optional
        Unique identifier for the dataset (default: None).
    labels : list of str, optional
        List of labels associated with the dataset (default: None).
    doi : str, optional
        Digital Object Identifier for the dataset (default: None).
    configuration_set_ids : list of str, optional
        List of configuration set IDs attached to the dataset (default: []).
    data_license : str, optional
        License associated with the dataset's data (default: "CC-BY-ND-4.0").
    publication_year : str, optional
        Year of publication for the dataset (default: None).
    """

    def __init__(
        self,
        name: str,
        authors: list[str],
        publication_link: str,
        data_link: str,
        description: str,
        config_batches,
        other_links: list[str] = None,
        dataset_id: str = None,
        labels: list[str] = None,
        doi: str = None,
        configuration_set_ids: list[str] = [],
        data_license: str = None,
        date_requested: str = None,
        publication_year: str = None,
        equilibrium: bool = False,
    ):
        for auth in authors:
            if not "".join(auth.split(" ")[-1].replace("-", "")).isalpha():
                raise RuntimeError(
                    f"Bad author name '{auth}'. Author names "
                    "can only contain [a-z][A-Z]"
                )
        for required in (date_requested, data_license, publication_year, dataset_id):
            if not required:
                raise RuntimeError(f"Missing required field {required}")
        self.name = name
        self.authors = authors
        self.publication_link = publication_link
        self.data_link = data_link
        self.other_links = other_links
        self.description = description
        self.data_license = data_license
        self.dataset_id = dataset_id
        self.doi = doi
        assert datetime.strptime(publication_year, "%Y")
        self.publication_year = publication_year
        self.configuration_set_ids = configuration_set_ids
        self.equilibrium = equilibrium
        if self.configuration_set_ids is None:
            self.configuration_set_ids = []
        self.row_dict = self.to_row_dict(config_batches=config_batches)
        self.row_dict["date_added_to_colabfit"] = get_date()
        assert datetime.strptime(date_requested, "%Y-%m-%d")
        self.row_dict["date_requested"] = datetime.strptime(date_requested, "%Y-%m-%d")
        id_prefix = "__".join(
            [
                self.name,
                "-".join([unidecode(auth.split()[-1]) for auth in authors]),
            ]
        )
        if len(id_prefix) > (MAX_STRING_LENGTH - len(dataset_id) - 2):
            id_prefix = id_prefix[: MAX_STRING_LENGTH - len(dataset_id) - 2]
            logger.warning(f"ID prefix is too long. Clipping to {id_prefix}")
        extended_id = f"{id_prefix}__{dataset_id}"
        self.row_dict["extended_id"] = extended_id
        self._generate_hash_and_id()
        self.row_dict["id"] = self.dataset_id
        self.row_dict["labels"] = labels
        logger.info(self.row_dict)

    def get_identifier_keys(self) -> list[str]:
        """Return the keys used for Dataset identification."""
        return ["extended_id"]

    def to_row_dict(self, config_batches):
        row_dict = _empty_dict_from_schema(dataset_schema)
        row_dict["last_modified"] = get_last_modified()
        row_dict["nconfiguration_sets"] = len(self.configuration_set_ids)

        seen_configs = set()
        seen_props = set()
        nsites = 0
        seen_nperiodic = set()
        seen_dim_type_keys = set()
        unique_dim_types = []
        all_elements = set()
        element_counts = Counter()
        seen_methods = set()
        seen_software = set()
        prop_counts = {
            "atomization_energy": 0,
            "adsorption_energy": 0,
            "electronic_band_gap": 0,
            "cauchy_stress": 0,
            "energy_above_hull": 0,
            "formation_energy": 0,
            "energy": 0,
        }
        atomic_forces_count = 0
        energy_n = 0
        energy_mean_acc = 0.0
        energy_m2 = 0.0

        for batch in config_batches:
            prop_ids = batch.column("property_id").to_pylist()
            config_ids = batch.column("configuration_id").to_pylist()
            nsites_col = batch.column("nsites").to_pylist()
            nperiodic_col = batch.column("nperiodic_dimensions").to_pylist()
            dim_types_col = batch.column("dimension_types").to_pylist()
            elements_col = batch.column("elements").to_pylist()
            atomic_nums_col = batch.column("atomic_numbers").to_pylist()
            method_col = batch.column("method").to_pylist()
            software_col = batch.column("software").to_pylist()
            atomization_col = batch.column("atomization_energy").to_pylist()
            adsorption_col = batch.column("adsorption_energy").to_pylist()
            band_gap_col = batch.column("electronic_band_gap").to_pylist()
            cauchy_col = batch.column("cauchy_stress").to_pylist()
            hull_col = batch.column("energy_above_hull").to_pylist()
            formation_col = batch.column("formation_energy").to_pylist()
            energy_col = batch.column("energy").to_pylist()
            forces_col = batch.column("atomic_forces").to_pylist()

            for i in range(len(prop_ids)):
                if config_ids[i] is not None:
                    seen_configs.add(config_ids[i])
                if prop_ids[i] is not None:
                    seen_props.add(prop_ids[i])
                nsites += nsites_col[i] or 0
                npd = nperiodic_col[i]
                if npd is not None:
                    seen_nperiodic.add(npd)
                dt = dim_types_col[i]
                if dt is not None:
                    key = tuple(dt)
                    if key not in seen_dim_type_keys:
                        seen_dim_type_keys.add(key)
                        unique_dim_types.append(dt)
                elems = elements_col[i]
                if elems:
                    all_elements.update(elems)
                nums = atomic_nums_col[i]
                if nums:
                    element_counts.update(nums)
                if method_col[i] is not None:
                    seen_methods.add(method_col[i])
                if software_col[i] is not None:
                    seen_software.add(software_col[i])
                if atomization_col[i] is not None:
                    prop_counts["atomization_energy"] += 1
                if adsorption_col[i] is not None:
                    prop_counts["adsorption_energy"] += 1
                if band_gap_col[i] is not None:
                    prop_counts["electronic_band_gap"] += 1
                if cauchy_col[i] is not None:
                    prop_counts["cauchy_stress"] += 1
                if hull_col[i] is not None:
                    prop_counts["energy_above_hull"] += 1
                if formation_col[i] is not None:
                    prop_counts["formation_energy"] += 1
                e = energy_col[i]
                if e is not None:
                    prop_counts["energy"] += 1
                    energy_n += 1
                    delta = e - energy_mean_acc
                    energy_mean_acc += delta / energy_n
                    energy_m2 += delta * (e - energy_mean_acc)
                af = forces_col[i]
                if af is not None and len(af) > 0:
                    atomic_forces_count += 1

        row_dict["nconfigurations"] = len(seen_configs)
        row_dict["nsites"] = nsites
        row_dict["nperiodic_dimensions"] = sorted(seen_nperiodic)
        row_dict["dimension_types"] = unique_dim_types
        row_dict["methods"] = sorted(seen_methods)
        row_dict["software"] = sorted(seen_software)
        row_dict["elements"] = sorted(list(all_elements))
        row_dict["nelements"] = len(row_dict["elements"])

        total_elements = sum(element_counts.values())
        logger.info(f'{total_elements} {row_dict["nsites"]}')
        assert total_elements == row_dict["nsites"]

        element_symbol_counts = {
            ELEMENT_MAP[num]: count for num, count in element_counts.items()
        }
        sorted_symbols = sorted(element_symbol_counts.keys())
        row_dict["total_elements_ratios"] = [
            element_symbol_counts[sym] / total_elements for sym in sorted_symbols
        ]

        row_dict["nproperty_objects"] = len(seen_props)
        for prop_col, count in prop_counts.items():
            row_dict[f"{prop_col}_count"] = count
        row_dict["atomic_forces_count"] = atomic_forces_count

        if energy_n > 0:
            row_dict["energy_variance"] = energy_m2 / energy_n
            row_dict["energy_mean"] = energy_mean_acc
        else:
            row_dict["energy_variance"] = None
            row_dict["energy_mean"] = None

        row_dict["authors"] = self.authors
        row_dict["description"] = self.description
        row_dict["license"] = self.data_license
        row_dict["links"] = str(
            {
                "source-publication": self.publication_link,
                "source-data": self.data_link,
                "other": self.other_links,
            }
        )
        row_dict["name"] = self.name
        row_dict["publication_year"] = self.publication_year
        row_dict["doi"] = self.doi
        row_dict["equilibrium"] = self.equilibrium
        return row_dict

    def __str__(self):
        return (
            f"Dataset(description='{self.description}', "
            f"nconfiguration_sets={len(self.row_dict['configuration_sets'])}, "
            f"nproperty_objects={self.row_dict['nproperty_objects']}, "
            f"nconfigurations={self.row_dict['nconfigurations']}"
        )

    def __repr__(self):
        return str(self)
