import logging

import pyspark.sql.functions as sf
from unidecode import unidecode

from colabfit import MAX_STRING_LENGTH
from colabfit.tools.vast.schema import dataset_schema
from colabfit.tools.vast.utilities import (
    ELEMENT_MAP,
    _empty_dict_from_schema,
    _hash,
    get_last_modified,
    str_to_arrayof_int,
    str_to_arrayof_str,
)

logger = logging.getLogger(__name__)


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
    ):
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
        self.configuration_set_ids = configuration_set_ids
        if self.configuration_set_ids is None:
            self.configuration_set_ids = []
        self.row_dict = self.to_row_dict(config_df=config_df, prop_df=prop_df)
        self.row_dict["id"] = self.dataset_id
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
        self._hash = _hash(self.row_dict, ["extended_id"])
        self.row_dict["hash"] = str(self._hash)
        self.row_dict["labels"] = labels
        logger.info(self.row_dict)

    def to_row_dict(self, config_df, prop_df):
        """"""
        row_dict = _empty_dict_from_schema(dataset_schema)
        row_dict["last_modified"] = get_last_modified()
        row_dict["nconfiguration_sets"] = len(self.configuration_set_ids)
        config_df = config_df.select(
            "id",
            "elements",
            "atomic_numbers",
            "nsites",
            "nperiodic_dimensions",
            "dimension_types",
            # "labels",
        )

        prop_df = prop_df.select(
            "id",
            "atomization_energy",
            "atomic_forces_00",
            "adsorption_energy",
            "electronic_band_gap",
            "cauchy_stress",
            "formation_energy",
            "energy",
        )

        int_array_cols = ["atomic_numbers", "dimension_types"]
        str_array_cols = ["elements"]
        config_df = config_df.select(
            [
                (
                    str_to_arrayof_int(sf.col(col)).alias(col)
                    if col in int_array_cols
                    else col
                )
                for col in config_df.columns
            ]
        ).select(
            [
                (
                    str_to_arrayof_str(sf.col(col)).alias(col)
                    if col in str_array_cols
                    else col
                )
                for col in config_df.columns
            ]
        )
        config_df.cache()
        agg_df = config_df.agg(
            sf.count_distinct("id").alias("nconfigurations"),
            sf.sum("nsites").alias("nsites"),
            sf.collect_set("nperiodic_dimensions").alias("nperiodic_dimensions"),
            sf.collect_set("dimension_types").alias("dimension_types"),
            sf.flatten(sf.collect_set("elements")).alias("elements"),
        )
        agg_row = agg_df.collect()[0]
        row_dict["nconfigurations"] = agg_row["nconfigurations"]
        row_dict["nsites"] = agg_row["nsites"]
        row_dict["nperiodic_dimensions"] = agg_row["nperiodic_dimensions"]
        row_dict["dimension_types"] = agg_row["dimension_types"]
        row_dict["elements"] = sorted(list(set(agg_row["elements"])))
        row_dict["nelements"] = len(row_dict["elements"])

        atomic_ratios_df = (
            config_df.select("atomic_numbers")
            .withColumn("single_element", sf.explode("atomic_numbers"))
            .groupBy("single_element")
            .agg(sf.count("single_element").alias("count"))
        )
        total_elements = atomic_ratios_df.agg(sf.sum("count")).collect()[0][0]
        atomic_ratios_df = atomic_ratios_df.withColumn(
            "ratio", sf.col("count") / total_elements
        )
        logger.info(f'{total_elements} {row_dict["nsites"]}')
        assert total_elements == row_dict["nsites"]

        element_map_expr = sf.create_map(
            [
                sf.lit(k)
                for pair in [(k, v) for k, v in ELEMENT_MAP.items()]
                for k in pair
            ]
        )

        atomic_ratios_coll = (
            atomic_ratios_df.withColumn(
                "element", element_map_expr[sf.col("single_element")]
            )
            .select("element", "ratio")
            .collect()
        )
        row_dict["total_elements_ratios"] = [
            x["ratio"] for x in sorted(atomic_ratios_coll, key=lambda x: x["element"])
        ]
        config_df.unpersist()

        count_df = prop_df.agg(
            sf.count_distinct("id").alias("nproperty_objects"),
            sf.count("atomization_energy").alias("atomization_energy_count"),
            sf.count("adsorption_energy").alias("adsorption_energy_count"),
            sf.count("electronic_band_gap").alias("electronic_band_gap_count"),
            sf.count("cauchy_stress").alias("cauchy_stress_count"),
            sf.count("formation_energy").alias("formation_energy_count"),
            sf.count("energy").alias("energy_count"),
            sf.variance("energy").alias("energy_variance"),
            sf.mean("energy").alias("energy_mean"),
            sf.count_if(
                (sf.col("atomic_forces_00") != "[]")
                & (sf.col("atomic_forces_00").isNotNull())
            ).alias("atomic_forces_count"),
        )
        count_row = count_df.collect()[0].asDict()
        row_dict.update(count_row)
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
