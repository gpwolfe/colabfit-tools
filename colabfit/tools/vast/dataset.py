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
    config_df : pyspark.sql.DataFrame
        DataFrame containing configuration set information.
    prop_df : pyspark.sql.DataFrame
        DataFrame containing property information.
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

    Attributes
    ----------
    name : str
        The name of the dataset.
    authors : list of str
    publication_link : str
        Link to the publication associated with the dataset.
    data_link : str
        Link to the source data for the dataset.
    other_links : list of str or None
        Additional external links related to the dataset.
    description : str
    data_license : str
        License associated with the dataset's data.
    dataset_id : str or None
        Unique identifier for the dataset.
    doi : str or None
        Digital Object Identifier for the dataset.
    publication_year : str or None
        Year of publication for the dataset.
    configuration_set_ids : list of str
        List of configuration set IDs attached to the dataset.
    row_dict : dict
        Dictionary containing aggregated statistics and metadata for the dataset,
        appropriate for use in a Spark DataFrame, Vast DB table or similar.
        Includes:
            - nconfigurations: Number of configurations.
            - nsites: Total number of sites.
            - nelements: Number of unique elements.
            - elements: List of unique elements.
            - nperiodic_dimensions: Distinct periodic dimensions.
            - dimension_types: Distinct dimension types.
            - total_elements_ratios: Ratios of each element in the dataset.
            - nproperty_objects: Number of property objects.
            - Various property counts and statistics.
            - dataset authors
            - dataset description
            - dataset license
            - links to publication, data, and resources associated with the dataset.
            - dataset name
            - dataset publication_year
            - doi: Digital Object Identifier for the dataset.
    labels : list of str or None
        List of labels associated with the dataset.

    Methods
    -------
    to_row_dict(config_df, prop_df)
        Aggregates statistics and metadata from the configuration and property DataFrames
        into a dictionary representation suitable for use in a Spark DataFrame, Vast DB
        table or similar.

    __str__()
        Returns a string summary of the dataset.

    __repr__()
        Returns a string representation of the dataset.
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
        equilibrium: bool = False,
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
        self.equilibrium = equilibrium
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
            "atomic_forces",
            "adsorption_energy",
            "electronic_band_gap",
            "energy_above_hull",
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
            sf.count("energy_above_hull").alias("energy_above_hull_count"),
            sf.count("formation_energy").alias("formation_energy_count"),
            sf.count("energy").alias("energy_count"),
            sf.variance("energy").alias("energy_variance"),
            sf.mean("energy").alias("energy_mean"),
            sf.count_if(
                (sf.col("atomic_forces") != "[]") & (sf.col("atomic_forces").isNotNull())
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
