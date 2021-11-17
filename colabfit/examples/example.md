# Summary

This section is ignored during parsing. It is intended to be used as a place to
display additional information about a Dataset, and will be ignored by `Dataset.from_markdown()`.

All other sections and tables are required for use with
`Dataset.from_markdown()`. Sections should be specified using `<h1>` headers.
Exact header spelling and capitalization is required. All table column headers
are required unless otherwise specified. Sections that have tables in them will
only use the table for `Dataset.from_markdown()`. Sections may be given in different
orders than shown here. Additional sections may be added, but will be ignored by
`Dataset.from_markdown()`.

Any files should be provided as links in Markdown as described
[here](https://www.markdownguide.org/basic-syntax/#links), using the format
`"[<text>](<path_to_file>)"`.


The table below will be automatically generated when using `Dataset.to_markdown()`.

|||
|---|---|
|Chemical systems||
|Element ratios||
|# of configurations||
|# of atoms||

# Name

A single-line name to use for the dataset.

# Authors

Authors of the dataset (one per line)

# Links

Links to associate with the dataset (one per line)

# Description

A multi-line human-readable description of the dataset

# Data

A table for storing the inputs to `load_data()`. First column of each row ('Elements', 'File', 'Format', 'Name field') must be as spelled/capitalized here. File name must Must have the following rows (with first columns spelled/capitalized in this way):

|||
|---|---|
|Elements| a comma-separated list of elements ("Mo, Ni, Cu")|
|File| a hyperlink to the raw data file ([example.extxyz](example.extxyz))|
|Format| a string specifying the file format ('xyz', 'cfg')|
|Name field| the key to use as the name of configurations ('name')|

# Properties

The table below should have the following columns:
* `Property`: An OpenKIM Property Definition ID from [the list of approved OpenKIM Property
   Definitions](https://openkim.org/properties), OR a link to a local EDN file
   (using the notation `[<name_of_property>](<path_to_edn_file>)`) that has been
   formatted according to the [KIM Properties
   Framework](https://openkim.org/doc/schema/properties-framework/) , OR
   `default`. Note that `default` means that the generic base property for
   energies/forces/stresses/etc. should be used.
* `KIM field`: A string that can be used as a key for an EDN dictionary
  representation of an OpenKIM Property Instance
* `ASE field`: A string that can be used as a key for the `info` or `arrays`
  dictionary on a configuration
* `Units`: A string describing the units of the field (in an ASE-readable
  format), or `None` if the field is unitless

|Property|KIM field|ASE field|Units|
|---|---|---|---|
|default|energy|energy|eV|
|default|forces|F|eV/Ang|
|[my-custom-property](colabfit/tests/files/test_property.edn)|a-custom-field-name|field-name|None|
|[my-custom-property](colabfit/tests/files/test_property.edn)|a-custom-1d-array|1d-array|eV|
|[my-custom-property](colabfit/tests/files/test_property.edn)|a-custom-per-atom-array|per-atom-array|eV|



# Property settings

The tabular form of `Dataset.property_settings_regexes`. Note that the "Labels" and "Files" columns should be in the table, but may be left empty. For example:

|Regex|Method|Description|Labels|Files|
|---|---|---|---|---|
| `.*` | VASP | energies/forces/stresses | PBE, GGA |  |

# Configuration sets

The tabular form of `Dataset.configuration_set_regexes`. Only the first two columns ("Regex" and "Description") are required. The rest will be filled in programmatically by `Dataset.to_markdown()` and ignored by `Dataset.from_markdown()`. For example:

|Regex|Description|# of structures| # of atoms|
|---|---|---|---|
| `default` | The default CS to use for configurations | | |
| `H2O` | AIMD snapshots of liquid water at 100K | | |

# Configuration labels

The tabular form of `Dataset.configuration_label_regexes`. Only the first two columns ("Regex" and "Labels") are required. The rest will be filled in programmatically by `Dataset.to_markdown()` and ignored by `Dataset.from_markdown()`. For example:

|Regex|Labels|Counts|
|---|---|---|
| `H2O` | water, molecule |  |
| `.*` | 100K |  |