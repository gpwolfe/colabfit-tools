from colabfit.tools.pg.property import MAIN_KEY_MAP, PropertyInfo, property_info


class PropertyMap:
    """
    A class to store and check metadata and key/field mappings for Property objects.

    Initialize with list of property definitions, then add properties with set_property
    or set_properties.
    Add metadata fields with set_metadata_field: 'software', 'method', 'input' required.
    Use get_property_map to validate and return a complete property map to use with
    Property objects.

    Attributes:
    -----------
    property_definitions : list
        List of property definitions.
    properties : dict
        Property map of property names to keys/fields and units.
    _metadata : dict
        Metadata information (equivalent to PI_METADATA from prior implementation).

    Methods:
    --------
    set_metadata_field(key: str, value: any, dynamic=False):
        Sets a metadata field with the given key and value. If dynamic is True, metadata
        will be populated from a field in the AtomicConfiguration object
        (ie: atom.info[field]). Otherwise a constant value. Default is False.
    get_metadata():
        Validates and returns a dictionary of metadata.
    set_property(property_name: str, field: str, units: str, original_file_key: str,
                                additional: list[tuple] = []):
        Sets a property with the details from dict or property_info namedtuple.
    set_properties(properties: list[dict | property_info]):
        Sets multiple properties from a list of dictionaries or property_info objects.
    get_property(property_name: str):
        Returns property details by name of property.
    validate_metadata():
        Ensure required fields are set in metadata.
    validate_properties():
        Ensure required fields and units are set in properties.
    get_property_map():
        Validates, returns the complete property map including metadata and properties.
    """

    def __init__(self, property_definitions: list):
        self.property_definitions = {
            p["property-name"]: p for p in property_definitions
        }
        self._metadata = {
            "software": {"value": None, "required": True},
            "method": {"value": None, "required": True},
            "input": {"value": None, "required": True},
            "property_keys": {
                "value": {
                    p["property-name"]: None for p in self.property_definitions.values()
                },
            },
        }
        self.properties = {}
        for name in self.property_definitions:
            main_key = MAIN_KEY_MAP[name].key
            self.properties[name] = {main_key: {"field": None, "units": None}}

    def set_metadata_field(self, key: str, value, dynamic=False):
        if dynamic:
            self._metadata[key] = {"field": value}
        else:
            self._metadata[key] = {"value": value}

    def get_metadata(self):
        self.validate_metadata()
        return {
            k: v
            for k, v in self._metadata.items()
            if (v.get("value") or v.get("field"))
        }

    def set_property(
        self,
        property_name: str,
        field: str,
        units: str,
        original_file_key: str,
        additional: list[tuple] = [],
    ):
        if property_name not in self.properties:
            raise KeyError(f"Property not included in PropertyMap: {property_name}")
        self.properties[property_name][MAIN_KEY_MAP[property_name].key] = {
            "field": field,
            "units": units,
        }
        self._metadata["property_keys"]["value"][property_name] = original_file_key
        for add in additional:
            key, value = add
            if key in self.property_definitions[property_name]:
                self.properties[property_name][key] = value
            else:
                raise KeyError(f"Key '{key}' not found in property '{property_name}'")

    def set_properties(self, properties: list[dict | property_info | PropertyInfo]):
        for prop in properties:
            if isinstance(prop, PropertyInfo):
                prop = prop.get_info()
            if isinstance(prop, dict):
                prop_name = prop["property_name"]
                field = prop["field"]
                units = prop["units"]
                original_file_key = prop["original_file_key"]
                additional = prop.get("additional", [])
                self.set_property(
                    prop_name, field, units, original_file_key, additional
                )
            elif isinstance(prop, property_info):
                prop_name = prop.property_name
                field = prop.field
                units = prop.units
                original_file_key = prop.original_file_key
                additional = prop.additional
                if additional is None:
                    additional = []
                self.set_property(
                    prop_name, field, units, original_file_key, additional
                )
        self.validate_properties()

    def get_property(self, property_name: str):
        if property_name not in self.properties:
            raise KeyError(f"Property not included in PropertyMap: {property_name}")
        return self.properties[property_name]

    def validate_metadata(self):
        for key, value in self._metadata.items():
            if (
                value.get("required")
                and value.get("value") is None
                and value.get("field") is None
            ):
                raise ValueError(f"Metadata key '{key}' is required but not set.")
        for prop_name in self.properties.keys():
            if self._metadata["property_keys"]["value"][prop_name] is None:
                raise ValueError(
                    f"Metadata must have 'original_file_key' set for each property. None set for '{prop_name}'."  # noqa E501
                )

    def validate_properties(self):
        for prop_name, prop in self.properties.items():
            main_key = prop.get(MAIN_KEY_MAP[prop_name].key)
            if (
                main_key is None
                or main_key["field"] is None
                or main_key["units"] is None
            ):
                raise ValueError(
                    f"Property '{prop_name}' must have 'field' and 'units' set."
                )
            check_dict = {
                k: v
                for k, v in self.property_definitions[prop_name].items()
                if k
                not in [
                    "property-name",
                    "property-id",
                    "property-title",
                    "property-description",
                ]
            }
            for key, val in check_dict.items():
                if isinstance(prop.get(key), dict):
                    prop_view = prop[key]
                else:
                    prop_view = prop
                if (
                    val.get("required")
                    and prop_view.get("value") is None
                    and prop_view.get("field") is None
                ):
                    raise ValueError(f"Property '{prop_name}' must have '{key}' set.")
                elif not val.get("required") and prop_view.get(key) is None:
                    continue
                elif val.get("has-unit") and prop_view.get("units") is None:
                    raise ValueError(f"Property '{prop_name}' must have 'units' set.")
                elif (
                    val.get("has-unit") is False and prop_view.get("units") is not None
                ):
                    raise ValueError(
                        f"Property '{prop_name}' must have key {key}: 'units' set to None."  # noqa E501
                    )
            if self._metadata["property_keys"]["value"][prop_name] is None:
                raise ValueError(
                    f"Property '{prop_name}' must have 'original_file_key' set."
                )

    def get_property_map(self):
        self.validate_metadata()
        self.validate_properties()
        return {
            "_metadata": self.get_metadata(),
            **{k: v for k, v in self.properties.items()},
        }
