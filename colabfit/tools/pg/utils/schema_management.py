def _empty_dict_from_schema(schema):
    return {field.name: None for field in schema.columns}
