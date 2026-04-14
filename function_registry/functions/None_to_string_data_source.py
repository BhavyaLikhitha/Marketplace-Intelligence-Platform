def transform_data_source(value):
    if value is None:
        return None
    if not isinstance(value, str):
        return None
    if value.strip() == "":
        return None
    return str(value).strip()