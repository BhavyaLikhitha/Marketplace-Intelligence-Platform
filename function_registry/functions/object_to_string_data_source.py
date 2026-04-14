def transform_data_source(value):
    if value is None:
        return None
    if isinstance(value, str):
        if value.strip() == "":
            return None
        return value.strip()
    try:
        return str(value).strip()
    except Exception:
        return None