def transform_category(value):
    if value is None:
        return None
    if not isinstance(value, str):
        return None
    value = value.strip()
    if not value:
        return None
    try:
        return str(value)
    except Exception:
        return None