def transform_serving_size(value):
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return None
    try:
        result = float(value)
        return result
    except (ValueError, TypeError):
        return None
    except Exception:
        return None