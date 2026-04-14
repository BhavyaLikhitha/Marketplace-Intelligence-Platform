def transform_category(value):
    if value is None:
        return None
    
    if isinstance(value, str):
        value = value.strip()
        if value == "":
            return None
    
    try:
        if not isinstance(value, str):
            value = str(value)
        
        value = value.strip()
        
        if value == "":
            return None
        
        return value
        
    except Exception:
        return None