"""Transform various date formats to ISO YYYY-MM-DD string."""

from datetime import datetime


def transform_published_date(value):
    if value is None:
        return None

    if isinstance(value, str):
        value = value.strip()
        if value == "":
            return None

    original = str(value).strip()

    # Try multiple date formats
    formats = [
        "%Y%m%d",  # YYYYMMDD (e.g., 20160808)
        "%Y-%m-%d",  # ISO format (e.g., 2016-08-08)
        "%m/%d/%Y",  # US format (e.g., 08/08/2016)
        "%d/%m/%Y",  # EU format (e.g., 08/08/2016)
        "%Y/%m/%d",  # Slash ISO (e.g., 2016/08/08)
        "%b %d, %Y",  # Aug 8, 2016
        "%B %d, %Y",  # August 8, 2016
        "%d-%b-%Y",  # 08-Aug-2016
        "%m-%d-%Y",  # 08-08-2016
        "%d.%m.%Y",  # European (e.g., 08.08.2016)
    ]

    for fmt in formats:
        try:
            dt = datetime.strptime(original, fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue

    # If value is an integer (like 20160808), try direct parsing
    if isinstance(value, int) or (isinstance(value, str) and value.isdigit()):
        try:
            str_val = str(value).strip()
            if len(str_val) == 8:
                dt = datetime.strptime(str_val, "%Y%m%d")
                return dt.strftime("%Y-%m-%d")
        except ValueError:
            pass

    # Try pandas-style parsing as last resort
    try:
        dt = datetime.fromisoformat(original.replace("/", "-"))
        return dt.strftime("%Y-%m-%d")
    except (ValueError, AttributeError):
        pass

    # If all parsing fails, preserve original value as-is (don't return None)
    return original
