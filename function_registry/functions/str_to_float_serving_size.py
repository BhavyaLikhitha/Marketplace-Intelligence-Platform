import re

def transform_serving_size(value):
    if value is None:
        return None
    if not isinstance(value, str):
        return None
    value = value.strip()
    if not value:
        return None
    
    patterns = [
        # Pattern for "1 oz./30 mL" format
        r'(\d+(?:\.\d+)?)\s*oz\.?\s*/\s*(\d+(?:\.\d+)?)\s*mL',
        # Pattern for "1 oz" or "1oz" or "1.5 oz"
        r'(\d+(?:\.\d+)?)\s*oz',
        # Pattern for "30 mL" or "30mL"
        r'(\d+(?:\.\d+)?)\s*mL',
        # Pattern for "1 LB" or "1LB" or "1.5 LB"
        r'(\d+(?:\.\d+)?)\s*lb',
        # Pattern for "16 oz" in parentheses
        r'\(.*?(\d+(?:\.\d+)?)\s*oz.*?\)',
        # Pattern for standalone numbers that might be serving sizes
        r'\b(\d+(?:\.\d+)?)\s*(?:serving|portion|size)\b',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, value, re.IGNORECASE)
        if match:
            try:
                # Extract the first numeric group
                num_str = match.group(1)
                num = float(num_str)
                
                # Convert to ounces if needed
                if 'lb' in match.group(0).lower():
                    num *= 16  # Convert pounds to ounces
                elif 'ml' in match.group(0).lower():
                    num /= 29.5735  # Convert mL to ounces (approximate)
                
                return num
            except (ValueError, IndexError, AttributeError):
                continue
    
    return None