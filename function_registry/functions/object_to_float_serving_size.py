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
        # Pattern for oz/mL with numbers like "1 oz./30 mL"
        r'(\d+(?:\.\d+)?)\s*(?:oz|fl\.?\s*oz|ounce|fl\.?\s*ounce)[\s/]*(\d+(?:\.\d+)?)\s*(?:mL|ml)',
        # Pattern for weight like "Net Wt. 174g" or "Net WT 1 LB"
        r'net\s+(?:wt|weight)\.?\s*(\d+(?:\.\d+)?)\s*(g|kg|lb|oz|ounce|pound)',
        # Pattern for unit sizes like "Unit Size 1/2 lb." or "2/5#"
        r'(?:unit\s+size|size|item)[\s:]*(\d+(?:\/\d+)?)\s*(lb|oz|g|kg|#)',
        # Pattern for simple weight mentions like "1 LB" or "16 oz"
        r'(\d+(?:\.\d+)?)\s*(lb|oz|g|kg|#)\b',
        # Pattern for fractions like "1/2 lb" or "1/4 lb"
        r'(\d+/\d+)\s*(lb|oz|g|kg|#)',
        # Pattern for combined units like "1 oz./30 mL" - extract first number
        r'(\d+(?:\.\d+)?)\s*(?:oz|fl\.?\s*oz)[\s/]*\d',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, value, re.IGNORECASE)
        if match:
            groups = match.groups()
            if len(groups) >= 2:
                quantity_str = groups[0]
                unit = groups[1].lower()
                
                # Handle fractions
                if '/' in quantity_str:
                    try:
                        parts = quantity_str.split('/')
                        if len(parts) == 2:
                            numerator = float(parts[0])
                            denominator = float(parts[1])
                            if denominator != 0:
                                quantity = numerator / denominator
                            else:
                                continue
                        else:
                            continue
                    except (ValueError, ZeroDivisionError):
                        continue
                else:
                    try:
                        quantity = float(quantity_str)
                    except ValueError:
                        continue
                
                # Convert to ounces (common serving size unit)
                if unit in ['g', 'gram', 'grams']:
                    quantity = quantity * 0.035274  # grams to ounces
                elif unit in ['kg', 'kilogram', 'kilograms']:
                    quantity = quantity * 35.274  # kg to ounces
                elif unit in ['lb', 'pound', 'pounds', '#']:
                    quantity = quantity * 16  # pounds to ounces
                elif unit in ['ml', 'ml']:
                    quantity = quantity * 0.033814  # mL to fluid ounces
                
                # Round to reasonable precision
                quantity = round(quantity, 2)
                return float(quantity)
    
    # Try to find any standalone number that might be a weight
    standalone_patterns = [
        r'\b(\d+(?:\.\d+)?)\s*(?:oz|ounce|lb|pound|g|gram|kg|kilogram|ml)\b',
        r'\b(\d+/\d+)\s*(?:oz|ounce|lb|pound|g|gram)\b',
    ]
    
    for pattern in standalone_patterns:
        match = re.search(pattern, value, re.IGNORECASE)
        if match:
            quantity_str = match.group(1)
            if '/' in quantity_str:
                try:
                    parts = quantity_str.split('/')
                    if len(parts) == 2:
                        numerator = float(parts[0])
                        denominator = float(parts[1])
                        if denominator != 0:
                            return float(numerator / denominator)
                except (ValueError, ZeroDivisionError):
                    continue
            else:
                try:
                    return float(quantity_str)
                except ValueError:
                    continue
    
    return None