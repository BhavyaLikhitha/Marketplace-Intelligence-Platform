import re

def transform_product_name(value):
    if value is None:
        return None
    
    if not isinstance(value, str):
        try:
            value = str(value)
        except:
            return None
    
    value = value.strip()
    
    if not value:
        return None
    
    # Clean and standardize the product name
    cleaned = value.strip()
    
    # Remove extra whitespace
    cleaned = re.sub(r'\s+', ' ', cleaned)
    
    # Convert to title case for consistency
    # But preserve acronyms and special cases
    words = cleaned.split()
    title_words = []
    
    for word in words:
        if word.isupper() and len(word) > 1:
            # Preserve acronyms like FROSTED FLAKES
            title_words.append(word.title())
        else:
            title_words.append(word.title())
    
    cleaned = ' '.join(title_words)
    
    return cleaned