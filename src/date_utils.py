import re

def format_date(date_str):
    if not date_str or not isinstance(date_str, str):
        return ""
    
    # Extact MM-DD from possible formats like "03-16(周一)" or "03-16"
    match = re.search(r'(\d{2})-(\d{2})', date_str)
    if not match:
        return date_str
        
    month = match.group(1)
    day = match.group(2)
    
    m_int = int(month)
    # Target condition: 1-3 months are year 2026, other months are year 2025
    if 1 <= m_int <= 3:
        year = "2026"
    else:
        year = "2025"
        
    return f"{year}-{month}-{day}"
