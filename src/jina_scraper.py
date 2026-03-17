import os
import json
import re
import requests
from dotenv import load_dotenv
from loguru import logger
from src.date_utils import format_date

load_dotenv()

JINA_URL = "https://r.jina.ai/https://www.jisilu.cn/data/new_stock/hkipo/"

def fetch_data():
    jina_key = os.getenv('JINA_KEY')
    headers = {
        "X-Return-Format": "markdown"
    }
    if jina_key:
        headers["Authorization"] = f"Bearer {jina_key}"

    logger.info(f"Fetching data from Jina URL: {JINA_URL}")
    response = requests.get(JINA_URL, headers=headers)
    response.raise_for_status()
    
    md_path = os.path.join('db', 'jina_hkipo_output.md')
    with open(md_path, 'w', encoding='utf-8') as f:
        f.write(response.text)
    logger.info(f"Intermediate markdown data saved to {md_path}")
        
    return response.text

def parse_data(raw_text):
    """
    Parse the Jina markdown response, which actually contains the JSON data for the HK IPO table.
    """
    # Look for the JSON payload in the text
    match = re.search(r'(\{.*"page":.*\})', raw_text, re.DOTALL)
    if not match:
        logger.error("Could not find JSON payload in the response.")
        return []
    
    json_str = match.group(1)
    
    # In Jina markdown, raw brackets and slashes are sometimes escaped, like <\/>
    # and some links or extra text could be appended.
    # We will try to clean up Markdown links that got appended, e.g. `](https://...)`
    cleaned_str = re.sub(r'\]\(https://[^)]+\)', '', json_str)
    
    try:
        data = json.loads(cleaned_str)
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse JSON: {e}")
        # Let's save the faulty string for debugging if it fails
        with open("faulty_json.log", "w", encoding="utf-8") as f:
            f.write(cleaned_str)
        return []

    rows = data.get("rows", [])
    parsed_items = []
    
    for row in rows:
        cell = row.get("cell", {})
        
        # Mapping rules based on JSON keys to DB schema
        item = {
            'code': cell.get('stock_cd'),
            'name': cell.get('stock_nm'),
            'board': cell.get('market'),
            'sub_start': format_date(cell.get('apply_dt')),
            'sub_end': format_date(cell.get('apply_end_dt')),
            'list_date': format_date(cell.get('list_dt')),
            'price_range': cell.get('price_range'),
            'lot_amount': cell.get('single_draw_money'),
            'lot_win_rate': cell.get('lucky_draw_rt'),
            'issue_price': cell.get('issue_price'),
            'issue_pe_ratio': cell.get('issue_pe_range'),
            'greenshoe_public_offer': str(cell.get('green_rt', '')) + " " + str(cell.get('green_amount', '')),
            'comparable_companies': cell.get('ref_company'),
            'over_sub_multiple': cell.get('above_rt'),
            'total_fund_raising': cell.get('raise_money'),
            'issue_market_cap': cell.get('total_values'),
            'livermore_dark_pool': cell.get('gray_incr_rt'),
            'futu_dark_pool': cell.get('gray_incr_rt2'),
            'first_day_change': cell.get('first_incr_rt'),
            'total_change': cell.get('total_incr_rt'),
            'underwriter': cell.get('underwriter'),
            'prospectus': cell.get('prospectus')
        }
        
        # Cleanup "None" string if some was null
        for k, v in item.items():
            if v is None or v == "None" or v == " None":
                item[k] = ""
                
        parsed_items.append(item)
        
    logger.info(f"Successfully parsed {len(parsed_items)} items from JSON.")
    return parsed_items
