import os
import json
import re
import time
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import requests
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

JINA_URL = "https://r.jina.ai/https://www.jisilu.cn/data/new_stock/hkipo/"
REQUEST_TIMEOUT = (10, 30)
MAX_RETRIES = 3


def fetch_data(debug_output_path=None):
    jina_key = os.getenv('JINA_KEY')
    headers = {
        "X-Return-Format": "markdown"
    }
    if jina_key:
        headers["Authorization"] = f"Bearer {jina_key}"

    logger.info(f"Fetching data from Jina URL: {JINA_URL}")
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(JINA_URL, headers=headers, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()

            if debug_output_path:
                debug_file = Path(debug_output_path)
                debug_file.parent.mkdir(parents=True, exist_ok=True)
                debug_file.write_text(response.text, encoding='utf-8')
                logger.info(f"Intermediate markdown data saved to {debug_file}")

            return response.text
        except requests.RequestException as exc:
            if attempt == MAX_RETRIES:
                raise RuntimeError(f"Failed to fetch data after {MAX_RETRIES} attempts") from exc

            sleep_seconds = attempt * 2
            logger.warning(
                f"Fetch attempt {attempt}/{MAX_RETRIES} failed: {exc}. Retrying in {sleep_seconds} seconds..."
            )
            time.sleep(sleep_seconds)

def parse_data(raw_text):
    """
    Parse the Jina markdown response, which actually contains the JSON data for the HK IPO table.
    """
    # Look for the JSON payload in the text
    match = re.search(r'(\{.*"page":.*\})', raw_text, re.DOTALL)
    if not match:
        raise ValueError("Could not find JSON payload in the response.")
    
    json_str = match.group(1)
    
    # In Jina markdown, raw brackets and slashes are sometimes escaped, like <\/>
    # and some links or extra text could be appended.
    # We will try to clean up Markdown links that got appended, e.g. `](https://...)`
    cleaned_str = re.sub(r'\]\(https://[^)]+\)', '', json_str)
    
    try:
        data = json.loads(cleaned_str)
    except json.JSONDecodeError as e:
        raise ValueError(f"Failed to parse JSON: {e}") from e

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


# 格式化日期
def format_date(date_str, today=None):
    if not date_str or not isinstance(date_str, str):
        return ""
    
    # Extact MM-DD from possible formats like "03-16(周一)" or "03-16"
    match = re.search(r'(\d{2})-(\d{2})', date_str)
    if not match:
        return date_str
        
    month = match.group(1)
    day = match.group(2)

    month_int = int(month)
    day_int = int(day)
    today = today or datetime.now(ZoneInfo("Asia/Shanghai")).date()

    candidates = []
    for year in (today.year - 1, today.year, today.year + 1):
        try:
            candidates.append(date(year, month_int, day_int))
        except ValueError:
            continue

    if not candidates:
        return date_str

    best_match = min(candidates, key=lambda candidate: abs((candidate - today).days))
    return best_match.isoformat()
