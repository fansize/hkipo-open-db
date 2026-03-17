from src.save_db import init_db, save_ipo_data
from src.jina_scraper import fetch_data, parse_data
from loguru import logger

def main():
    logger.info("Starting HK IPO data scraping task...")
    
    # 1. Initialize SQLite database
    init_db()
    
    # 2. Fetch data from website via Jina AI
    try:
        raw_text = fetch_data()
    except Exception as e:
        logger.error(f"Failed to fetch data: {e}")
        return
        
    # 3. Parse JSON from the markdown output
    parsed_list = parse_data(raw_text)
    
    if not parsed_list:
        logger.warning("No data parsed. Stopping process.")
        return
        
    # 4. Save parsed entries to the database
    saved_count = save_ipo_data(parsed_list)
    logger.info(f"Task finished! Successfully updated {saved_count} HK IPO records.")

if __name__ == "__main__":
    main()
