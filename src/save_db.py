import sqlite3
import os
from loguru import logger

DB_FILE = 'db/ipo_data.db'

def init_db():
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        
        # Create table if it doesn't exist according to the 22 required fields
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS hk_ipo_data (
            code TEXT PRIMARY KEY, -- 代码
            name TEXT, -- 名称
            board TEXT, -- 上市板块
            sub_start TEXT, -- 申购起始
            sub_end TEXT, -- 申购截止
            list_date TEXT, -- 上市日
            price_range TEXT, -- 询价区间 （港元）
            lot_amount TEXT, -- 一手资金 （港元）
            lot_win_rate TEXT, -- 一手中签率
            issue_price TEXT, -- 发行价 （港元）
            issue_pe_ratio TEXT, -- 发行市盈率
            greenshoe_public_offer TEXT, -- 缲鞋保护 /公开发售
            comparable_companies TEXT, -- 可比公司
            over_sub_multiple TEXT, -- 超购倍数
            total_fund_raising TEXT, -- 募资总额 （亿港元）
            issue_market_cap TEXT, -- 发行时市值 （亿港元）
            livermore_dark_pool TEXT, -- 利弗莫尔暗盘涨蝠
            futu_dark_pool TEXT, -- 富途暗盘涨幅
            first_day_change TEXT, -- 首日涨幅
            total_change TEXT, -- 累计涨幅
            underwriter TEXT, -- 承销商
            prospectus TEXT -- 招股说明书
        )
        ''')
        
        conn.commit()
        logger.info(f"Database initialized successfully at {DB_FILE}")
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        raise
    finally:
        if 'conn' in locals() and conn:
            conn.close()

def save_ipo_data(data_list):
    if not data_list:
        logger.warning("No data provided to save.")
        return 0
        
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        
        count = 0
        for item in data_list:
            # Use UPSERT to only insert new rows or update changed fields for existing ones
            cursor.execute('''
            INSERT INTO hk_ipo_data (
                code, name, board, sub_start, sub_end, list_date, price_range,
                lot_amount, lot_win_rate, issue_price, issue_pe_ratio, greenshoe_public_offer,
                comparable_companies, over_sub_multiple, total_fund_raising, issue_market_cap,
                livermore_dark_pool, futu_dark_pool, first_day_change, total_change, underwriter, prospectus
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(code) DO UPDATE SET
                name=excluded.name,
                board=excluded.board,
                sub_start=excluded.sub_start,
                sub_end=excluded.sub_end,
                list_date=excluded.list_date,
                price_range=excluded.price_range,
                lot_amount=excluded.lot_amount,
                lot_win_rate=excluded.lot_win_rate,
                issue_price=excluded.issue_price,
                issue_pe_ratio=excluded.issue_pe_ratio,
                greenshoe_public_offer=excluded.greenshoe_public_offer,
                comparable_companies=excluded.comparable_companies,
                over_sub_multiple=excluded.over_sub_multiple,
                total_fund_raising=excluded.total_fund_raising,
                issue_market_cap=excluded.issue_market_cap,
                livermore_dark_pool=excluded.livermore_dark_pool,
                futu_dark_pool=excluded.futu_dark_pool,
                first_day_change=excluded.first_day_change,
                total_change=excluded.total_change,
                underwriter=excluded.underwriter,
                prospectus=excluded.prospectus
            ''', (
                item.get('code', ''),
                item.get('name', ''),
                item.get('board', ''),
                item.get('sub_start', ''),
                item.get('sub_end', ''),
                item.get('list_date', ''),
                item.get('price_range', ''),
                item.get('lot_amount', ''),
                item.get('lot_win_rate', ''),
                item.get('issue_price', ''),
                item.get('issue_pe_ratio', ''),
                item.get('greenshoe_public_offer', ''),
                item.get('comparable_companies', ''),
                item.get('over_sub_multiple', ''),
                item.get('total_fund_raising', ''),
                item.get('issue_market_cap', ''),
                item.get('livermore_dark_pool', ''),
                item.get('futu_dark_pool', ''),
                item.get('first_day_change', ''),
                item.get('total_change', ''),
                item.get('underwriter', ''),
                item.get('prospectus', '')
            ))
            count += 1
            
        conn.commit()
        logger.info(f"Successfully saved {count} records to database. Total incoming rows: {len(data_list)}")
        return count
    except Exception as e:
        logger.error(f"Failed to save data: {e}")
        raise
    finally:
        if 'conn' in locals() and conn:
            conn.close()
