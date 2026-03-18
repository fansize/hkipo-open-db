import sqlite3
import os
import sys
from loguru import logger

# Ensure the database directory exists
DB_DIR = 'db'
DB_FILE = os.path.join(DB_DIR, 'ipo_data.db')

def add_mock_data(code):
    """Adds or updates a mock IPO data record with the given stock code."""
    # Mock data with some default values to fill the 22 required fields
    item = {
        'code': code,
        'name': f'测试股票 {code}',
        'board': '主板',
        'sub_start': '2026-03-01',
        'sub_end': '2026-03-05',
        'list_date': '2026-03-15',
        'price_range': '10.0-12.0',
        'lot_amount': '2400',
        'lot_win_rate': '10%',
        'issue_price': '11.0',
        'issue_pe_ratio': '15.5',
        'greenshoe_public_offer': '有/20倍',
        'comparable_companies': '公司A, 公司B',
        'over_sub_multiple': '50',
        'total_fund_raising': '5.5',
        'issue_market_cap': '25.0',
        'livermore_dark_pool': '+5%',
        'futu_dark_pool': '+6%',
        'first_day_change': '+10%',
        'total_change': '+15%',
        'underwriter': '测试银行',
        'prospectus': 'http://example.com/prospectus.pdf'
    }
    
    try:
        if not os.path.exists(DB_DIR):
            os.makedirs(DB_DIR)
            
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        
        # Ensure table exists (though it should be handled by save_db.py)
        # We use the same schema as save_db.py
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS hk_ipo_data (
            code TEXT PRIMARY KEY,
            name TEXT, board TEXT, sub_start TEXT, sub_end TEXT, list_date TEXT,
            price_range TEXT, lot_amount TEXT, lot_win_rate TEXT, issue_price TEXT,
            issue_pe_ratio TEXT, greenshoe_public_offer TEXT, comparable_companies TEXT,
            over_sub_multiple TEXT, total_fund_raising TEXT, issue_market_cap TEXT,
            livermore_dark_pool TEXT, futu_dark_pool TEXT, first_day_change TEXT,
            total_change TEXT, underwriter TEXT, prospectus TEXT
        )
        ''')
        
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
        ''', tuple(item.values()))
        
        conn.commit()
        logger.info(f"Successfully added/updated mock data for code: {code}")
    except Exception as e:
        logger.error(f"Failed to add mock data: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

def delete_data(code):
    """Deletes a record from the database by its stock code."""
    try:
        if not os.path.exists(DB_FILE):
            logger.warning(f"Database file {DB_FILE} does not exist.")
            return

        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM hk_ipo_data WHERE code = ?", (code,))
        if cursor.rowcount > 0:
            logger.info(f"Successfully deleted data for code: {code}")
        else:
            logger.warning(f"No data found for code: {code}")
        conn.commit()
    except Exception as e:
        logger.error(f"Failed to delete data: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

def main():
    print("\n" + "="*30)
    print("IPO Mock 数据管理工具")
    print("="*30)
    
    while True:
        print("\n请选择操作:")
        print("1. 添加/更新测试数据 (Add/Update Mock Data)")
        print("2. 删除指定股票数据 (Delete Mock Data)")
        print("3. 退出 (Exit)")
        
        try:
            choice = input("\n请输入选项 (1-3): ").strip()
            
            if choice == '1':
                code = input("请输入要添加的股票代码: ").strip()
                if code:
                    add_mock_data(code)
                else:
                    print("错误: 股票代码不能为空。")
            elif choice == '2':
                code = input("请输入要删除的股票代码: ").strip()
                if code:
                    delete_data(code)
                else:
                    print("错误: 股票代码不能为空。")
            elif choice == '3':
                print("退出程序。")
                break
            else:
                print("错误: 无效的选择，请输入 1, 2 或 3。")
        except EOFError:
            break
        except KeyboardInterrupt:
            print("\n程序终止。")
            break

if __name__ == "__main__":
    main()
