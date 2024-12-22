import json
import os
from datetime import datetime

import akshare as ak
import pandas as pd 


# Constants
DATA_DIR = {
    'market': 'market_daily',
    'stock': 'stocks_daily'
}

def get_market_data() -> pd.DataFrame:
    """Fetch market data from AKShare API
    
    Returns:
        pd.DataFrame: Current day market data for China A-Share stocks
    """
    return ak.stock_zh_a_spot_em()

def handle_market_daily(df: pd.DataFrame) -> None:
    """Process and save daily market data
    
    Args:
        df (pd.DataFrame): Market data for all stocks
    """
    if len(df) <= 100:
        return

    today = str(datetime.now())[:10]
    year = today[:4]
    obj = json.loads(df.to_json(orient='records'))
    output = json.dumps(obj, indent=4, ensure_ascii=False)
    
    year_dir = os.path.join(DATA_DIR['market'], year)
    os.makedirs(year_dir, exist_ok=True)
    output_path = os.path.join(year_dir, f'{today}.json')
    with open(output_path, 'w') as fp:
        fp.write(output)

def handle_stock_daily(df: pd.DataFrame) -> None:
    """Process and save daily stock data for individual stocks
    
    Args:
        df (pd.DataFrame): Market data containing stock codes
    """
    codes = [code for code in df['代码'] if int(code) % 10 == 0]
    os.makedirs(DATA_DIR['stock'], exist_ok=True)

    for i, code in enumerate(codes, 1):
        print(f'Processing {i}/{len(codes)} {code}')
        try:
            stock_hist_df = ak.stock_zh_a_hist(
                symbol=code, 
                period="daily", 
                adjust="qfq"
            )
            output_path = os.path.join(DATA_DIR['stock'], f'{code}.csv')
            stock_hist_df.to_csv(output_path, index=False)
        except Exception as e:
            print(f'Failed to process stock {code}: {e}')


def main():
    """主程序入口"""
    market_data = get_market_data()
    handle_market_daily(market_data)
    handle_stock_daily(market_data)


if __name__ == "__main__":
    main()