import os
import json
from datetime import datetime
import akshare as ak

today = str(datetime.now())[:10]
stock_zh_a_spot_em_df = ak.stock_zh_a_spot_em()


obj = json.loads(stock_zh_a_spot_em_df.to_json(orient='records'))
output = json.dumps(obj, indent=4, ensure_ascii=False)
root = 'market_daily'
os.makedirs(root, exist_ok=True)
output_path = os.path.join(root, today + '.json')
with open(output_path, 'w') as fp:
    fp.write(output)