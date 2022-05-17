import datetime
import re
from sqlalchemy import create_engine
import pymysql
import pandas as pd
from database_read import read, creat_id
from sqlalchemy.types import VARCHAR, Float, Integer, Date, Numeric, Text

import datetime
import re
from sqlalchemy import create_engine
import pymysql
import pandas as pd
from dateutil.parser import parse
from database_read import read, creat_id
from sqlalchemy.types import VARCHAR, Float, Integer, Date, Numeric, Text

origin = pd.read_excel('D:/work/匹配表.xlsx')
origin = origin[['公司SKU', '平台', '平台SKU']]
index = origin[['平台', '平台SKU']]
index = index.drop_duplicates(keep='first')
index = index.reset_index(drop=True)  # 重建索引
for i in range(len(index)):
    print('%s-%s' % (i, index))
    sku_index = origin[(origin['平台'] == index.loc[i, '平台']) & (origin['平台SKU'] == index.loc[i, '平台SKU'])].index.tolist()
    origin.loc[sku_index, '公司SKU'] = origin.loc[sku_index, '公司SKU'].drop_duplicates().str.cat(sep='|')
origin.to_excel(excel_writer="D:\work\渠道sku.xlsx",
                index=False,
                encoding='utf-8')
print(1)
