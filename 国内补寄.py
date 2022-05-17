import pandas as pd
import csv
import os
import pymysql
from pathlib import Path
from openpyxl import Workbook
import pandas as pd
import re
import numpy as np
import pandas as pd

from database_read import read
from sqlalchemy import create_engine
import re
from datetime import datetime
# origin = pd.read_excel('D:/work/after_sale/总售后2022-04-30.xlsx')
origin = read('after_sales','new_after_salesv2')
origin['订单号'] = origin['订单号'].astype(str)
origin.head()

origin_buji = origin[origin["客服操作"].str.contains('3-5-0')]
origin_buji = origin_buji.reset_index(drop = True)
origin_buji['补寄配件次数'] = 0
origin_buji

def find_all(string, sub):
    start = 0
    pos = []
    while True:
        start = string.find(sub, start)
        if start == -1:
            return pos
        pos.append(start)
        start += len(sub)


inventory_in_buji = pd.DataFrame(
    columns=['id', '登记日期', '更新日期', '登记人', '店铺', '订单号', 'SKU', '序号', '订单状态', '顾客反馈', '客服操作', '操作代码', 'Refund', '备注'])
dengji_buji = pd.DataFrame(
    columns=['id', '登记日期', '更新日期', '登记人', '店铺', '订单号', 'SKU', '序号', '订单状态', '顾客反馈', '客服操作', 'Refund', '备注', '补寄sku数量',
             '补寄配件次数', '数据库补寄数量'])
code = ['3-5-0']
origin_buji.loc[:, '补寄配件次数'] = 0
import time

for i in range(len(origin_buji)):
    code_split_list = origin_buji.loc[i, '客服操作'].split('&')
    #     code_split_list = '3-1-0$ESAN1024542num1&2-4-0'.split('&')
    for j in code:
        num_sum = 0
        for k in range(len(code_split_list)):
               if j in code_split_list[k]:
                if "num" not in code_split_list[k] and "ship" not in code_split_list[k]:
                    num_sum = num_sum + 1
                if len(find_all(code_split_list[k], 'num')) == 1:
                    num = code_split_list[k].index('num')
                    num_n1 = int(code_split_list[k][num + 3:num + 4])
                    num_sum = num_n1
                #                     origin_buji.loc[i,'补寄数量'] = num_n1
                if 'ship' in code_split_list[k]:
                    num_ship = code_split_list[k].index('ship')
                    ship_n = int(code_split_list[k][num_ship + 4:num_ship + 5])
                    num_sum = num_sum + ship_n

        origin_buji.loc[i, '补寄配件次数'] = origin_buji.loc[i, '补寄配件次数'] + num_sum


now = time.strftime("%Y-%m-%d-%H_%M_%S", time.localtime(time.time()))
origin_buji.to_excel(excel_writer="D:/work/origin_buji/国内" + now + r".xlsx", index=False, encoding='utf-8')

# origin_buji_table = origin_buji.pivot_table(index = '订单号',aggfunc = { '补寄配件次数':'sum'} )
# origin_buji_table = origin_buji_table.reset_index()

inventory_buji= pd.read_excel('D:/work/国内补寄/总配件.xlsx')
inventory_buji['订单号'] = inventory_buji['订单号'].astype(str)
inventory_buji.dtypes

inventory_pivot = pd.pivot_table(inventory_buji, values=['SKU'], index=['订单号'], aggfunc='count')
inventory_pivot = inventory_pivot.reset_index()
origin_buji_big = origin_buji
inventory_buji_big =inventory_buji
inventory_3 = inventory_buji_big

import time

# 2022年售后漏登
# dengji_buji1 = pd.DataFrame(columns=['日期','寄件人','小组','店铺','订单号','地址','电话（亚马逊订单上的电话是虚拟号，打不通，一定要问到真实的电话号码）','sku','配件中文名称','追踪号','是否工厂支付运费','重量（KG）','RMB','USD','快递公司','国内补寄/国外补寄','校验','自发货系统SKU'])
# inventory_in_buji1 =  pd.DataFrame(columns=['日期','寄件人','小组','店铺','订单号','地址','电话（亚马逊订单上的电话是虚拟号，打不通，一定要问到真实的电话号码）','sku','配件中文名称','追踪号','是否工厂支付运费','重量（KG）','RMB','USD','快递公司','国内补寄/国外补寄','校验','自发货系统SKU'])
dengji_buji1 = pd.DataFrame(columns=['ID', '日期', '订单号', 'SKU', '详情', '寄件人', '店铺'])
inventory_in_buji1 = pd.DataFrame(columns=['ID', '日期', '订单号', 'SKU', '详情', '寄件人', '店铺'])
for i in range(len(inventory_3)):
    if (str(inventory_3.iloc[i, 2]) in origin_buji_big['订单号'].tolist()):
        # if inventory_3.iloc[i, 2] == '7863636525433':
        #     print('asdfdhghk')
        origin_output1 = pd.DataFrame(inventory_3[inventory_3['订单号'] == inventory_3.iloc[i, 2]])
        dengji_buji1 = dengji_buji1.append(origin_output1)
    else:
        print(i)
        origin_output = pd.DataFrame(inventory_3[inventory_3['订单号'] == inventory_3.iloc[i, 2]])
        inventory_in_buji1 = inventory_in_buji1.append(origin_output)

now = time.strftime("%Y-%m-%d-%H_%M_%S",time.localtime(time.time()))
dengji_buji1 = dengji_buji1[~dengji_buji1['订单状态'].isin(['已校对-P'])]
dengji_buji1 = dengji_buji1[~dengji_buji1['订单状态'].isin(['已校对-check'])]

inventory_in_buji1 = inventory_in_buji1[~inventory_in_buji1['订单状态'].isin(['已校对-P'])]
inventory_in_buji1 = inventory_in_buji1[~inventory_in_buji1['订单状态'].isin(['已校对-check'])]

dengji_buji1.to_excel(excel_writer="D:/work/国内补寄/国内补寄校对/py"+now+r"补寄清单售后已登记.xlsx", index=False,encoding='utf-8')
inventory_in_buji1.to_excel(excel_writer="D:/work/国内补寄/国内补寄校对/py"+now+r"补寄清单售后漏登记.xlsx", index=False,encoding='utf-8')

#和输出excel是同样的结果
loudeng = inventory_3[~inventory_3['订单号'].astype(str).isin(origin_buji_big['订单号'])]
loudeng
# loudeng.to_excel(excel_writer="D:/work/国内补寄/国内补寄校对/"+now+r"售后漏登记.xlsx", index=False,encoding='utf-8')

