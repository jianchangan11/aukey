import datetime
# 自己写的方法
import pandas as pd
import numpy as np
import time
from dateutil.parser import parse
from new_asin import new_asin
from origin_to_sql import origin_to_sql, origin_sql_total
from origin_to_skusale import origin_to_skusale
from check import check
from sale_to_sql import sale_sql_temp, sale_sql_total
from dataprocess import read
import pandas as pd
from database_read import read
from sqlalchemy import create_engine
import re
originzifa = pd.DataFrame()
for x in range(1, 14):
    origin_zi = pd.read_table(r'D:/work/自发货订单管理源数据/系统数据/新数据_周/%s.txt' % x)
#     print(origin_zi)
    origin_zifa = pd.DataFrame(origin_zi, columns=['公司SKU', '数量', '订单类型', '发货时间', '账号', '订单号', '包裹号'])
    originzifa = originzifa.append(origin_zifa)
    originzifa = originzifa.reset_index(drop=True)
originzifa = originzifa.rename(columns={'订单号': '平台订单号'})
print('新系统完成')

originzifa2021 = pd.DataFrame()
for x in range(1, 27):
    origin_2021 = pd.read_csv(r'D:/work/自发货订单管理源数据/系统数据/旧数据/旧%s.csv' % x)
#     print(origin_2021)
    origin_zifa2021 = pd.DataFrame(origin_2021, columns=['sku', '数量', '订单类型', '发货时间', '账号', '订单号', '包裹号'])
    originzifa2021 = originzifa2021.append(origin_zifa2021)
    originzifa2021 = originzifa2021.reset_index(drop=True)
# print(originzifa2021)
print('旧系统完成')
originzifa2021 = originzifa2021.rename(columns={'sku': '公司SKU', '订单号': '平台订单号'})
originzifa = pd.concat([originzifa, originzifa2021], axis=0)
originzifa =originzifa.drop_duplicates()
originzifa = originzifa.reset_index(drop=True)

print('拼接完成')

fanyistr = {
    "yourui Walmart US": 'walmart优瑞斯特',
    "heman Walmart US": 'walmart赫曼',
    "xinhemaoyi Walmart US": 'walmart信盒',
    "gongben Walmart US": 'walmart宫本',
    "xinhemaoyi wayfair": 'wayfair信盒',
    "weilu wayfair": 'wayfair维禄',
    "zhirun2021": 'ebay治润',
    "yaqinsale11": 'ebay雅秦',
    "linglang2021": 'ebay玲琅',
    "linglanglv21": 'ebay玲琅',
    "简砾": 'amazon简砾',
    "shangming": 'amazon尚铭',
    "jianli": 'amazon简砾',
    "heman": 'amazon赫曼',
    "赫曼": 'amazon赫曼',
    "Youruisite": 'amazon优瑞斯特',
    "信盒贸易": 'amazon信盒',
    "玲琅": 'amazon玲琅',
    "Central Power International Limited": 'amazoncpower',
    "维禄": 'amazon维禄',
    "宫本": 'amazon宫本',
    "xinhemaoyi": 'amazon信盒',
    "优瑞斯特": 'amazon优瑞斯特',
    "森月": 'amazon森月',
    "驰甬": 'amazon驰甬',
    "尚铭": 'amazon尚铭',
    "damaiwang": 'amazon哒唛旺',
    "哒唛旺": 'amazon哒唛旺',
    "bulubulu": 'amazon卟噜卟噜',
    "治润": 'amazon治润',
    "启珊": 'amazon启珊',
    "卟噜卟噜": 'amazon卟噜卟噜',
    "旗辰": 'amazon旗辰',
    "senyue": 'amazon森月',
    "NEXTFUR LLC": 'shopifynextfur',
    "赛迦曼": 'amazon赛迦曼',
    "gongben": 'amazon宫本',
    "weilu": 'amazon维禄',
    "NEXTFUR": 'shopifynextfur',
}


def tran(my_str, rep):
    rep = dict((re.escape(k), v) for k, v in rep.items())
    pattern = re.compile("|".join(rep.keys()))
    my_str = pattern.sub(lambda m: rep[re.escape(m.group(0))], my_str)
    return my_str

# 翻译
originzifa['账号'] = originzifa['账号'].apply(lambda x: tran(x, fanyistr))
#之所以加平台是为了优化平台订单号 你可以考虑把平台和账号属性合二为一 节约储存空间
#这是一个很重要的坑 早期的订单号很混乱（旧系统） 同一单下有多个产品时
#比如第一单会写原订单号
#第二单会写-1 第三单有时候写-1-1 有时候写-2
#还原为原订单号 方便查找检索
#这个我放在第二个文件做了 你可以在这一步统一做好 还原订单号再存源数据
originzifa['平台'] = 0
originzifa.loc[(originzifa['账号'].str.contains('amazon')), '平台'] = 'amazon'
originzifa.loc[(originzifa['账号'].str.contains('wayfair')), '平台'] = 'wayfair'
originzifa.loc[(originzifa['账号'].str.contains('ebay')), '平台'] = 'ebay'
originzifa.loc[(originzifa['账号'].str.contains('walmart')), '平台'] = 'walmart'
originzifa.loc[(originzifa['账号'].str.contains('shopify')), '平台'] = 'shopify'

# 量太大 先存一下
# originzifa.to_sql("自发货源数据", connect2, if_exists='replace', index=False)
#
# #对刚刚处理好的数据进行订单号优化 每个平台订单号的位数都是限定的
# originzifa = read('inventory', '自发货源数据')
# print(originzifa)
originzifa_wayfair = originzifa[((originzifa['平台']) == 'wayfair')]
originzifa_wayfair = originzifa_wayfair.reset_index(drop=True)
originzifa_wayfair['平台订单号'] = originzifa_wayfair['平台订单号'].str[0:11]

originzifa_walmart = originzifa[((originzifa['平台']) == 'walmart')]
originzifa_walmart = originzifa_walmart.reset_index(drop=True)
originzifa_walmart['平台订单号'] = originzifa_walmart['平台订单号'].str[0:13]

originzifa_shopify = originzifa[((originzifa['平台']) == 'shopify')]
originzifa_shopify = originzifa_shopify.reset_index(drop=True)
originzifa_shopify['平台订单号'] = originzifa_shopify['平台订单号'].str[0:13]

originzifa_amazon = originzifa[((originzifa['平台']) == 'amazon')]
originzifa_amazon = originzifa_amazon.reset_index(drop=True)
originzifa_amazon['平台订单号'] = originzifa_amazon['平台订单号'].str[0:19]


originzifa_ebay = originzifa[((originzifa['平台']) == 'ebay')]
originzifa_ebay = originzifa_ebay.reset_index(drop=True)
originzifa_ebay['平台订单号'] = originzifa_ebay['平台订单号'].str[0:14]

newzifa = pd.concat([originzifa_amazon, originzifa_ebay, originzifa_wayfair, originzifa_walmart, originzifa_shopify])
newzifa = newzifa.reset_index(drop=True)
# print(newzifa.head(2))

print('处理订单号完成')

#这个时间主要是方便提供查询 大致知道这一单是什么时候的 大致知道这一单最早发货和最后售后差了多久 也可以考虑是否优化
#这个看了订单号和SKU
origintime = pd.DataFrame(newzifa, columns=['平台订单号', '公司SKU', '发货时间'])
origintime = origintime.sort_values(by=['发货时间'])
origintime = origintime.reset_index(drop=True)
origintime1 = origintime.drop_duplicates(subset=['平台订单号', '公司SKU'], keep='first')
origintime1 = origintime1.reset_index(drop=True)
origintime2 = origintime.drop_duplicates(subset=['平台订单号', '公司SKU'], keep='last')
origintime2 = origintime2.reset_index(drop=True)
origintime1 = origintime1.rename(columns={'发货时间': '最早时间'})
origintime2 = origintime2.rename(columns={'发货时间': '最晚时间'})
or_time = pd.merge(origintime1, origintime2, how='outer', on=['平台订单号', '公司SKU'])
or_time = or_time.reset_index(drop=True)

#这个是只看同一单的时间跨度（我觉得这个和上一个可以优化）
origintimex = pd.DataFrame(newzifa, columns=['平台订单号', '发货时间'])
origintimex = origintimex.sort_values(by=['发货时间'])
origintimex = origintimex.reset_index(drop=True)
origintimex1 = origintimex.drop_duplicates(subset=['平台订单号'], keep='first')
origintimex1 = origintimex1.reset_index(drop=True)
origintimex2 = origintimex.drop_duplicates(subset=['平台订单号'], keep='last')
origintimex2 = origintimex2.reset_index(drop=True)
origintimex1 = origintimex1.rename(columns={'发货时间': '最早时间'})
origintimex2 = origintimex2.rename(columns={'发货时间': '最晚时间'})
or_timex = pd.merge(origintimex1, origintimex2, how='outer', on=['平台订单号'])
origintimex = origintimex.drop_duplicates(subset=['平台订单号'])
or_timex = or_timex.reset_index(drop=True)

# 分两类 正常发货清单（正常单和手工单）和补寄清单
origin_zifa_a = newzifa[((newzifa['订单类型']) != '补寄订单')]
origin_zifa_a = origin_zifa_a.reset_index(drop=True)
origin_zifa_b = newzifa[((newzifa['订单类型']) == '补寄订单')]
origin_zifa_b = origin_zifa_b.reset_index(drop=True)
# 正常清单数量整理
origin_zifaout = pd.pivot_table(origin_zifa_a, values=['数量'], index=['平台订单号', '账号', '公司SKU'], aggfunc='sum')
origin_zifaout = origin_zifaout.reset_index()
# print(origin_zifaout.head(2))
# 补寄清单数量整理
buji_zifaout = pd.pivot_table(origin_zifa_b, values=['数量'], index=['平台订单号', '账号', '公司SKU'], aggfunc='sum')
buji_zifaout = buji_zifaout.reset_index()
buji_zifaout = buji_zifaout.rename(columns={'数量': '补寄数量'})
# print(buji_zifaout.head(2))

#以订单号SKU为维度的简单表
originlast = pd.merge(origin_zifaout, buji_zifaout, how='outer', on=['平台订单号', '账号', '公司SKU'])
originlast = originlast.merge(or_time, how='left', on=['平台订单号', '公司SKU'])
# originlast.to_sql("自发货数据", connect1, if_exists='replace', index=False)
originlast.head()

origin = read('after_sales','new_after_salesv2')
inventory = originlast.copy()
inventory1 = inventory.copy()

import math
for i in range(len(inventory1)):
    if math.isnan(inventory1.loc[i,'数量']) and not math.isnan(inventory1.loc[i,'补寄数量']):
        inventory1.loc[i,'数量'] = inventory1.loc[i,'补寄数量']

AdDate = datetime.date(2020,1, 1)
inventory1['最早时间'] = pd.to_datetime(inventory1['最早时间'])
inventory1 = inventory1[inventory1['最早时间'] >= datetime.datetime.combine(AdDate, datetime.datetime.min.time())]
inventory1 = inventory1.pivot_table(index = ['平台订单号', '公司SKU'], aggfunc='sum')
inventory1 = inventory1.reset_index()

inventory_id = inventory.pivot_table(index = ['平台订单号','账号'],values =['数量'], aggfunc='count')
inventory_id= inventory_id.reset_index()
origin_id = origin.pivot_table(index=['订单号', '店铺'], values=['序号'], aggfunc='count')
origin_id= origin_id.reset_index()
origin_id.replace(['eBay-治润', 'eBay-雅秦', 'eBay-玲琅', 'Wayfair-信盒', 'Wayfair-维禄', 'Nextfur-Shopify',
                   'Walmart-优瑞斯特', 'Walmart-宫本', 'Walmart-赫曼', 'Walmart-信盒', '赫曼', '信盒', '宫本',
                   '驰甬','维禄', '森月', '玲琅', '治润', '哒唛旺', '简砾', '旗辰', '赛迦曼', '启珊', '信盒-法国',
                   '信盒-意大利', '信盒-西班牙', 'Central_Power_International_Limited'], ['ebay治润',
                    'ebay雅秦', 'ebay玲琅', 'wayfair信盒', 'wayfair维禄', 'shopifynextfur', 'walmart优瑞斯特',
                    'walmart宫本', 'walmart赫曼', 'walmart信盒', 'amazon赫曼', 'amazon信盒', 'amazon宫本',
                    'amazon驰甬', 'amazon维禄', 'amazon森月', 'amazon玲琅', 'amazon治润', 'amazon哒唛旺',
                    'amazon简砾', 'amazon旗辰', 'amazon赛迦曼', 'amazon启珊','amazon信盒欧线', 'amazon信盒欧线', 'amazon信盒欧线', "amazoncpower"], inplace=True)

inventory2_id = inventory_id[inventory_id['平台订单号'].isin(origin_id['订单号'].drop_duplicates())]
print('整理完成')
empty1 = pd.DataFrame(
    columns=['id', '登记日期', '更新日期', '登记人', '店铺', '订单号', 'SKU', '序号', '订单状态', '顾客反馈', '客服操作', 'Refund', '备注', '登记数量',
             '自发货数量', '库账号'])

for i in range(len(inventory2_id)):
    inventoryid_list = inventory2_id[inventory2_id['平台订单号'] == origin_id.iat[i, 0]]
    originid_list = origin_id[origin_id['订单号'] == origin_id.iat[i, 0]]
    if origin_id.iat[i, 0] in inventory2_id['平台订单号'].tolist():
        if list(set(inventoryid_list['账号'])) == list(set(originid_list['店铺'])):
            continue
        else:
            ee = pd.DataFrame(origin[origin['订单号'] == origin_id.iat[i, 0]])
            if len(ee) > 1:
                ee['库账号'] = list(set(inventoryid_list['账号']))[0]
            else:
                ee['库账号'] = list(set(inventoryid_list['账号']))
            empty1 = empty1.append(ee)
empty1 = empty1.sort_values(by =['店铺'])
empty1 = empty1.drop_duplicates()
now = time.strftime("%Y-%m-%d-%H_%M",time.localtime(time.time()))
empty1.to_excel(excel_writer="D:/work/订单数量校准/店铺错误/py店铺错误"+now+r"py.xlsx", index=False,encoding='utf-8')

origin1 = origin.pivot_table(index = ['订单号','SKU'],values = ['序号'], aggfunc = 'count')
origin1 = origin1.reset_index()

inventory2 = inventory1[inventory1['平台订单号'].isin(origin1['订单号'].drop_duplicates())]

iteration = read('match','iteration')
iteration = iteration[['SKU','SKU序号']]

now = time.strftime("%Y-%m-%d-%H_%M", time.localtime(time.time()))
df_empty = pd.DataFrame(columns=['id', '登记日期', '更新日期', '登记人', '店铺', '订单号', 'SKU', '序号', '订单状态', '顾客反馈', '客服操作', 'Refund', '备注','登记数量','自发货数量'])
df_empty1 = pd.DataFrame(columns=['id', '登记日期', '更新日期', '登记人', '店铺', '订单号', 'SKU', '序号', '订单状态', '顾客反馈', '客服操作', 'Refund', '备注','登记数量','自发货数量'])
df_empty2 = pd.DataFrame(columns=['id', '登记日期', '更新日期', '登记人', '店铺', '平台订单号', '公司SKU', '序号', '订单状态', '顾客反馈', '客服操作', 'Refund', '备注','登记数量','自发货数量'])
df_empty3 = pd.DataFrame(columns=['id', '登记日期', '更新日期', '登记人', '店铺', '平台订单号', '公司SKU', '序号', '订单状态', '顾客反馈', '客服操作', 'Refund', '备注','登记数量','自发货数量'])
df_empty4 = pd.DataFrame(columns=['id', '登记日期', '更新日期', '登记人', '店铺', '平台订单号', '公司SKU', '序号', '订单状态', '顾客反馈', '客服操作', 'Refund', '备注','登记数量','自发货数量'])
empty2 = pd.DataFrame(columns=['id', '登记日期', '更新日期', '登记人', '店铺', '订单号', 'SKU', '序号', '订单状态', '顾客反馈', '客服操作', 'Refund', '备注','登记数量','自发货数量'])

for i in range(len(origin1)):
    inventory_list = inventory2[inventory2['平台订单号'] == origin1.iat[i, 0]]
    origin_list = origin[origin['订单号'] == origin1.iat[i, 0]]
    if origin1.iat[i, 0] in inventory2['平台订单号'].tolist():
        sit_order = inventory2['平台订单号'].tolist().index(origin1.iat[i, 0])
        if origin1.iat[i, 2] > inventory2.iat[sit_order, 2] and origin1.iat[i, 1] == inventory2.iat[sit_order, 1]:
            origin_output = pd.DataFrame(origin[origin['订单号'] == origin1.iat[i, 0]])
            origin_output['登记数量'] = origin1.iat[i, 2]
            origin_output['自发货数量'] = inventory2.iat[sit_order, 2]
            df_empty = df_empty.append(origin_output)

        if set(origin_list['SKU']).issubset(set(inventory_list['公司SKU'])):
            continue
        else:
            ee = pd.DataFrame(origin[origin['订单号'] == origin1.iat[i, 0]])
            ee = ee.reset_index()
            inventory_list = inventory_list.reset_index()
            if len(ee) == len(inventory_list['公司SKU']):
                print(origin1.iat[i, 0], '*****')
                print(ee['SKU'], '--------')
                print(inventory_list['公司SKU'], '@@@@@@')
                for k in range(len(ee)):
                    ee.loc[k, 'fbm_SKU'] = inventory_list.loc[k, '公司SKU']
                    empty2 = empty2.append(ee)
            if len(ee) != len(inventory_list['公司SKU']):
                a = pd.merge(origin_list, iteration, on='SKU')
                b = pd.merge(inventory_list, iteration, left_on='公司SKU',right_on='SKU')
                c = pd.merge(a, b, on='SKU序号')
                df_empty4 = df_empty4.append(c)
    else:
        origin_output1 = pd.DataFrame(origin[origin['订单号'] == origin1.iat[i, 0]])
        df_empty1 = df_empty1.append(origin_output1)


empty2 = empty2.drop_duplicates(['id', '登记日期', '更新日期', '登记人', '店铺', '订单号', 'SKU', '序号', '订单状态', '顾客反馈', '客服操作', 'Refund', '备注'],keep = 'last')
# empty2 = empty2.drop('index',axis = 1)
df_empty1 = df_empty1.drop_duplicates()
df_empty1 = df_empty1[~df_empty1['订单状态'].isin(['已校对-P'])]
df_empty1 = df_empty1[~df_empty1['订单状态'].isin(['已校对-check'])]
empty2= empty2[~empty2['订单状态'].isin(['已校对-P'])]
empty2 = empty2[~empty2['订单状态'].isin(['已校对-check'])]
df_empty4 = df_empty4[~df_empty4['订单状态'].isin(['已校对-P'])]
df_empty4 = df_empty4[~df_empty4['订单状态'].isin(['已校对-check'])]
df_empty = df_empty[~df_empty['订单状态'].isin(['已校对-P'])]
df_empty = df_empty[~df_empty['订单状态'].isin(['已校对-check'])]
df_empty4 = df_empty4.drop_duplicates()
df_empty4 = df_empty4.drop(['登记数量','自发货数量','index','数量','补寄数量'],axis = 1)
df_empty = df_empty.drop_duplicates()
empty2.to_excel(excel_writer="D:/work/订单数量校准/sku个数匹配且有误/pysku个数匹配且有误"+now+r"py.xlsx", index=False,encoding='utf-8')
df_empty1.to_excel(excel_writer="D:/work/订单数量校准/订单不存在/py订单不存在"+now+r"py.xlsx", index=False,encoding='utf-8')
df_empty4.to_excel(excel_writer="D:/work/订单数量校准/sku个数不匹配且有误/pysku个数不匹配且有误"+now+r"py.xlsx", index=False,encoding='utf-8')
df_empty.to_excel(excel_writer="D:/work/订单数量校准/订单数量校准/py订单数量校准"+now+r"py.xlsx", index=False,encoding='utf-8')





