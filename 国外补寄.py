import pandas as pd
import csv
import os
import pymysql
from database_read import read
from pathlib import Path
from openpyxl import Workbook
import pandas as pd
from datetime import datetime
import re
from tkinter import _flatten
import numpy as np



# originzifa = pd.DataFrame()
# # 2021-2022年新系统数据循环读取并拼接
# #新系统就是我们现在在用的系统 我是用的销售报表部分 你需要替换成订单管理部分
# #下过来是csv文件 我试过了读不进 读excel很慢（因为量太大）
# #所以每次下过来先复制所有粘贴到txt文件可以大大提升读写速度
#
# for x in range(1, 14):
#     origin_zi = pd.read_table(r'D:/work/自发货订单管理源数据/系统数据/新数据_周/%s.txt' % x)
#     print(origin_zi)
#     origin_zifa = pd.DataFrame(origin_zi, columns=['订单号', '账号', '平台SKU','公司SKU', '数量', '包裹号','订单类型', '订单状态', '创建时间'])
#     originzifa = originzifa.append(origin_zifa)
#     originzifa = originzifa.reset_index(drop=True)
# originzifa = originzifa.rename(columns={'订单号': '平台订单号','创建时间': '订单时间'})
# originzifa['订单时间'] =pd.to_datetime(originzifa['订单时间'])
# #导出的时候会出现订单号没有的情况，后期可以删掉下面这行
# originzifa =originzifa[~originzifa['平台订单号'].isnull()]
# print('新系统完成')
# #温馨提示一下所用字段可能需要增加和改变 比如平台SKU
# # 后续因为需要统计退货率以及地区分区 所以需要把邮编拿进来 这一块暂时不考虑（因为我还没想好拿哪些字段）
# #暂时你需要哪些字段你就拿哪些字段 这些是我当时认为要拿的
# print(originzifa.head(2))
# #以下是旧系统数据 我们在2021.10之前用的是另一个系统 这些旧数据旧直接读好了 字段和新的不一样 你可以像我这样分开处理再合并
# # 也可以一起改名后统一处理
# originzifa2021 = pd.DataFrame()
# # 过往数据（旧系统）
# for x in range(1, 28):
#     origin_2021 = pd.read_csv(r'D:/work/自发货订单管理源数据/系统数据/旧数据/旧%s.csv' % x)
# #     print(origin_2021)
#     origin_zifa2021 = pd.DataFrame(origin_2021, columns=['订单号','账号','原始sku', 'sku', '数量', '包裹号', '订单类型', '订单状态', '发货时间'])
#     originzifa2021 = originzifa2021.append(origin_zifa2021)
#     originzifa2021 = originzifa2021.reset_index(drop=True)
# originzifa2021 = originzifa2021.rename(columns={'sku': '公司SKU', '原始sku': '平台SKU', '订单号': '平台订单号','发货时间': '订单时间'})
#
# print(originzifa2021.head(2))
# print('旧系统完成')
#
# originzifa = pd.concat([originzifa, originzifa2021], axis=0)
# originzifa = originzifa.reset_index(drop=True)
# print(originzifa.head(2))
# print('拼接完成')
#
# #新旧系统对于店铺名命名方式也不同所以写个正则翻译一下店铺名
# #后续我们自己的数据都会以这个命名方式为准
# fanyistr = {
#     "yourui Walmart US": 'walmart优瑞斯特',
#     "heman Walmart US": 'walmart赫曼',
#     "xinhemaoyi Walmart US": 'walmart信盒',
#     "gongben Walmart US": 'walmart宫本',
#     "xinhemaoyi wayfair": 'wayfair信盒',
#     "weilu wayfair": 'wayfair维禄',
#     "zhirun2021": 'ebay治润',
#     "yaqinsale11": 'ebay雅秦',
#     "linglang2021": 'ebay玲琅',
#     "linglanglv21": 'ebay玲琅',
#     "简砾": 'amazon简砾',
#     "shangming": 'amazon尚铭',
#     "jianli": 'amazon简砾',
#     "heman": 'amazon赫曼',
#     "赫曼": 'amazon赫曼',
#     "Youruisite": 'amazon优瑞斯特',
#     "信盒贸易": 'amazon信盒',
#     "玲琅": 'amazon玲琅',
#     "Central Power International Limited": 'amazoncpower',
#     "维禄": 'amazon维禄',
#     "宫本": 'amazon宫本',
#     "xinhemaoyi": 'amazon信盒',
#     "优瑞斯特": 'amazon优瑞斯特',
#     "森月": 'amazon森月',
#     "驰甬": 'amazon驰甬',
#     "尚铭": 'amazon尚铭',
#     "damaiwang": 'amazon哒唛旺',
#     "哒唛旺": 'amazon哒唛旺',
#     "bulubulu": 'amazon卟噜卟噜',
#     "治润": 'amazon治润',
#     "启珊": 'amazon启珊',
#     "卟噜卟噜": 'amazon卟噜卟噜',
#     "旗辰": 'amazon旗辰',
#     "senyue": 'amazon森月',
#     "NEXTFUR LLC": 'shopifynextfur',
#     "赛迦曼": 'amazon赛迦曼',
#     "gongben": 'amazon宫本',
#     "weilu": 'amazon维禄',
#     "NEXTFUR": 'shopifynextfur',
# }
#
#
# def tran(my_str, rep):
#     rep = dict((re.escape(k), v) for k, v in rep.items())
#     pattern = re.compile("|".join(rep.keys()))
#     my_str = pattern.sub(lambda m: rep[re.escape(m.group(0))], my_str)
#     return my_str
#
# # print(type(originzifa['账号'] ))
# # 翻译
# originzifa['账号'] = originzifa['账号'].apply(lambda x: tran(x, fanyistr))
# #之所以加平台是为了优化平台订单号 你可以考虑把平台和账号属性合二为一 节约储存空间
# #这是一个很重要的坑 早期的订单号很混乱（旧系统） 同一单下有多个产品时
# #比如第一单会写原订单号
# #第二单会写-1 第三单有时候写-1-1 有时候写-2
# #还原为原订单号 方便查找检索
# #这个我放在第二个文件做了 你可以在这一步统一做好 还原订单号再存源数据
# originzifa['平台'] = 0
# originzifa.loc[(originzifa['账号'].str.contains('amazon')), '平台'] = 'amazon'
# originzifa.loc[(originzifa['账号'].str.contains('wayfair')), '平台'] = 'wayfair'
# originzifa.loc[(originzifa['账号'].str.contains('ebay')), '平台'] = 'ebay'
# originzifa.loc[(originzifa['账号'].str.contains('walmart')), '平台'] = 'walmart'
# originzifa.loc[(originzifa['账号'].str.contains('shopify')), '平台'] = 'shopify'
#
# originzifa_wayfair = originzifa[((originzifa['平台']) == 'wayfair')]
# originzifa_wayfair = originzifa_wayfair.reset_index(drop=True)
# originzifa_wayfair['平台订单号'] = originzifa_wayfair['平台订单号'].str[0:11]
#
# originzifa_walmart = originzifa[((originzifa['平台']) == 'walmart')]
# originzifa_walmart = originzifa_walmart.reset_index(drop=True)
# originzifa_walmart['平台订单号'] = originzifa_walmart['平台订单号'].str[0:13]
#
# originzifa_shopify = originzifa[((originzifa['平台']) == 'shopify')]
# originzifa_shopify = originzifa_shopify.reset_index(drop=True)
# originzifa_shopify['平台订单号'] = originzifa_shopify['平台订单号'].str[0:13]
#
# originzifa_amazon = originzifa[((originzifa['平台']) == 'amazon')]
# originzifa_amazon = originzifa_amazon.reset_index(drop=True)
# originzifa_amazon['平台订单号'] = originzifa_amazon['平台订单号'].str[0:19]
#
# originzifa_ebay = originzifa[((originzifa['平台']) == 'ebay')]
# originzifa_ebay = originzifa_ebay.reset_index(drop=True)
# originzifa_ebay['平台订单号'] = originzifa_ebay['平台订单号'].str[0:14]
#
# newzifa = pd.concat([originzifa_amazon, originzifa_ebay, originzifa_wayfair, originzifa_walmart, originzifa_shopify])
# newzifa = newzifa.reset_index(drop=True)
# newzifa.drop('平台', axis=1, inplace=True)
# print(newzifa)
# print('处理订单号完成')
#
# ten_sector = pd.read_excel('D:/work/库存进度表/十部产品.xlsx',usecols=['SKU','分类'])
# ten_sector_c = ten_sector[ten_sector['分类'] == "成品"]
# ten_sector_p = ten_sector[ten_sector['分类'] == "配件"]
#
# origin_zifa_a = newzifa[((newzifa['订单类型']) != '补寄订单')]
# origin_zifa_a = origin_zifa_a.reset_index(drop=True)
# origin_zifa_b = newzifa[((newzifa['订单类型']) == '补寄订单')]
# origin_zifa_b = origin_zifa_b.reset_index(drop=True)
#
# origin_zifa_bc = origin_zifa_b[origin_zifa_b['公司SKU'].isin(ten_sector_c['SKU'].drop_duplicates())]
# origin_zifa_bc_num = pd.pivot_table(origin_zifa_bc, values=['数量'], index=['平台订单号', '账号', '公司SKU', '订单时间'], aggfunc='sum')
# origin_zifa_bc_num =origin_zifa_bc_num.reset_index()
# origin_zifa_bc_num =origin_zifa_bc_num.rename(columns={'数量': '补寄sku数量','订单时间': '补寄订单时间'})
# print(len(origin_zifa_bc))
# origin_zifa_bc.head(2)
#
# buji_sku = pd.merge(origin_zifa_a, origin_zifa_bc_num, how='outer', on=['平台订单号', '账号', '公司SKU'])
#
# buji_sku.head(2)
#
#
# origin_zifa_bp = origin_zifa_b[origin_zifa_b['公司SKU'].isin(ten_sector_p['SKU'].drop_duplicates())]
# origin_zifa_bp_num = pd.pivot_table(origin_zifa_bp, values=['数量'], index=['平台订单号', '账号','包裹号'], aggfunc='sum')
# origin_zifa_bp_num =origin_zifa_bp_num.reset_index()
# print(len(origin_zifa_bp))
# origin_zifa_bp.head(2)
#
# list123 = pd.DataFrame(origin_zifa_bp['平台订单号']).drop_duplicates()
#
# for i in range(len(list123)):
#     print(i)
#     mask = (origin_zifa_bp['平台订单号'] == list123.iat[i, 0])
#     order_list = origin_zifa_bp[origin_zifa_bp['平台订单号'] == list123.iat[i, 0]]
#     if list123.iat[i, 0] in buji_sku['平台订单号'].tolist():
#         sit = (buji_sku.index == buji_sku['平台订单号'].tolist().index(list123.iat[i, 0]))
#         a = len(set(list(_flatten(order_list['包裹号'].str.split(',').tolist()))))
#         buji_sku.loc[sit, '补寄配件次数'] = a
# print('自发货源数据new1 完成')


origin = read('after_sales','new_after_salesv2')
origin['订单号'] = origin['订单号'].astype(str)
inventory =read('inventory','自发货源数据new1')
inventory_buji = inventory
inventory_buji["补寄sku数量"] =inventory_buji["补寄sku数量"].fillna(0)
inventory_buji["补寄配件次数"] =inventory_buji["补寄配件次数"].fillna(0)
inventory_buji['补寄数量'] = inventory_buji['补寄sku数量']+inventory_buji['补寄配件次数']
inventory_buji.head()

origin_buji = origin[origin["客服操作"].str.contains('3-1-0')|origin["客服操作"].str.contains('3-2-0')|origin["客服操作"].str.contains('3-3-0')|origin["客服操作"].str.contains('3-4-0')|origin["订单状态"].str.contains('已校对-check')]
origin_buji = origin_buji.reset_index(drop = True)
origin_buji['补寄sku数量'] = 0



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
    columns=['id', '登记日期', '更新日期', '登记人', '店铺', '订单号', 'SKU', '序号', '订单状态', '顾客反馈', '客服操作', '操作代码', 'Refund', '备注',
             '补寄sku数量', '补寄配件次数', '数据库补寄数量'])
code = ['3-1-0', '3-2-0', '3-3-0']
origin_buji['补寄配件次数'] = 0
import time

for i in range(len(origin_buji)):
    code_split_list = origin_buji.loc[i, '客服操作'].split('&')
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
                if 'ship' in code_split_list[k]:
                    num_ship = code_split_list[k].index('ship')
                    ship_n = int(code_split_list[k][num_ship + 4:num_ship + 5])
                    num_sum = num_sum + ship_n
        origin_buji.loc[i, '补寄sku数量'] = origin_buji.loc[i, '补寄sku数量'] + num_sum
    num_sum1 = 0
    for k in range(len(code_split_list)):
        if '3-4-0' in code_split_list[k]:
            if "num" not in code_split_list[k] and "ship" not in code_split_list[k]:
                num_sum1 = num_sum1 + 1
            if len(find_all(code_split_list[k], 'num')) == 1:
                num1 = code_split_list[k].index('num')
                num_n2 = int(code_split_list[k][num1 + 3:num1 + 4])
                num_sum1 = num_n2
            if 'ship' in code_split_list[k]:
                num_ship1 = code_split_list[k].index('ship')
                ship_n1 = int(code_split_list[k][num_ship1 + 4:num_ship1 + 5])
                num_sum1 = num_sum1 + ship_n1
    origin_buji.loc[i, '补寄配件次数'] = origin_buji.loc[i, '补寄配件次数'] + num_sum1
    print(origin_buji.loc[i, '客服操作'], origin_buji.loc[i, '补寄sku数量'])
now = time.strftime("%Y-%m-%d-%H_%M_%S", time.localtime(time.time()))
origin_buji.to_excel(excel_writer="D:/work/origin_buji/" + now + r"origin_buji——new.xlsx", index=False, encoding='utf-8')


origin_buji['客服操作'] = origin_buji['客服操作'].apply(lambda x: x.split('&'))
origin_buji[ '补寄SKU']=''
for i in range(len(origin_buji)):
    for ix in range(len(origin_buji.iloc[i, 10])):
        # 对客服操作字段的拆分计算
        # 补寄处理
        if origin_buji.iloc[i, 10][ix][0:5] == '3-1-0':
            if len(origin_buji.iloc[i, 10][ix]) > 10:
                temp_sku = origin_buji.iloc[i, 10][ix][6:].split('num')
                print(type(temp_sku))
                if len(temp_sku)>1:
                    origin_buji.loc[i, '补寄SKU'] =origin_buji.loc[i, '补寄SKU']+' '+ temp_sku[0]
#                     origin_buji.loc[i, 'SKU_renew数量'] = temp_sku[1]
                else:
                    pass
        if origin_buji.iloc[i, 10][ix][0:5] == '3-2-0':
            if len(origin_buji.iloc[i, 10][ix]) > 10:
                temp_sku = origin_buji.iloc[i, 10][ix][6:].split('num')
                # print(type(temp_sku))
                if len(temp_sku)>1:
                    origin_buji.loc[i, '补寄SKU'] =origin_buji.loc[i, '补寄SKU']+' '+ temp_sku[0]
#                     origin_buji.loc[i, 'SKU_renew数量'] = temp_sku[1]
                else:
                    pass
        if origin_buji.iloc[i, 10][ix][0:5] == '3-3-0':
            if len(origin_buji.iloc[i, 10][ix]) > 10:
                temp_sku = origin_buji.iloc[i, 10][ix][6:].split('num')
                # print(type(temp_sku))
                if len(temp_sku)>1:
                    origin_buji.loc[i, '补寄SKU'] = origin_buji.loc[i, '补寄SKU']+' '+temp_sku[0]
#                     origin_buji.loc[i, 'SKU_renew数量'] = temp_sku[1]
                else:
                    pass

origin_buji['客服操作'] = origin['客服操作']
origin_buji['补寄数量'] = origin_buji['补寄sku数量']+origin_buji['补寄配件次数']
origin_buji.to_excel(excel_writer="D:/work/origin_buji/py"+now+r"origin_buji——sku.xlsx", index=False,encoding='utf-8')

origin_buji_table = origin_buji.pivot_table(index = '订单号',aggfunc = { '补寄数量':'sum'} )
origin_buji_table = origin_buji_table.reset_index()
origin_buji_table.head(2)
inventory_pivot = pd.pivot_table(inventory_buji, values=['补寄数量'], index=['平台订单号'], aggfunc='sum')
inventory_pivot = inventory_pivot.reset_index()
inventory_pivot.head(2)

print('售后补寄数量已提取')


#总的补寄数量不同以及订单登记错误
dengji_buji = pd.DataFrame(columns=['id', '登记日期', '更新日期', '登记人', '店铺', '订单号', 'SKU', '序号', '订单状态', '顾客反馈', '客服操作','操作代码', 'Refund', '备注','补寄sku数量','补寄配件次数','补寄数量','数据库补寄数量'])
import time
for i in range(len(origin_buji)):
    print(i)
    if (origin_buji.iloc[i, 5] in inventory_buji['平台订单号'].tolist()):
        sku_sit = inventory_pivot['平台订单号'].tolist().index(origin_buji.iloc[i, 5])
        table_sit = origin_buji_table['订单号'].tolist().index(origin_buji.iloc[i, 5])
        if int(origin_buji_table.iloc[table_sit, 1]) != int(inventory_pivot.iloc[sku_sit, 1]):
            print(origin_buji.loc[i, '订单号'], '***********')
            origin_output1 = pd.DataFrame(origin[origin['订单号'] == origin_buji.iloc[i, 5]])
            origin_output1.loc[:,'补寄数量'] = origin_buji_table.iloc[table_sit, 1]
            origin_output1.loc[:,'数据库补寄数量'] = inventory_pivot.iloc[sku_sit, 1]
            dengji_buji = dengji_buji.append(origin_output1)
    else:
        print(origin_buji.iloc[i, 5], '-------')
        table_sit = origin_buji_table['订单号'].tolist().index(origin_buji.iloc[i, 5])
        origin_output = pd.DataFrame(origin[origin['订单号'] == origin_buji.iloc[i, 5]])
        origin_output.loc[:,'补寄数量'] = origin_buji_table.iloc[table_sit, 1]
        inventory_in_buji = inventory_in_buji.append(origin_output)
# 加个去重！！！！！
now = time.strftime("%Y-%m-%d-%H_%M",time.localtime(time.time()))
dengji_buji.to_excel(excel_writer="D:/work/登记补寄数量/国外补寄登记补寄数量不符/py"+now+r"登记补寄数量不符.xlsx", index=False,encoding='utf-8')
inventory_in_buji.to_excel(excel_writer="D:/work/错误订单/错填或不在补寄订单中的订单/py"+now+r"错填或不在补寄订单中的订单.xlsx", index=False,encoding='utf-8')
print('总的补寄数量不同以及订单登记错误完成')


#自发货中有补寄订单，但是售后漏登记
#已校对——check的是没有补寄操作的，但是有补寄订单（数据中心核对了），所有要给他赋一个值证明它补寄过
origin_buji.loc[origin_buji['订单状态'] == '已校对-check','补寄数量'] =1
origin_buji_big = origin_buji[origin_buji['补寄数量']>0]
inventory_buji_big =inventory_buji[inventory_buji['补寄数量']>0]
import datetime
AdDate = datetime.date(2022,1, 1)
inventory_buji_big.loc[:, '订单时间'] = pd.to_datetime(inventory_buji_big['订单时间'])
inventory_buji_big.loc[:,'补寄订单时间'] = pd.to_datetime(inventory_buji_big['补寄订单时间'])
inventory_1 = inventory_buji_big[inventory_buji_big['订单时间'] >= datetime.datetime.combine(AdDate, datetime.datetime.min.time())  ]
inventory_2 = inventory_buji_big[inventory_buji_big['补寄订单时间'] >= datetime.datetime.combine(AdDate, datetime.datetime.min.time())]
# inventory_3 = pd.merge(inventory_1,inventory_2,how = 'outer', on = ['平台订单号','账号','平台SKU','公司SKU','数量','包裹号','订单类型','订单状态','补寄sku数量','补寄配件次数','补寄数量'])
inventory_3 = pd.merge(inventory_1, inventory_2, how='outer',
                       on=['平台订单号', '账号', '平台SKU', '公司SKU', '数量', '包裹号',  '订单状态', '补寄sku数量', '补寄配件次数', '补寄数量'])
inventory_list =inventory_3['平台订单号'].drop_duplicates()

dengji_buji1 = pd.DataFrame(
    columns=['平台订单号', '账号', '平台SKU', '公司SKU', '数量', '包裹号', '订单类型', '订单状态', '订单时间', '补寄订单时间', '补寄sku数量', '补寄配件次数',
             '补寄数量'])
inventory_in_buji1 = pd.DataFrame(
    columns=['平台订单号', '账号', '平台SKU', '公司SKU', '数量', '包裹号', '订单类型', '订单状态', '订单时间', '补寄订单时间', '补寄sku数量', '补寄配件次数',
             '补寄数量'])
#漏登
for i in range(len(inventory_3)):
    if (inventory_3.iloc[i, 0] in origin_buji_big['订单号'].tolist()):
        origin_output1 = pd.DataFrame(inventory_3[inventory_3['平台订单号'] == inventory_3.iloc[i, 0]])
        dengji_buji1 = dengji_buji1.append(origin_output1)
    else:
        origin_output = pd.DataFrame(inventory_3[inventory_3['平台订单号'] == inventory_3.iloc[i, 0]])
        inventory_in_buji1 = inventory_in_buji1.append(origin_output)

match = read('match', 'iteration')
match = match[['SKU', 'SKU序号']]
match = match.rename(columns={'SKU序号': 'sku序号'})
zifahuo = pd.merge(inventory_in_buji1, match, left_on=['公司SKU'], right_on=['SKU'], how='left')
saler = read('sale', 'sku_by_saler')
date = datetime.date(9999, 12, 31)
saler = saler[saler['结束时间'] == date]
saler = saler[['sku序号', '店铺', '运维']]
saler = saler.rename(columns={'店铺': '账号'})
inventory_in_buji12 = pd.merge(zifahuo, saler, on=['账号', 'sku序号'], how='left')
inventory_in_buji12
now = time.strftime("%Y-%m-%d-%H_%M_%S",time.localtime(time.time()))
dengji_buji1 = dengji_buji1[~dengji_buji1['订单状态'].isin(['已校对-P'])]
dengji_buji1 = dengji_buji1[~dengji_buji1['订单状态'].isin(['已校对-check'])]

inventory_in_buji12 = inventory_in_buji12[~inventory_in_buji12['订单状态'].isin(['已校对-P'])]
inventory_in_buji12 = inventory_in_buji12[~inventory_in_buji12['订单状态'].isin(['已校对-check'])]

dengji_buji1.to_excel(excel_writer="D:/work/登记补寄数量/国外补寄自发货订单售后已登记/"+now+r"国外补寄自发货订单售后已登记py.xlsx", index=False,encoding='utf-8')
inventory_in_buji12.to_excel(excel_writer="D:/work/登记补寄数量/国外补寄自发货订单售后漏登记/"+now+r"国外补寄自发货订单售后漏登记py.xlsx", index=False,encoding='utf-8')
print('自发货中有补寄订单，但是售后漏登记')
#细化补寄SKU和补寄配件不同
# origin_buji_notin = origin_buji[origin_buji['订单号'].isin(dengji_buji['订单号'])]
# origin_buji1 = origin_buji_notin[['id', '登记人', '店铺', '订单号', 'SKU','补寄sku数量','补寄配件次数','补寄SKU','补寄数量']]
# origin_buji1 = origin_buji1.rename(columns={'补寄配件次数': '登记补寄配件次数'})
# origin_buji1 = origin_buji1.rename(columns={'补寄sku数量': '登记补寄sku数量'})
# origin_buji1 = origin_buji1.rename(columns={'补寄数量': '登记补寄数量'})
# origin_buji1 = origin_buji1.rename(columns={'订单号': '平台订单号'})
# origin_buji1 = origin_buji1.rename(columns={'SKU': '公司SKU'})
#
# origin_buji1.head(2)
# inventory_origin = inventory[inventory['平台订单号'].isin(origin_buji1['平台订单号'].drop_duplicates())]
# merge = pd.merge(inventory_origin,origin_buji1,how= 'outer',on = ['平台订单号','公司SKU'])
# now = time.strftime("%Y-%m-%d-%H_%M_%S",time.localtime(time.time()))
# merge.to_excel(excel_writer="D:/work/登记补寄数量/国外补寄寄sku和补寄配件不同/"+now+r"国外补寄sku和补寄配件不同py.xlsx", index=False,encoding='utf-8')
# print('补寄SKU和补寄配件不同')



