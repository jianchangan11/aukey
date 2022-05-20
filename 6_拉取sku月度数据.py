import datetime
import datetime
import pandas as pd
from database_read import read
import re
import warnings
warnings.filterwarnings("ignore")
inventory = read('inventory','自发货源数据new1')
#划分时间
AdDate = datetime.date(2021,1, 1)
ENDDate = datetime.date(2022,5, 1)
inventory['订单时间'] = pd.to_datetime(inventory['订单时间'])
inventory['补寄sku订单时间'] = pd.to_datetime(inventory['补寄sku订单时间'])
inventory['补寄配件订单时间'] = pd.to_datetime(inventory['补寄配件订单时间'])
#选取字段，限制时间
inventory = inventory[["公司SKU","订单时间","补寄sku订单时间","补寄配件订单时间","账号",'数量']]
inventory = inventory[(inventory['订单时间'] >= datetime.datetime.combine(AdDate, datetime.datetime.min.time()))&(inventory['订单时间'] < datetime.datetime.combine(ENDDate, datetime.datetime.min.time()))]

# 提取年月
inventory['year'] = inventory['订单时间'].dt.year
inventory['month'] = inventory['订单时间'].dt.month
#统计sku月度数量
# inventory['year'] = inventory['订单时间'].dt.year
# inventory['month'] = inventory['订单时间'].dt.month
inventory = inventory.pivot_table(index=['公司SKU','year','month','账号'], values=['数量'], aggfunc='sum')
inventory = inventory.reset_index()
inventory.head(2)

#输出保存
inventory.to_excel(excel_writer="D:/work/sku_count.xlsx", index=False,encoding='utf-8')


# # 纯粹的补寄sku     选择所有补寄订单
# 读取所有订单
# originzifa = pd.DataFrame()
# for x in range(1,14):
#     origin_zi = pd.read_table(r"D:/work/自发货订单管理源数据/系统数据/新数据_周/%s.txt" % x )
#     origin_zifa = pd.DataFrame(origin_zi, columns=['订单号', '账号', '平台SKU','公司SKU', '数量', '包裹号','订单类型', '订单状态', '创建时间'])
#     originzifa = originzifa.append(origin_zifa)
#     originzifa = originzifa.reset_index(drop=True)
# originzifa = originzifa.rename(columns={'订单号': '平台订单号','创建时间': '订单时间'})#统一列名
# originzifa['订单时间'] =pd.to_datetime(originzifa['订单时间'])
# originzifa =originzifa[~originzifa["订单状态"].str.contains('已取消')]  #去掉已取消的订单
# originzifa.head(2)
# print('新系统完成')
# #存在正常订单取消改发补寄订单的情况，所以将这些订单改为正常订单
# originzifa_list = originzifa.pivot_table(index=['平台订单号'], values=['数量'], aggfunc='count')
# originzifa_list= originzifa_list.reset_index()
# originzifa_list =originzifa_list[originzifa_list['数量'] == 1] #将单个正常订单和单个补寄订单的订单号取出
# #在整个数据中再到单个正常订单和单个补寄订单的记录，筛选出其中的补寄订单
# originzifa_order = originzifa[originzifa["平台订单号"].isin(originzifa_list["平台订单号"])]
# originzifa_listpivot = pd.merge(originzifa_order[['平台订单号','订单类型']],originzifa_list,on = '平台订单号')
# buji = originzifa_listpivot[originzifa_listpivot['订单类型'] == '补寄订单']
# # index = originzifa.loc[originzifa['平台订单号'].isin(buji['平台订单号']),:].index
# #将筛选出来的补寄订单改为正常订单
# originzifa.loc[originzifa['平台订单号'].isin(buji['平台订单号']),'订单类型'] = '正常订单'
# originzifa =originzifa[~originzifa['平台订单号'].isnull()]
#
# #温馨提示一下所用字段可能需要增加和改变 比如平台SKU
# # 后续因为需要统计退货率以及地区分区 所以需要把邮编拿进来 这一块暂时不考虑（因为我还没想好拿哪些字段）
# #暂时你需要哪些字段你就拿哪些字段 这些是我当时认为要拿的
# print(originzifa.head(2))
#
# #以下是旧系统数据 我们在2021.10之前用的是另一个系统 这些旧数据旧直接读好了 字段和新的不一样 你可以像我这样分开处理再合并
# # 也可以一起改名后统一处理
# originzifa2021 = pd.DataFrame()
# # 过往数据（旧系统）
# from pathlib import Path
# for x in range(1, 27):
#     origin_2021 = pd.read_csv(r'D:/work/自发货订单管理源数据/系统数据/旧数据/旧%s.csv' % x)
#     #     print(origin_2021)  #将所有的订单标准来源表，在之后的查看中可以明确知道那个订单来自那个表
#     #     origin_2021['表'] = x
#     origin_zifa2021 = pd.DataFrame(origin_2021,
#                                    columns=['订单号', '账号', '原始sku', 'sku', '数量', '包裹号', '订单类型', '订单状态', '发货时间'])
#
#     originzifa2021 = originzifa2021.append(origin_zifa2021)
#     originzifa2021 = originzifa2021.reset_index(drop=True)
# originzifa2021 = originzifa2021.rename(columns={'sku': '公司SKU', '原始sku': '平台SKU', '订单号': '平台订单号', '发货时间': '订单时间'})
# # 防止重复读取
# originzifa2021 = originzifa2021.drop_duplicates()
#
# print(originzifa2021.head(2))
# print('旧系统完成')
#
# #旧数据的包裹号会有出现-1,-2的形式，所以要将正确的包裹号提取出来。
# originzifa2021['包裹号'] = originzifa2021['包裹号'].apply(lambda x:x.split("-")[0])
#
# originzifa = pd.concat([originzifa, originzifa2021], axis=0)
# originzifa = originzifa.reset_index(drop=True)
# print(originzifa.head(2))
# print('拼接完成')
#
# #新旧系统对于店铺名命名方式也不同所以写个正则翻译一下店铺名
# #后续我们自己的数据都会以这个命名方式为准
# fanyistr = {
#     "yourui Walmart US": 'walmart优瑞斯特', "heman Walmart US": 'walmart赫曼',
#     "xinhemaoyi Walmart US": 'walmart信盒', "gongben Walmart US": 'walmart宫本',
#     "xinhemaoyi wayfair": 'wayfair信盒', "weilu wayfair": 'wayfair维禄', "zhirun2021": 'ebay治润',
#     "yaqinsale11": 'ebay雅秦',   "linglang2021": 'ebay玲琅',
#     "linglanglv21": 'ebay玲琅',  "简砾": 'amazon简砾',
#     "shangming": 'amazon尚铭',     "jianli": 'amazon简砾',
#     "heman": 'amazon赫曼',    "赫曼": 'amazon赫曼',
#     "Youruisite": 'amazon优瑞斯特',    "信盒贸易": 'amazon信盒',
#     "玲琅": 'amazon玲琅',  "Central Power International Limited": 'amazoncpower',
#     "维禄": 'amazon维禄',    "宫本": 'amazon宫本',
#     "xinhemaoyi": 'amazon信盒',   "优瑞斯特": 'amazon优瑞斯特',
#     "森月": 'amazon森月',   "驰甬": 'amazon驰甬',
#     "尚铭": 'amazon尚铭',#     "damaiwang": 'amazon哒唛旺',
#     "哒唛旺": 'amazon哒唛旺',     "bulubulu": 'amazon卟噜卟噜',
#     "治润": 'amazon治润', "启珊": 'amazon启珊',
#     "卟噜卟噜": 'amazon卟噜卟噜',"旗辰": 'amazon旗辰',
#     "senyue": 'amazon森月', "NEXTFUR LLC": 'shopifynextfur',
#     "赛迦曼": 'amazon赛迦曼',  "gongben": 'amazon宫本',
#     "weilu": 'amazon维禄',   "NEXTFUR": 'shopifynextfur',
# }

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
# #比如第一单会写原订单号,第二单会写-1 第三单有时候写-1-1 有时候写-2
# #还原为原订单号 方便查找检索
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
# #选择所有补寄订单
# inventory = newzifa[newzifa['订单类型'] == '补寄订单']
# inventory
#
# # 选择21年第一季度
# AdDate = datetime.date(2021,1, 1)
# ENDDate = datetime.date(2022,5, 1)
# inventory['发货时间'] = pd.to_datetime(inventory['发货时间'])
# inventory = inventory[["公司SKU","发货时间","账号",'数量']]
# inventory = inventory[(inventory['发货时间'] >= datetime.datetime.combine(AdDate, datetime.datetime.min.time()))&(inventory['发货时间'] < datetime.datetime.combine(ENDDate, datetime.datetime.min.time()))]
##提取年月
# inventory['year'] = inventory['发货时间'].dt.year
# inventory['month'] = inventory['发货时间'].dt.month
# inventory = inventory.pivot_table(index=['公司SKU','year','month','账号'], values=['数量'], aggfunc="sum")
# inventory = inventory.reset_index()
#
# inventory.to_excel(excel_writer="D:/work/invnetorybuji2.xlsx", index=False,encoding='utf-8')