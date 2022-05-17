import pandas as pd
from database_read import read
from sqlalchemy import create_engine
import re
from tkinter import _flatten

connect2 = create_engine(
    f'mysql+pymysql://{"test1"}:{"123456"}@{"rm-bp15wb9k07q3p10q6yo.mysql.rds.aliyuncs.com"}:{3306}/{"inventory"}?charset=utf8')

originzifa = pd.DataFrame()
# 2021-2022年新系统数据循环读取并拼接
#新系统就是我们现在在用的系统 我是用的销售报表部分 你需要替换成订单管理部分
#下过来是csv文件 我试过了读不进 读excel很慢（因为量太大）
#所以每次下过来先复制所有粘贴到txt文件可以大大提升读写速度

originzifa = pd.DataFrame()
for x in range(1,14):
    origin_zi = pd.read_table(r"D:/work/自发货订单管理源数据/系统数据/新数据_周/%s.txt" % x )
#     print(origin_zi[origin_zi['订单号'] == 'NF10834863947-4504431296678'])
    origin_zifa = pd.DataFrame(origin_zi, columns=['订单号', '账号', '平台SKU','公司SKU', '数量', '包裹号','订单类型', '订单状态', '创建时间'])
    originzifa = originzifa.append(origin_zifa)
    originzifa = originzifa.reset_index(drop=True)
originzifa = originzifa.rename(columns={'订单号': '平台订单号','创建时间': '订单时间'})
originzifa['订单时间'] =pd.to_datetime(originzifa['订单时间'])
originzifa =originzifa[~originzifa["订单状态"].str.contains('已取消')]
originzifa.head(2)
print('新系统完成')
originzifa_list = originzifa.pivot_table(index=['平台订单号'], values=['数量'], aggfunc='count')
originzifa_list= originzifa_list.reset_index()
originzifa_list =originzifa_list[originzifa_list['数量'] == 1]
originzifa_order = originzifa[originzifa["平台订单号"].isin(originzifa_list["平台订单号"])]
originzifa_listpivot = pd.merge(originzifa_order[['平台订单号','订单类型']],originzifa_list,on = '平台订单号')
buji = originzifa_listpivot[originzifa_listpivot['订单类型'] == '补寄订单']
# index = originzifa.loc[originzifa['平台订单号'].isin(buji['平台订单号']),:].index
originzifa.loc[originzifa['平台订单号'].isin(buji['平台订单号']),'订单类型'] = '正常订单'
originzifa =originzifa[~originzifa['平台订单号'].isnull()]

#温馨提示一下所用字段可能需要增加和改变 比如平台SKU
# 后续因为需要统计退货率以及地区分区 所以需要把邮编拿进来 这一块暂时不考虑（因为我还没想好拿哪些字段）
#暂时你需要哪些字段你就拿哪些字段 这些是我当时认为要拿的
print(originzifa.head(2))

#以下是旧系统数据 我们在2021.10之前用的是另一个系统 这些旧数据旧直接读好了 字段和新的不一样 你可以像我这样分开处理再合并
# 也可以一起改名后统一处理
originzifa2021 = pd.DataFrame()
# 过往数据（旧系统）
from pathlib import Path
for x in range(1, 27):
    origin_2021 = pd.read_csv(r'D:/work/自发货订单管理源数据/系统数据/旧数据/旧%s.csv' % x)
    #     print(origin_2021)
    #     origin_2021['表'] = x
    origin_zifa2021 = pd.DataFrame(origin_2021,
                                   columns=['订单号', '账号', '原始sku', 'sku', '数量', '包裹号', '订单类型', '订单状态', '发货时间'])

    originzifa2021 = originzifa2021.append(origin_zifa2021)
    originzifa2021 = originzifa2021.reset_index(drop=True)
originzifa2021 = originzifa2021.rename(columns={'sku': '公司SKU', '原始sku': '平台SKU', '订单号': '平台订单号', '发货时间': '订单时间'})
# originzifa2021 = originzifa2021.drop_duplicates()

print(originzifa2021.head(2))
print('旧系统完成')

originzifa = pd.concat([originzifa, originzifa2021], axis=0)
originzifa = originzifa.reset_index(drop=True)
print(originzifa.head(2))
print('拼接完成')

#新旧系统对于店铺名命名方式也不同所以写个正则翻译一下店铺名
#后续我们自己的数据都会以这个命名方式为准
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

# print(type(originzifa['账号'] ))
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
newzifa.drop('平台', axis=1, inplace=True)
print(newzifa)
print('处理订单号完成')

ten_sector = pd.read_excel('D:/work/库存进度表/十部产品.xlsx',usecols=['SKU','分类'])
ten_sector_c = ten_sector[ten_sector['分类'] == "成品"]
ten_sector_p = ten_sector[ten_sector['分类'] == "配件"]
newzifa.loc[(newzifa['订单类型'] == '手工订单')& (newzifa['公司SKU'].isin(ten_sector_c['SKU'])),'订单类型'] = '正常订单'
newzifa.loc[(newzifa['订单类型'] == '手工订单')& (newzifa['公司SKU'].isin(ten_sector_p['SKU'])),'订单类型'] = '补寄订单'

origin_zifa_a = newzifa[((newzifa['订单类型']) != '补寄订单')]
origin_zifa_a = origin_zifa_a.reset_index(drop=True)
origin_zifa_b = newzifa[((newzifa['订单类型']) == '补寄订单')]
origin_zifa_b = origin_zifa_b.reset_index(drop=True)

origin_zifa_bc = origin_zifa_b[origin_zifa_b['公司SKU'].isin(ten_sector_c['SKU'].drop_duplicates())]
origin_zifa_bc_num = pd.pivot_table(origin_zifa_bc, values=['数量'], index=['平台订单号', '账号', '公司SKU'], aggfunc='sum')
origin_zifa_bc_num =origin_zifa_bc_num.reset_index()
origin_zifa_bc_num =origin_zifa_bc_num.rename(columns={'数量': '补寄sku数量'})
print(len(origin_zifa_bc))
origin_zifa_bc.head(2)

origin_zifa_bc1 =origin_zifa_bc[['平台订单号','账号','公司SKU','订单时间']]
origin_zifa_bc1 =origin_zifa_bc1.rename(columns={'订单时间': '补寄sku订单时间'})
origin_zifa_bc_num1 = pd.merge(origin_zifa_bc_num,origin_zifa_bc1,how = 'outer',on =['平台订单号','账号','公司SKU'])
origin_zifa_bc_num1.drop_duplicates(subset=['平台订单号','账号','公司SKU'],keep='first',inplace=True)

buji_sku = pd.merge(origin_zifa_a, origin_zifa_bc_num1, how='outer', on=['平台订单号', '账号', '公司SKU'])

buji_sku.head(2)


origin_zifa_bp = origin_zifa_b[origin_zifa_b['公司SKU'].isin(ten_sector_p['SKU'].drop_duplicates())]
origin_zifa_bp_num = pd.pivot_table(origin_zifa_bp, values=['数量'], index=['平台订单号', '账号', '订单时间'], aggfunc='sum')
origin_zifa_bp_num =origin_zifa_bp_num.reset_index()
origin_zifa_bp_num =origin_zifa_bp_num.rename(columns={'订单时间': '补寄配件订单时间'})
origin_zifa_bp_num =origin_zifa_bp_num.drop(columns='数量')
print(len(origin_zifa_bp))
origin_zifa_bp.head(2)

list123 = pd.DataFrame(origin_zifa_bp['平台订单号']).drop_duplicates()

from tkinter import _flatten
for i in range(len(list123)):
    print(i)
    mask = (origin_zifa_bp['平台订单号'] ==list123.iat[i,0] )
    order_list = origin_zifa_bp[origin_zifa_bp['平台订单号'] == list123.iat[i,0]]
    if list123.iat[i,0] in buji_sku['平台订单号'].tolist():
        sit = (buji_sku.index ==buji_sku['平台订单号'].tolist().index(list123.iat[i,0]))
        a = len(set(list(_flatten(order_list['包裹号'].str.split(',').tolist()))))
        buji_sku.loc[sit,'补寄配件次数'] = a

buji_sku_merge = pd.merge(buji_sku, origin_zifa_bp_num,how = 'outer',on=['平台订单号', '账号'])
buji_sku_merge.head(2)
import math
import math
import numpy as np
for i in buji_sku_merge[buji_sku_merge['补寄配件次数'].isnull()& ~buji_sku_merge['补寄配件订单时间'].isnull()].index.tolist():
    buji_sku_merge.loc[buji_sku_merge['补寄配件次数'].isnull()& ~buji_sku_merge['补寄配件订单时间'].isnull(),'补寄配件订单时间'] = np.nan
print("整理结束")

# 量太大 先存一下
# newzifa = newzifa.encode("utf-8").decode("latin1")
buji_sku1=buji_sku.drop('订单类型', axis=1)
# buji_sku1=buji_sku.drop('订单时间', axis=1)
# buji_sku1=buji_sku.drop('补寄订单时间', axis=1)
buji_sku1.to_sql("自发货源数据new1", connect2, if_exists='replace', index=False)

