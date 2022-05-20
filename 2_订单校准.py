import datetime
import time
import pandas as pd
from database_read import read
import re
import warnings
warnings.filterwarnings("ignore")
#读取数据，以订单号，sku，数量，补寄数量的格式，不以自发货源数据new1的格式，所以重新读取数据

originzifa = pd.DataFrame()
for x in range(1,14):
    origin_zi = pd.read_table(r"D:/work/自发货订单管理源数据/系统数据/新数据_周/%s.txt" % x )
    origin_zifa = pd.DataFrame(origin_zi, columns=['订单号', '账号', '平台SKU','公司SKU', '数量', '包裹号','订单类型', '订单状态', '创建时间'])
    originzifa = originzifa.append(origin_zifa)
    originzifa = originzifa.reset_index(drop=True)
originzifa = originzifa.rename(columns={'订单号': '平台订单号','创建时间': '订单时间'})#统一列名
originzifa['订单时间'] =pd.to_datetime(originzifa['订单时间'])
originzifa =originzifa[~originzifa["订单状态"].str.contains('已取消')]  #去掉已取消的订单
originzifa.head(2)
print('新系统完成')
#存在正常订单取消改发补寄订单的情况，所以将这些订单改为正常订单
originzifa_list = originzifa.pivot_table(index=['平台订单号'], values=['数量'], aggfunc='count')
originzifa_list= originzifa_list.reset_index()
originzifa_list =originzifa_list[originzifa_list['数量'] == 1] #将单个正常订单和单个补寄订单的订单号取出
#在整个数据中再到单个正常订单和单个补寄订单的记录，筛选出其中的补寄订单
originzifa_order = originzifa[originzifa["平台订单号"].isin(originzifa_list["平台订单号"])]
originzifa_listpivot = pd.merge(originzifa_order[['平台订单号','订单类型']],originzifa_list,on = '平台订单号')
buji = originzifa_listpivot[originzifa_listpivot['订单类型'] == '补寄订单']
# index = originzifa.loc[originzifa['平台订单号'].isin(buji['平台订单号']),:].index
#将筛选出来的补寄订单改为正常订单
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
    #     print(origin_2021)  #将所有的订单标准来源表，在之后的查看中可以明确知道那个订单来自那个表
    #     origin_2021['表'] = x
    origin_zifa2021 = pd.DataFrame(origin_2021,
                                   columns=['订单号', '账号', '原始sku', 'sku', '数量', '包裹号', '订单类型', '订单状态', '发货时间'])

    originzifa2021 = originzifa2021.append(origin_zifa2021)
    originzifa2021 = originzifa2021.reset_index(drop=True)
originzifa2021 = originzifa2021.rename(columns={'sku': '公司SKU', '原始sku': '平台SKU', '订单号': '平台订单号', '发货时间': '订单时间'})
# 防止重复读取
originzifa2021 = originzifa2021.drop_duplicates()

print(originzifa2021.head(2))
print('旧系统完成')

#旧数据的包裹号会有出现-1,-2的形式，所以要将正确的包裹号提取出来。
originzifa2021['包裹号'] = originzifa2021['包裹号'].apply(lambda x:x.split("-")[0])

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

#读取售后数据
origin = read('after_sales','new_after_salesv2')
inventory = originlast.copy()
inventory1 = inventory.copy()
#找出数据表中单纯的补寄订单，将补寄数量添加到数量中
import math
for i in range(len(inventory1)):
    if math.isnan(inventory1.loc[i,'数量']) and not math.isnan(inventory1.loc[i,'补寄数量']):
        inventory1.loc[i,'数量'] = inventory1.loc[i,'补寄数量']
#设置时间，选取一定时间范围内的数据
AdDate = datetime.date(2020,1, 1)
inventory1['最早时间'] = pd.to_datetime(inventory1['最早时间'])
inventory1 = inventory1[inventory1['最早时间'] >= datetime.datetime.combine(AdDate, datetime.datetime.min.time())]
inventory1 = inventory1.pivot_table(index = ['平台订单号', '公司SKU'], aggfunc='sum')
inventory1 = inventory1.reset_index()
#找出每个自发货数据订单对应的账号
inventory_id = inventory.pivot_table(index = ['平台订单号','账号'],values =['数量'], aggfunc='count')
inventory_id= inventory_id.reset_index()
#找出售后数据对应的店铺账号
origin_id = origin.pivot_table(index=['订单号', '店铺'], values=['序号'], aggfunc='count')
origin_id= origin_id.reset_index()
#将所有售后店铺的名称进行替换，方便后期比较店铺是否错误
origin_id.replace(['eBay-治润', 'eBay-雅秦', 'eBay-玲琅', 'Wayfair-信盒', 'Wayfair-维禄', 'Nextfur-Shopify',
                   'Walmart-优瑞斯特', 'Walmart-宫本', 'Walmart-赫曼', 'Walmart-信盒', '赫曼', '信盒', '宫本',
                   '驰甬','维禄', '森月', '玲琅', '治润', '哒唛旺', '简砾', '旗辰', '赛迦曼', '启珊', '信盒-法国',
                   '信盒-意大利', '信盒-西班牙', 'Central_Power_International_Limited'], ['ebay治润',
                    'ebay雅秦', 'ebay玲琅', 'wayfair信盒', 'wayfair维禄', 'shopifynextfur', 'walmart优瑞斯特',
                    'walmart宫本', 'walmart赫曼', 'walmart信盒', 'amazon赫曼', 'amazon信盒', 'amazon宫本',
                    'amazon驰甬', 'amazon维禄', 'amazon森月', 'amazon玲琅', 'amazon治润', 'amazon哒唛旺',
                    'amazon简砾', 'amazon旗辰', 'amazon赛迦曼', 'amazon启珊','amazon信盒欧线', 'amazon信盒欧线', 'amazon信盒欧线', "amazoncpower"], inplace=True)
#找出售后订单在自发货数据中对应的数据，缩小数据范围，加快运行时间
inventory2_id = inventory_id[inventory_id['平台订单号'].isin(origin_id['订单号'].drop_duplicates())]
print('整理完成')

#设置空表
empty1 = pd.DataFrame(
    columns=['id', '登记日期', '更新日期', '登记人', '店铺', '订单号', 'SKU', '序号', '订单状态', '顾客反馈', '客服操作', 'Refund', '备注', '登记数量',
             '自发货数量', '库账号'])
#比较店铺是否一致
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
empty1 = empty1.sort_values(by =['店铺'])#店铺排序
empty1 = empty1.drop_duplicates()#去重
now = time.strftime("%Y-%m-%d-%H_%M",time.localtime(time.time()))
empty1.to_excel(excel_writer="D:/work/订单数量校准/店铺错误/py店铺错误"+now+r"py.xlsx", index=False,encoding='utf-8')

#统计每个售后登记订单sku的条数，用于比较订单条数是否准确
origin1 = origin.pivot_table(index = ['订单号','SKU'],values = ['序号'], aggfunc = 'count')
origin1 = origin1.reset_index()
#找出售后订单sku对应的自发货订单
inventory2 = inventory1[inventory1['平台订单号'].isin(origin1['订单号'].drop_duplicates())]
#读取迭代表，后面用于匹配订单数量不准确的运维人员
iteration = read('match','iteration')
iteration = iteration[['SKU','SKU序号']]

#给不同内容设置不同的表
now = time.strftime("%Y-%m-%d-%H_%M", time.localtime(time.time()))
df_empty = pd.DataFrame(columns=['id', '登记日期', '更新日期', '登记人', '店铺', '订单号', 'SKU', '序号', '订单状态', '顾客反馈', '客服操作', 'Refund', '备注','登记数量','自发货数量'])
df_empty1 = pd.DataFrame(columns=['id', '登记日期', '更新日期', '登记人', '店铺', '订单号', 'SKU', '序号', '订单状态', '顾客反馈', '客服操作', 'Refund', '备注','登记数量','自发货数量'])
df_empty2 = pd.DataFrame(columns=['id', '登记日期', '更新日期', '登记人', '店铺', '平台订单号', '公司SKU', '序号', '订单状态', '顾客反馈', '客服操作', 'Refund', '备注','登记数量','自发货数量'])
df_empty3 = pd.DataFrame(columns=['id', '登记日期', '更新日期', '登记人', '店铺', '平台订单号', '公司SKU', '序号', '订单状态', '顾客反馈', '客服操作', 'Refund', '备注','登记数量','自发货数量'])
df_empty4 = pd.DataFrame(columns=['id', '登记日期', '更新日期', '登记人', '店铺', '平台订单号', '公司SKU', '序号', '订单状态', '顾客反馈', '客服操作', 'Refund', '备注','登记数量','自发货数量'])
empty2 = pd.DataFrame(columns=['id', '登记日期', '更新日期', '登记人', '店铺', '订单号', 'SKU', '序号', '订单状态', '顾客反馈', '客服操作', 'Refund', '备注','登记数量','自发货数量'])
# 订单数量校准
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

#输出，输出的时候要将已校对-P，已校对-check的订单去掉，因为这些都是检查过没有错误的，相当于封存
empty2 = empty2.drop_duplicates(['id', '登记日期', '更新日期', '登记人', '店铺', '订单号', 'SKU', '序号', '订单状态', '顾客反馈', '客服操作', 'Refund', '备注'],keep = 'last')
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
# df_empty4 = df_empty4.drop(['登记数量','自发货数量','index','数量','补寄数量'],axis = 1)
df_empty = df_empty.drop_duplicates()
empty2.to_excel(excel_writer="D:/work/订单数量校准/sku个数匹配且有误/pysku个数匹配且有误"+now+r"py.xlsx", index=False,encoding='utf-8')
df_empty1.to_excel(excel_writer="D:/work/订单数量校准/订单不存在/py订单不存在"+now+r"py.xlsx", index=False,encoding='utf-8')
df_empty4.to_excel(excel_writer="D:/work/订单数量校准/sku个数不匹配且有误/pysku个数不匹配且有误"+now+r"py.xlsx", index=False,encoding='utf-8')
df_empty.to_excel(excel_writer="D:/work/订单数量校准/订单数量校准/py订单数量校准"+now+r"py.xlsx", index=False,encoding='utf-8')





