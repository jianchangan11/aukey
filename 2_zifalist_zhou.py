import pandas as pd
from database_read import read
from sqlalchemy import create_engine

# 新
connect1 = create_engine(
    f'mysql+pymysql://{"test1"}:{"123456"}@{"rm-bp15wb9k07q3p10q6yo.mysql.rds.aliyuncs.com"}:{3306}/{"inventory"}?charset=utf8')


#对刚刚处理好的数据进行订单号优化 每个平台订单号的位数都是限定的
originzifa = read('inventory', '自发货源数据')
print(originzifa)
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
print(newzifa)

print('处理订单号完成')

#这个时间主要是方便提供查询 大致知道这一单是什么时候的 大致知道这一单最早发货和最后售后差了多久 也可以考虑是否优化
#这个看了订单号和SKU
origintime = pd.DataFrame(newzifa, columns=['平台订单号', 'SKU', '发货时间'])
origintime = origintime.sort_values(by=['发货时间'])
origintime = origintime.reset_index(drop=True)
origintime1 = origintime.drop_duplicates(subset=['平台订单号', 'SKU'], keep='first')
origintime1 = origintime1.reset_index(drop=True)
origintime2 = origintime.drop_duplicates(subset=['平台订单号', 'SKU'], keep='last')
origintime2 = origintime2.reset_index(drop=True)
origintime1 = origintime1.rename(columns={'发货时间': '最早时间'})
origintime2 = origintime2.rename(columns={'发货时间': '最晚时间'})
or_time = pd.merge(origintime1, origintime2, how='outer', on=['平台订单号', 'SKU'])
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
origin_zifaout = pd.pivot_table(origin_zifa_a, values=['数量'], index=['平台订单号', '账号', 'SKU'], aggfunc='sum')
origin_zifaout = origin_zifaout.reset_index()
print(origin_zifaout)
# 补寄清单数量整理
buji_zifaout = pd.pivot_table(origin_zifa_b, values=['数量'], index=['平台订单号', '账号', 'SKU'], aggfunc='sum')
buji_zifaout = buji_zifaout.reset_index()
buji_zifaout = buji_zifaout.rename(columns={'数量': '补寄数量'})
print(buji_zifaout)

#以订单号SKU为维度的简单表
originlast = pd.merge(origin_zifaout, buji_zifaout, how='outer', on=['平台订单号', '账号', 'SKU'])
originlast = originlast.merge(or_time, how='left', on=['平台订单号', 'SKU'])
originlast.to_sql("自发货数据", connect1, if_exists='replace', index=False)

# 正常清单数量整理
origin_zifaout_a = pd.pivot_table(origin_zifa_a, values=['数量'], index=['平台订单号', '账号'], aggfunc='sum')
origin_zifaout_a = origin_zifaout_a.reset_index()
print(origin_zifaout_a)
# 补寄清单数量整理
buji_zifaout_a = pd.pivot_table(origin_zifa_b, values=['数量'], index=['平台订单号', '账号'], aggfunc='sum')
buji_zifaout_a = buji_zifaout_a.reset_index()
buji_zifaout_a = buji_zifaout_a.rename(columns={'数量': '补寄数量'})
print(buji_zifaout)
#仅以订单号为维度的简单表
originlastb = pd.merge(origin_zifaout_a, buji_zifaout_a, how='outer', on=['平台订单号', '账号'])

originlastb = originlastb.merge(or_timex, how='left', on=['平台订单号'])

originlastb.to_sql("自发货数据new", connect1, if_exists='replace', index=False)
