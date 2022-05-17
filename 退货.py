import pandas as pd

from database_read import read
from sqlalchemy import create_engine
import re
from datetime import datetime

import time
#分文件导出
# originzifa = pd.DataFrame()
# amazon = ['赫曼', '信盒', '宫本', '维禄','玲琅', '驰甬', '治润',]
# origin = read('after_sales','new_after_salesv2')
# for x in range(len(amazon)):
#     print(x)
#     jixiao = pd.read_excel(r'D:/work/绩效管理表/%s店铺绩效管理表-新 (3).xlsx' % amazon[x],sheet_name = '退货' )
#     print(x)
#     print(amazon[x])
#     jixiao = jixiao[jixiao['运输状态'].str.contains('成功签收')|jixiao['运输状态'].str.contains('运输途中')|jixiao['运输状态'].str.contains('到达代取')]
#     dengji_buji1 = pd.DataFrame()
#     inventory_in_buji1 =  pd.DataFrame()
#     for i in range(len(jixiao)):
#         if (jixiao.iloc[i,0] in origin['订单号'].tolist()):
#
#             origin_output1 = pd.DataFrame(jixiao[jixiao['订单号']==jixiao.iloc[i,0]])
#             dengji_buji1=dengji_buji1.append(origin_output1)
#         else:
#
#             origin_output = pd.DataFrame(jixiao[jixiao['订单号']==jixiao.iloc[i,0]])
#             inventory_in_buji1=inventory_in_buji1.append(origin_output)
#     now = time.strftime("%Y-%m-%d-%H_%M",time.localtime(time.time()))
#     dengji_buji1.to_excel(excel_writer="D:/work/绩效管理表/退货已登/"+now+r"%s退货后已登记.xlsx" % amazon[x], index=False,encoding='utf-8')
#     inventory_in_buji1.to_excel(excel_writer="D:/work/绩效管理表/退货漏登/"+now+r"%s退货售后漏登记.xlsx" % amazon[x], index=False,encoding='utf-8')
originzifa = pd.DataFrame()
amazon = ['赫曼', '信盒', '宫本', '维禄','玲琅', '驰甬', '治润',]
for x in range(len(amazon)):
    jixiao = pd.read_excel(r'D:/work/绩效管理表/%s店铺绩效管理表-新 (3).xlsx' % amazon[x],sheet_name = '退货' )
#     print(jixiao[jixiao['订单号'] == '112-2860662-8515429'])
    jixiao['店铺'] = amazon[x]
    origin_zifa = pd.DataFrame(jixiao, columns=['订单号', '公司SKU', '快递方', '负责人','退货单号', '运输状态','退货时间','店铺'])
    originzifa = originzifa.append(origin_zifa)
    originzifa = originzifa.reset_index(drop=True)
originzifa.head(2)

originzifa = originzifa[originzifa['运输状态'].str.contains('成功签收')|originzifa['运输状态'].str.contains('运输途中')|originzifa['运输状态'].str.contains('到达代取')]

dengji_buji2 = pd.DataFrame()
inventory_in_buji2 =  pd.DataFrame()
for i in range(len(originzifa)):
    if (originzifa.iloc[i,0] in origin['订单号'].tolist()):
        origin_output2 = pd.DataFrame(originzifa[originzifa['订单号']==originzifa.iloc[i,0]])
        dengji_buji2=dengji_buji2.append(origin_output2)
    else:

        origin_output2 = pd.DataFrame(originzifa[originzifa['订单号']==originzifa.iloc[i,0]])
        inventory_in_buji2=inventory_in_buji2.append(origin_output2)
now = time.strftime("%Y-%m-%d-%H_%M_%S",time.localtime(time.time()))
dengji_buji2.to_excel(excel_writer="D:/work/绩效管理表/退货已登/py"+now+r"退货后已登记.xlsx" , index=False,encoding='utf-8')
inventory_in_buji2.to_excel(excel_writer="D:/work/绩效管理表/退货漏登/py"+now+r"退货售后漏登记.xlsx" , index=False,encoding='utf-8')



