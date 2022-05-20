import pandas as pd
from database_read import read
from sqlalchemy import create_engine
import re
from datetime import datetime
import time
origin = read('after_sales','new_after_salesv2')
originzifa = pd.DataFrame()
amazon = ['赫曼', '信盒', '宫本', '维禄','玲琅', '驰甬', '治润',]
#读取绩效管理表中的退货数据，并将他拼接在一起
for x in range(len(amazon)):
    jixiao = pd.read_excel(r'D:/work/绩效管理表/%s店铺绩效管理表-新.xlsx' % amazon[x],sheet_name = '退货' )
    jixiao['店铺'] = amazon[x]
    origin_zifa = pd.DataFrame(jixiao, columns=['订单号', '公司SKU', '快递方', '负责人','退货单号', '运输状态','退货时间','店铺'])
    originzifa = originzifa.append(origin_zifa)
    originzifa = originzifa.reset_index(drop=True)
originzifa.head(2)
#筛选出成功签收，运输途中，到达代取的订单
originzifa = originzifa[originzifa['运输状态'].str.contains('成功签收')|originzifa['运输状态'].str.contains('运输途中')|originzifa['运输状态'].str.contains('到达代取')]

dengji_buji2 = pd.DataFrame()
inventory_in_buji2 =  pd.DataFrame()
#筛选出不在售后登记的退货订单，即退货漏登记
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



