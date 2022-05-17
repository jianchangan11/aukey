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
from database_read import read, creat_id
from sqlalchemy.types import VARCHAR, Float, Integer, Date, Numeric, Text
# 翻译
fankui = {
    "1-1-C": "未上网(未收到)",
    "1-2-X": "缺货(未收到)",
    "1-3-K": "显示签收(未收到)",
    "1-4-K": "快递停滞/送错州/要求自提(未收到)",
    "1-5-K": "快递中途破损/退回(未收到)",
    "1-6-X": "未录入自发货(未收到)",
    "2-1-C": "仓库发错(发错货)",
    "2-2-Z": "工厂装错(发错货)",
    "3-1-M": "发货后改地址(改地址)",
    "3-2-M": "询问物流(改地址)",
    "4-1-M": "无理由退货(不想要)",
    "4-2-X": "网页描述与实物不符(不想要)",
    "4-3-M": "不符合预期/不满意(不想要)",
    "4-4-K": "超时送达(不想要)",
    "4-5-Z": "有异味(不想要)",
    "4-6-M": "重复下单(不想要)",
    "4-7-X": "价格贵(不想要)",
    "4-8-F": "无法安装/不会用(不想要)",
    "4-9-M": "未授权购买/不小心购买(不想要)",
    "4-10-K": "快递态度恶劣(不想要)",
    "4-11-F": "需要床箱/床垫不适合(不想要)",
    "4-12-M": "尺寸买错(不想要)",
    "4-13-M": "更换付款方式(不想要)",
    "4-14-M": "其他(不想要)",
    "5-1-Y": "断裂(包装破损)",
    "5-2-Y": "弯折/开裂/变形(包装破损)",
    "5-3-Y": "破损/破洞(包装破损)",
    "5-4-Y": "松动/掉落(包装破损)",
    "5-5-Y": "腐蚀/发霉/生虫/生锈/污渍(包装破损)",
    "5-6-Y": "划痕/凹痕/凸痕/压痕/褶皱(包装破损)",
    "5-7-Y": "少件(包装破损)",
    "6-1-Z": "断裂(包装未破损)",
    "6-2-Z": "弯折/开裂/变形(包装未破损)",
    "6-3-Z": "破损/破洞(包装未破损)",
    "6-4-Z": "松动/掉落(包装未破损)",
    "6-5-Z": "腐蚀/发霉/生虫/生锈/污渍(包装未破损)",
    "6-6-Z": "划痕/凹痕/凸痕/压痕/褶皱(包装未破损)",
    "6-7-S": "少件(包装未破损)",
    "6-8-S": "错件(包装未破损)",
    "6-9-Z": "异味(包装未破损)",
    "6-10-Z": "走线歪斜/纽扣歪斜(包装未破损)",
    "6-11-Z": "孔位错误/缺失(包装未破损)",
    "6-12-Z": "色差(包装未破损)",
    "6-13-Z": "配件功能失效(包装未破损)",
    "6-14-S": "缺少编号/说明书或标签填错(包装未破损)",
    "7-1-Z": "断裂(使用后故障)",
    "7-2-Z": "弯折/开裂/变形(使用后故障)",
    "7-3-Z": "配件松脱(使用后故障)",
    "7-4-Z": "功能故障(使用后故障)",
    "7-5-Z": "产品不稳固(使用后故障)",
    "7-6-Z": "腐蚀/发霉/生虫/生锈(使用后故障)",
    "7-7-Z": "异味(使用后故障)",
    "7-8-Z": "噪音(使用后故障)",
    "8-1-M": "问题已反馈未解决(差评)",
    "8-2-M": "问题未反馈(差评)",
    "9-1-P": "描述/图片不符合",
    "9-2-P": "面单问题(wayfair平台问题)",
    "9-3-P": "平台客服拒绝沟通(wayfair平台问题)",
    "0-0-0": "未知问题(wayfair平台问题)",
}
yuanying = {
    "1-1-0": "退全款",
    "1-2-0": "退部分款",
    "2-1-0": "出仓前截回",
    "2-2-0": "半路拦截退货",
    "2-3-0": "拒收退货",
    "2-4-0": "地址不明/错误退货",
    "2-5-0": "买家自行退货",
    "2-6-0": "买家使用我司Return label退货",
    "2-7-0": "买家使用Amazon Return label退货",
    "2-8-0": "买家使用仓库Return label退货",
    "2-9-0": "买家上门取件退货",
    "3-1-0": "海外仓补寄新件",
    "3-2-0": "海外仓补寄退件",
    "3-3-0": "海外仓补寄破损件",
    "3-4-0": "海外仓补寄配件",
    "3-5-0": "国内补寄配件",
    "3-6-0": "国内补寄电子说明书",
    "4-1-0": "label created暂不能修改",
    "4-2-0": "in transit已申请修改地址",
    "4-3-0": "修改失败",
    "5-1-0": "等待买家补充信息",
    "5-2-0": "等待开发提供方案",
    "5-3-0": "目前方案买家不满意",
    "5-4-0": "已解决",
}
zeren = {
    "C": '仓库',
    "F": '开发',
    "K": '快递',
    "S": '少配件',
    "X": '销售',
    "Y": '运输破损',
    "Z": '质量',
    "M": '买家',
    "U": '未知',
}
dianpu = {
    "信盒-法国": 'amazon信盒欧线',
    "信盒-意大利": 'amazon信盒欧线',
    "信盒-西班牙": 'amazon信盒欧线',
    "赫曼": 'amazon赫曼',
    "信盒": 'amazon信盒',
    "宫本": 'amazon宫本',
    "驰甬": 'amazon驰甬',
    "维禄": 'amazon维禄',
    "森月": 'amazon森月',
    "玲琅": 'amazon玲琅',
    "治润": 'amazon治润',
    "哒唛旺": 'amazon哒唛旺',
    "简砾": 'amazon简砾',
    "旗辰": 'amazon旗辰',
    "赛迦曼": 'amazon赛迦曼',
    "启珊": 'amazon启珊',
    "Central_Power_International_Limited": "amazoncpower",
    "Walmart-优瑞斯特": 'walmart优瑞斯特',
    "Walmart-宫本": 'walmart宫本',
    "Walmart-赫曼": 'walmart赫曼',
    "Walmart-信盒": 'walmart信盒',
    "eBay-治润": 'ebay治润',
    "eBay-雅秦": 'eBay雅秦',
    "eBay-玲琅": 'ebay玲琅',
    "Wayfair-信盒": 'wayfair信盒',
    "Wayfair-维禄": 'wayfair维禄',
    "Nextfur-Shopify": 'shopifynextfur',
}

def tran(my_str, rep):
    rep = dict((re.escape(k), v) for k, v in rep.items())
    pattern = re.compile("|".join(rep.keys()))
    my_str = pattern.sub(lambda m: rep[re.escape(m.group(0))], my_str)
    return my_str
# 读取sql售后表里的原始数据
origin = read('after_sales', 'new_after_salesv2')
origin['客服操作'] = origin['客服操作'].apply(
    lambda x: tran(x, yuanying).replace('&', ',').replace("#", "Part").replace('$', 'Part').replace('num', '数量'))
origin['顾客反馈'] = origin['顾客反馈'].apply(
    lambda x: tran(x, fankui).replace('&', ',').replace("#", "Part").replace('$', 'Part').replace('num', '数量'))
date = datetime.date.today()
# 输出excel
origin.to_excel(excel_writer="D:/work/after_sale/总售后%s.xlsx" % date,
                        index=False,
                        encoding='utf-8', freeze_panes=[1, 1])
