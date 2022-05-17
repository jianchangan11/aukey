import datetime
# 自己写的方法
import pandas as pd
from dateutil.parser import parse
from new_asin import new_asin
from origin_to_sql import origin_to_sql, origin_sql_total
from origin_to_skusale import origin_to_skusale
from check import check
from sale_to_sql import sale_sql_temp, sale_sql_total
from dataprocess import read
# 变量
# 当天下载的数据截止至前一天，如当天中国时间为4.7，美国时间是4.6.那么下载的销售数据截止4.6（美国时间），因为美国与中国时差 差 1 .
# 由于美国销售的当天是4.6 ，当天的广告数据是不准确的，所以广告数据要往前推一天，即4.5。
# 广告时间段
AdDate = datetime.date(2022,5, 11)
CurrentAdDate = datetime.date(2022, 5, 14)
# 销售时间段
XsDate = AdDate + datetime.timedelta(days=1)     #销售数据晚一天
CurrentXsDate = CurrentAdDate + datetime.timedelta(days=1)
Length = CurrentXsDate - XsDate
# 所含店铺
amazon = ['amazon赫曼', 'amazon信盒', 'amazon宫本', 'amazon维禄', 'amazon森月', 'amazon简砾',
          'amazon玲琅', 'amazon哒唛旺', 'amazon驰甬', 'amazon治润', 'amazoncpower', 'amazon旗辰', 'amazon启珊','amazon赛迦曼']  #'amazon赛迦曼' 暂停下载
wayfair = ['wayfair信盒', 'wayfair维禄']
walmart = ['walmart优瑞斯特', 'walmart赫曼', 'walmart信盒', 'walmart宫本']
ebay = ['ebay治润', 'ebay雅秦', 'ebay玲琅']
shopify = ['shopifynextfur']
total = amazon + wayfair + walmart + ebay + shopify
# 文件地址
sale_url = 'D:/work/sale/5.11-5.15/'

# 匹配表要匹配的字段
#id_sort = ['KEY', 'sku序号', 'ASIN', '渠道SKU', '渠道sku', "Item Id"]
id_sort = [ 'sku序号', 'ASIN', '渠道sku', "Item Id"]
# skusale输出的变量
value_1 = [u'销量', u'coupon', u'销售额', u'成本单价']  #算毛利和周转
value_2 = [u'销量']
# 原始数据处理的字段
amazon_select = ["order-status", "quantity", "sku", "quantity", "item-price", "shipping-price",
                 "item-promotion-discount", "fulfillment-channel", "purchase-date", "amazon-order-id","sales-channel"]
amazon_ad_select = ["Advertised SKU", "Spend", "Date"]
wayfair_select = ["Order Status", "Item Number", "Quantity", "Wholesale Price", "PO Date", "PO Number", "Warehouse Name"]
walmart_select = ["Status", "SKU", "Qty", "Item Cost", "Shipping Cost", "Order Date", "PO#", "Fulfillment Entity"]
walmart_ad = ["Item Id", "Ad Spend", "Date"]
ebay_select = ["Custom Label", "Total Price", "Sold For", "Quantity", "Sale Date", "Shipping And Handling",
               "Order Number"]
ebay_ad = ["Item ID", "Ad fees", "Date sold"]
shopify_select = ["Cancelled at", "Lineitem sku", "Created at", "Name", "Lineitem quantity", "Lineitem price",
                  "Discount Amount", "Taxes", "Shipping", "Created at", "Name"]
shopify_ad = ["SKU", "Ad Cost", "Date"]
cost_price = ['成本单价4.12', '成本单价详情', '建议售价', '建议售价详情']
# 匹配表需要匹配的字段
id_sort = ['KEY', 'sku序号', 'ASIN', '渠道SKU', '渠道sku', "Item Id"]
# 匹配表更改
# 一段时间的数据
match_origin = read('match', '新匹配表')
match_origin['结束时间'] = match_origin['结束时间'].replace('9999-12-31', '2100-01-01')
print(datetime.datetime.combine(AdDate, datetime.datetime.min.time()))
match_origin['结束时间'] = pd.to_datetime(match_origin['结束时间'])
# 筛选日期
match_origin = match_origin[match_origin['结束时间'] > datetime.datetime.combine(AdDate, datetime.datetime.min.time())]
match_origin = match_origin.drop_duplicates(subset=['渠道sku', '店铺'])


# 主函数
if __name__ == '__main__':

    # match = read('match', '新匹配表')
    # print(match['店铺'] == 'walmart宫本')

    # match = match[match['店铺'] == 'amazon赫曼']
    # print(match)
    # 新增渠道sku

    # new_asin(amazon, walmart, ebay, wayfair, shopify, sale_url, match_origin)

    # 原始数据存入origin数据库
    # origin_to_sql(amazon, walmart, ebay, wayfair, sale_url, AdDate, CurrentAdDate)
    # origin_sql_total(amazon, walmart, ebay, wayfair, AdDate)
    # origin = pd.read_excel('E:\sale\wayfair维禄销售.xlsx')

    # # 跑销售数据
    # origin = pd.read_excel(sale_url + '%s销售.xlsx' % amazon[0], sheet_name=0, usecols=amazon_select)
    # if len(origin) > 0:
    #     for x in range(len(origin)):
    #         origin.loc[x, 'purchase-date'] = parse(origin.loc[x, 'purchase-date']) - datetime.timedelta(hours=8)
    #         origin.loc[x, 'purchase-date'] = (origin.loc[x, 'purchase-date']).date()
    #     origin = origin[(origin['purchase-date'] <= CurrentXsDate) & (origin['purchase-date'] >= AdDate)]
    # # 原始数据转sku序号销售W
    origin_to_skusale(amazon, walmart, ebay, wayfair, shopify, sale_url,
                      XsDate, AdDate, CurrentXsDate, CurrentAdDate, id_sort,
                      amazon_select, amazon_ad_select,
                      wayfair_select, walmart_select, walmart_ad, ebay_select,
                      ebay_ad, shopify_select, shopify_ad, cost_price, match_origin)

    # 校准表格数据
    check(amazon, walmart, wayfair, ebay, Length)
    print('运行结束')
    # 存储销售数据到sale数据库
    # sale_sql_temp(amazon, wayfair, walmart, ebay, shopify, total, AdDate)
    # sale_sql_total(amazon, wayfair, walmart, ebay, shopify, total, AdDate, CurrentAdDate)

    # 处理售后数据到after_sale_total数据库

    # 合并销售数据和售后数据到sku_sale数据库

    # 生成sku汇总表格和销售汇总表格

    # 生成销售汇总网页数据到sku_sale_app数据库

    # 生成sku汇总数据
