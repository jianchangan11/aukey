import csv
import os
import pymysql
from pathlib import Path
from openpyxl import Workbook
import pandas as pd
import re
import numpy as np


#   txt读取
def txt2xl(txtFile, xlFile):
    if Path(xlFile).exists():
        os.remove(xlFile)
    wb = Workbook()
    ws = wb.worksheets[0]
    ws.title = '销售数据'
    with open(txtFile, 'rt', encoding='utf-8') as data:
        reader = csv.reader(data, delimiter='\t')
        for row in reader:
            ws.append(row)
    wb.save(xlFile)
    return xlFile


# 读取数据库里的表
def read(basename, tablename):
    connc = pymysql.Connect(
        host='rm-bp15wb9k07q3p10q6yo.mysql.rds.aliyuncs.com',
        user='test1',
        password='123456',
        database='%s' % basename,
        port=3306,
        charset='utf8'
    )

    cur = connc.cursor()
    sql = 'select * from `%s`;' % tablename
    cur.execute(sql)

    des = cur.description
    title = []
    for j in range(len(des)):
        title.append(des[j][0])

    origin = cur.fetchall()
    origin = pd.DataFrame(list(origin))
    origin.columns = title
    return origin


# 数据库创造id
def creat_id(engine, db_name, table_name):
    # 增加主键
    with engine.connect() as con:
        con.execute("""ALTER TABLE `{}`.`{}` \
                ADD COLUMN `id` INT NOT NULL AUTO_INCREMENT FIRST, \
                ADD PRIMARY KEY (`id`);"""
                    .format(db_name, table_name))


# 数据库判断表是否存在
def table_exists(basename, table_name):
    connect = pymysql.Connect(
        host='rm-bp15wb9k07q3p10q6yo.mysql.rds.aliyuncs.com',
        user='test1',
        password='123456',
        database='%s' % basename,
        port=3306,
        charset='utf8'
    )
    con = connect.cursor()
    con.execute('use %s' % basename)

    sql = "show tables;"
    con.execute(sql)
    tables = [con.fetchall()]
    table_list = re.findall('(\'.*?\')', str(tables))
    table_list = [re.sub("'", '', each) for each in table_list]
    con.close()
    connect.close()
    if table_name in table_list:
        return 1  # 存在返回1
    else:
        return 0  # 不存在返回0


# txt转excel
def amazon_change(amazon, sale_url):
    for i in range(len(amazon)):
        original = sale_url + '%s销售.txt' % amazon[i]
        changed = sale_url + '%s销售.xlsx' % amazon[i]
        newInfile = txt2xl(original, changed)


# 新增sku
def add(store, sale_url, store_attribute, store_attribute_index, attribute, attribute_index, type, match_origin ):
    # 读取 ebay 店铺
    if store in ['ebay治润', 'ebay雅秦', 'ebay玲琅']:
        sku_match = pd.read_excel(sale_url + '%s销售.xlsx' % store)
    #  sku_match = read('origin', '%s销售2022' % store)


    # 连接数据库
    connc = pymysql.Connect(
        host='rm-bp15wb9k07q3p10q6yo.mysql.rds.aliyuncs.com',
        user='test1',
        password='123456',
        database='match',
        port=3306,
        charset='utf8'
    )

    if Path(sale_url + '%s%s.xlsx' % (store, type)).exists():
        origin = pd.read_excel(sale_url + '%s%s.xlsx' % (store, type))  # 读取数据
        if (store in ['amazon赫曼', 'amazon信盒', 'amazon宫本', 'amazoncpower', 'amazon驰甬', 'amazon简砾', 'amazon旗辰','amazon玲琅',
                      'amazon哒唛旺', 'amazon森月', 'amazon治润', 'amazon维禄', 'amazon启珊', 'amazon赛迦曼']) & (type == '销售'):
            origin = origin.drop(origin[origin["sales-channel"] == 'Non-Amazon'].index)
            origin = origin.drop(origin[origin["item-price"].isnull()].index)
            origin = origin.drop(origin[origin["item-price"] == 0].index)

        match = match_origin   # 从数据库match中读取新匹配表，并命名为match
        match = match[match['店铺'] == store]    # store是各个电商平台的店铺，如Amazon
        match = match.drop_duplicates(subset=attribute)
        match = match.drop(match[match[attribute_index].isnull()].index)
        match = match.reset_index(drop=True)  # match 表去重，删空余值，重置index
        if len(store_attribute) == 2:
            origin = origin.loc[:, [store_attribute[0], store_attribute[1]]]  # 读取下载的数据，并截取sku 和 asin
            origin = origin.drop_duplicates(subset=[store_attribute[0], store_attribute[1]])  # 去重
        else:
            origin = origin.loc[:, [store_attribute[0]]]
            origin = origin.drop(origin[origin[store_attribute[0]] == ""].index)


            origin = origin.drop_duplicates(subset=[store_attribute[0]])
        origin = origin.reset_index(drop=True)   # 最终得到的origin 数据是只有sku 和 asin 的
        for j in range(len(origin)):
            if (store not in ['ebay治润', 'ebay雅秦', 'ebay玲琅']) or (type == '销售'):
                # attribute_Index 是 渠道 sku
                asin = match[match[attribute_index] == str(origin.iloc[j, 0])].index.tolist()

            else:
                # 只有ebay 广告执行以下语句，因为ebay广告包含多条sku
                asin = match[match[attribute_index].str.contains(str(origin.iloc[j, 0]))].index.tolist()
            if str(asin) != "[]":
                continue
            else:
                # 在匹配表中找不到对应的sku执行以下语句 即寻找新增的sku
                if str(origin.iloc[j, 0]) != 'nan':
                    # print(origin.iloc[j, 0])
                    if len(store_attribute) == 2:  # ebay 销售和Amazon广告、销售执行以下语句
                        print(store, ' ', origin.iloc[j, 0], origin.iloc[j, 1])
                    else:  # ebay 广告 、 walmart 广告和销售、wayfair销售会执行以下语句
                        if store not in ['ebay治润', 'ebay雅秦', 'ebay玲琅']:
                            # walmart 广告和销售、wayfair销售会执行以下语句 shopifynextfur会执行以下语句
                            print(store, ' ', origin.iloc[j, 0])
                        else:
                            # ebay 广告会执行以下语句
                            index = sku_match[sku_match['Item Number'] == origin.iloc[j, 0]].index.tolist()
                           # print('index', index)  #  输出结果为多个index
                           #print('aaa', sku_match.loc[index, 'Custom Label'])  # 多个index对应的custom label为同一个sku
                            if index:
                                # 输出 asin 和 sku
                                print(store, ' ', origin.iloc[j, 0], sku_match.loc[index[0], 'Custom Label'])
                                # print('custom label')
                            else:
                                print(store, ' ', origin.iloc[j, 0])
                                # print('new add')
            # if (store not in ['ebay治润', 'ebay雅秦', 'ebay玲琅']) or (type == '销售'):
            #     asin = match[match['ASIN'] == str(origin.iloc[j, 1])].index.tolist()
            # if str(asin) != "[]":
            #         continue
            # else:
            #     if str(origin.iloc[j, 0]) != 'nan':
            #         if len(store_attribute) == 2:
            #             print(store, ' ', origin.iloc[j, 1], origin.iloc[j, 0])
                    # sql = "insert into `新匹配表`(`渠道sku`)" + "values(%s)"
                    # add_data = (origin.iloc[j, 0])
                    # cur.execute(sql, add_data)
                    # connc.commit()
    else:
        print(sale_url + '%s%s.xlsx不存在' % (store, type))
    connc.close()


# sku——sale销售金额

# 数据预处理，转换为同一格式
def amazon_to_model(origin, index_1):
    # 清洗数据，排除cancel项和0项。
    origin = origin.drop(origin[origin["order-status"] == 'Cancelled'].index)
    origin = origin.drop(origin[origin["sales-channel"] == 'Non-Amazon'].index)
    origin = origin.drop(origin[origin["item-price"].isnull()].index)
    origin = origin.drop(origin[origin["item-price"] == 0].index)
    origin = origin.drop(origin[origin["quantity"] == 0].index)
    origin = origin.drop('order-status', axis=1)
    origin = origin.where(origin.notnull(), 0)
    origin = origin.reset_index(drop=True)
    origin = origin[["sku", "quantity", "item-price", "shipping-price", "item-promotion-discount",
                     "purchase-date", "fulfillment-channel"]]
    # 转换成model类型
    amazon_model = pd.DataFrame(columns=(index_1, '销量', 'coupon', '销售额', 'date', 'type', '自发货', '平台发货'))
    for i in range(len(origin)):
        if origin.iloc[i, 6] == 'Merchant':#!!!!!!!!!!!!!!!!!!!!!!!!!
            temp = pd.Series(
                {index_1: origin.iloc[i, 0], '销量': float(origin.iloc[i, 1]), 'coupon': float(origin.iloc[i, 4]),
                 '销售额': float(origin.iloc[i, 2] + origin.iloc[i, 3]), 'date': origin.iloc[i, 5], 'type': origin.iloc[i, 6], '自发货': 1, '平台发货':0})
        else:
            temp = pd.Series(
                {index_1: origin.iloc[i, 0], '销量': float(origin.iloc[i, 1]), 'coupon': float(origin.iloc[i, 4]),
                 '销售额': float(origin.iloc[i, 2] + origin.iloc[i, 3]), 'date': origin.iloc[i, 5], 'type': origin.iloc[i, 6], '自发货': 0, '平台发货':1})
        # if temp["Merchant"] == 1 & int(temp["销量"]) > int(temp["Merchant"]):#!!!!!!!!!!
        temp['自发货'] = temp["自发货"] * temp['销量']
        temp['平台发货'] = temp["平台发货"] * temp['销量']
        amazon_model = amazon_model.append(temp, ignore_index=True)
    return amazon_model

def ad_to_model(origin, index_1):
    # 清洗数据，排除cancel项和0项。
    origin = origin.drop(origin[origin["Spend"] == 0].index)
    origin = origin.where(origin.notnull(), 0)
    origin = origin.reset_index(drop=True)
    origin = origin[["Advertised SKU", "Spend", "Date"]]
    # 转换成model类型
    ad_model = pd.DataFrame(columns=(index_1, '销量', 'coupon', '销售额', 'date', 'type'))
    for i in range(len(origin)):
        temp = pd.Series(
            {index_1: origin.iloc[i, 0], '销量': 0.000, 'coupon': 0.000,
             '销售额': -float(origin.iloc[i, 1]), 'date': origin.iloc[i, 2], 'type': 'FBM'})
        ad_model = ad_model.append(temp, ignore_index=True)
    return ad_model

def wayfair_to_model(origin, index_1):
    # 清洗数据，排除cancel项和0项。
    origin = origin.drop(origin[origin["Order Status"] == 'Cancelled'].index)
    origin = origin.drop(origin[origin["Quantity"] == 0].index)
    origin = origin.drop("Order Status", axis=1)
    origin["Warehouse Name"] = origin["Warehouse Name"].where(origin["Warehouse Name"].notnull(), '0')
    origin = origin.where(origin.notnull(), 0)
    origin = origin.reset_index(drop=True)
    Order = ["Item Number", "Quantity", "Wholesale Price", "PO Date", "Warehouse Name"]
    origin = origin[Order]
    # 转换成model类型
    waifair_model = pd.DataFrame(columns=(index_1, '销量', 'coupon', '销售额', 'date', 'type', '自发货', '平台发货'))
    for i in range(len(origin)):
        if (origin.iloc[i, 4].find('CG') != -1) or (origin.iloc[i, 4] == '0'):  # !!!!!!!!!!!!!!!!!!!!!!!!!
            temp = pd.Series(
                {index_1: origin.iloc[i, 0], '销量': float(origin.iloc[i, 1]), 'coupon': 0.000,
                 '销售额': float(origin.iloc[i, 2] * float(origin.iloc[i, 1])), 'date': origin.iloc[i, 3],
                 'type': 'wayfair', '自发货': 0, '平台发货':1 })
        else:
            temp = pd.Series(
                {index_1: origin.iloc[i, 0], '销量': float(origin.iloc[i, 1]), 'coupon': 0.000,
                 '销售额': float(origin.iloc[i, 2] * float(origin.iloc[i, 1])), 'date': origin.iloc[i, 3],
                 'type': 'wayfair', '自发货':1, '平台发货': 0})
        temp['自发货'] = temp["自发货"] * temp['销量']
        temp['平台发货'] = temp["平台发货"] * temp['销量']
        waifair_model = waifair_model.append(temp, ignore_index=True)
    return waifair_model


def walmart_to_model(origin, index_1):
    # 清洗数据，排除cancel项和0项。
    origin = origin.drop(origin[origin["Status"] == 'Cancelled'].index)
    origin = origin.drop(origin[origin["Qty"] == 0].index)
    origin = origin.drop("Status", axis=1)
    origin = origin.where(origin.notnull(), 0)
    origin = origin.reset_index(drop=True)
    Order = ["SKU", "Qty", "Item Cost", "Shipping Cost", "Order Date", "Fulfillment Entity"]
    origin = origin[Order]
    # 转换成model类型
    walmart_model = pd.DataFrame(columns=(index_1, u'销量', u'coupon', u'销售额', u'Shipping Cost', 'date', 'type', '自发货', '平台发货'))
    for i in range(len(origin)):
        if origin.iloc[i, 5] == 'Seller Fulfilled':  # !!!!!!!!!!!!!!!!!!!!!!!!!
            temp = pd.Series(
                {index_1: origin.iloc[i, 0], '销量': float(origin.iloc[i, 1]), 'coupon': 0.000,
                 '销售额': float(origin.iloc[i, 2]) * float(origin.iloc[i, 1]) + float(origin.iloc[i, 3]),
                 'date': origin.iloc[i, 4], 'type': 'FBM', '自发货':1, '平台发货': 0})
        else:
            temp = pd.Series(
                {index_1: origin.iloc[i, 0], '销量': float(origin.iloc[i, 1]), 'coupon': 0.000,
                 '销售额': float(origin.iloc[i, 2]) * float(origin.iloc[i, 1]) + float(origin.iloc[i, 3]),
                 'date': origin.iloc[i, 4], 'type': 'FBM', '自发货': 0, '平台发货': 1})
        temp['自发货'] = temp["自发货"] * temp['销量']
        temp['平台发货'] = temp["平台发货"] * temp['销量']

        walmart_model = walmart_model.append(temp, ignore_index=True)
    return walmart_model


def walmartad_to_model(origin, index_1):
    # 清洗数据，排除cancel项和0项。
    origin = origin.drop(origin[origin["Ad Spend"] == 0].index)
    origin = origin.where(origin.notnull(), 0)
    origin = origin.reset_index(drop=True)
    origin = origin[["Item Id", "Ad Spend", "Date"]]
    # 转换成model类型
    ad_model = pd.DataFrame(columns=(index_1, '销量', 'coupon', '销售额', 'date', 'type'))
    for i in range(len(origin)):
        temp = pd.Series(
            {index_1: str(origin.iloc[i, 0]), '销量': 0.000, 'coupon': 0.000,
             '销售额': -float(origin.iloc[i, 1]), 'date': origin.iloc[i, 2], 'type': 'FBM'})
        ad_model = ad_model.append(temp, ignore_index=True)
    return ad_model


def ebay_to_model(origin, index_1):
    # 清洗数据，排除cancel项和0项。
    origin["Custom Label"] = origin["Custom Label"].where(origin["Custom Label"].notnull(), 0)
    origin = origin.drop(origin[origin["Sold For"] == 0].index)
    origin = origin.drop(origin[origin["Custom Label"] == 0].index)
    origin = origin.reset_index(drop=True)
    Order = ["Custom Label", "Quantity", "Sold For", "Shipping And Handling", "Sale Date"]
    origin = origin[Order]
    # 转换成model类型
    ebay_model = pd.DataFrame(columns=(index_1, u'销量', u'coupon', u'销售额', 'date', 'type'))
    for i in range(len(origin)):
        temp = pd.Series(
            {index_1: origin.iloc[i, 0], '销量': float(origin.iloc[i, 1]), 'coupon': 0.000,
             '销售额': float(origin.iloc[i, 1]) * float(origin.iloc[i, 2]) + float(origin.iloc[i, 3]),
             'date': origin.iloc[i, 4], 'type': 'FBM'})
        ebay_model = ebay_model.append(temp, ignore_index=True)
    return ebay_model


def ebayad_to_model(origin, index_1):
    # 清洗数据，排除cancel项和0项。
    origin = origin.drop(origin[origin["Ad fees"] == 0].index)
    origin = origin.where(origin.notnull(), 0)
    origin = origin.reset_index(drop=True)
    origin = origin[["Item ID", "Ad fees", "Date sold"]]
    # 转换成model类型
    ad_model = pd.DataFrame(columns=(index_1, '销量', 'coupon', '销售额', 'date', 'type'))
    for i in range(len(origin)):
        temp = pd.Series(
            {index_1: str(origin.iloc[i, 0]), '销量': 0.000, 'coupon': 0.000,
             '销售额': -float(origin.iloc[i, 1]), 'date': origin.iloc[i, 2], 'type': 'FBM'})
        ad_model = ad_model.append(temp, ignore_index=True)
    return ad_model


def shopifyad_to_model(origin, index_1):
    # 清洗数据，排除cancel项和0项。
    origin = origin.where(origin.notnull(), 0)
    origin = origin.reset_index(drop=True)
    origin = origin[["SKU", "Ad Cost", "Date"]]
    # 转换成model类型
    ad_model = pd.DataFrame(columns=(index_1, '销量', 'coupon', '销售额', 'date', 'type'))
    for i in range(len(origin)):
        temp = pd.Series(
            {index_1: str(origin.iloc[i, 0]), '销量': 0.000, 'coupon': 0.000,
             '销售额': -float(origin.iloc[i, 1].astype(str).replace("$", "")), 'date': origin.iloc[i, 2], 'type': 'FBM'})
        ad_model = ad_model.append(temp, ignore_index=True)
    return ad_model


def shopify_to_model(origin, index_1):
    # 清洗数据，排除cancel项和0项。
    origin.drop_duplicates(subset=["Name", "Created at", "Lineitem sku"], inplace=True)
    origin["Cancelled at"] = origin["Cancelled at"].where(origin["Cancelled at"].notnull(), 0)
    origin["Discount Amount"] = origin["Discount Amount"].where(origin["Discount Amount"].notnull(), '')
    origin = origin.drop(origin[origin["Cancelled at"] != 0].index)
    origin = origin.reset_index(drop=True)
    Order = ["Lineitem sku", "Lineitem quantity", "Discount Amount", "Lineitem price", "Shipping", "Created at"]
    origin = origin[Order]
    for i in range(len(origin)):
        if origin.iloc[i, 2] == '':
            origin.iloc[i - 1, 2] = origin.iloc[i - 1, 2] / 2
            origin.iloc[i, 2] = origin.iloc[i - 1, 2]
    # 转换成model类型
    shopify_model = pd.DataFrame(columns=(index_1, u'销量', u'coupon', u'销售额', 'date', 'type'))
    for i in range(len(origin)):
        temp = pd.Series(
            {index_1: origin.iloc[i, 0], '销量': float(origin.iloc[i, 1]), 'coupon': float(origin.iloc[i, 2]),
             '销售额': (float(origin.iloc[i, 1]) * float(origin.iloc[i, 3]) + float(origin.iloc[i, 4])),
             'date': origin.iloc[i, 5], 'type': 'FBM'})
        shopify_model = shopify_model.append(temp, ignore_index=True)
    return shopify_model


# index_1是指ASIN映射到匹配表的字段
# index_2是指匹配表里的最后用到的识别字段
# value是指最后要输出的变量
def model_to_output(origin, match, index_1, index_2, cost_price, suggest_price):
    # 第一次处理asin并合并数量（相当于excel透视）
    # 匹配表用0替换空值
    match = match.where(match.notnull(), 0)
    # 用index_1匹配index_2
    a_s = pd.merge(origin, match, on=index_1, how='left')  # 将导出来的渠道sku表 与数据库匹配表进行合并
    a_s = a_s.drop(index_1, axis=1)
    a_s = a_s.drop('id', axis=1)
    # 处理index_2并合并数量
    # print(a_s.columns)
    # print(a_s,'\n------------------------\n')
    # print(a_s[['Merchant','Amazon']], '- - - - - - - - - - - - - - - - - - -')
    # print(type(a_s[['Merchant','Amazon']]))
    if '自发货' in a_s.columns.tolist():
        a_s = a_s.pivot_table(values=[u'销量', u'coupon', u'销售额', u'自发货', u'平台发货'],
                              index=[index_2, 'date', 'type'], fill_value=0, aggfunc='sum')
        order = [index_2, u'销量', u'coupon', u'销售额', 'date', 'type', '自发货',  '平台发货']
    else:
        a_s = a_s.pivot_table(values=[u'销量', u'coupon', u'销售额'],
                              index=[index_2, 'date', 'type'], fill_value=0, aggfunc='sum')
        order = [index_2, u'销量', u'coupon', u'销售额', 'date', 'type']
    df = a_s

    df = df.reset_index()  # 重建索引
    # print(df.columns)
    # print('\n', df, '\n****************************************')


    n = 0
    # AB箱通过建议售价判断销售占比
    suggest = read('sale', suggest_price)
    # 判断a是否存在于index_2中，并进行处理
    process_output = pd.DataFrame()
    for i in range(len(df)):
        process_output = process_output.append(df.iloc[i], ignore_index=True)
        process_output = process_output[order]

        if 'a' in str(df.iloc[i, 0]):
            # 用a拆分sku序号
            match_list = list(map(int, df.iat[i, 0].split("a")))   # iat 等价于 iloc ， 运行速度比 iloc 更快
            # 拆完的sku序号中不重复的值
            unique_list = np.unique(match_list)
            suggest_total = 0

            suggest_item_price = [0] * len(unique_list)
            for ix in range(len(unique_list)):
                num = match_list.count(unique_list[ix])
                if process_output.loc[i + n, 'type'] == 'wayfair':
                    suggest_item_price[ix] = int(suggest.loc[suggest['sku序号'] == int(unique_list[ix]), 'wayfair 建议售价'])
                elif process_output.loc[i + n, 'type'] == 'Amazon':
                    suggest_item_price[ix] = int(suggest.loc[suggest['sku序号'] == int(unique_list[ix]), 'FBA建议售价'])
                elif process_output.loc[i + n, 'type'] == 'Merchant':
                    suggest_item_price[ix] = int(suggest.loc[suggest['sku序号'] == int(unique_list[ix]), 'FBM建议售价'])
                else:
                    suggest_item_price[ix] = int(suggest.loc[suggest['sku序号'] == int(unique_list[ix]), 'FBM建议售价'])
                suggest_total += suggest_item_price[ix] * num
            for ix in range( len( unique_list ) ):
                process_output.iloc[i + n, 0] = int(unique_list[ix])
                num = match_list.count(unique_list[ix])
                process_output.iloc[i + n, 1] = process_output.iloc[i + n, 1] * num
                # if 'Merchant' in process_output.columns.tolist() or '自发货' in process_output.columns.tolist():#!!!!!!!!!!!!!!!!!!!!
                if '自发货' in process_output.columns.tolist():
                    process_output.iloc[i + n, 6] = process_output.iloc[i + n, 6] * num
                    process_output.iloc[i + n, 7] = process_output.iloc[i + n, 7] * num

                # 判断是否有查询到建议售价，如果没有建议售价，价格平摊
                if (suggest_item_price[ix] == 0) | (suggest_total == 0):
                    process_output.iloc[i + n, 3] = process_output.iloc[i + n, 3] / len(match_list) * num
                    process_output.iloc[i + n, 2] = process_output.iloc[i + n, 2] / len(match_list) * num
                else:
                    process_output.iloc[i + n, 3] = process_output.iloc[i + n, 3] * (suggest_item_price[ix] / suggest_total) * num
                    process_output.iloc[i + n, 2] = process_output.iloc[i + n, 2] * (suggest_item_price[ix] / suggest_total) * num
                if len(unique_list) - 1 - ix > 0:
                    n += 1
                    process_output = process_output.append(df.iloc[i], ignore_index=True)
        else:
            process_output.iloc[i + n, 0] = int(process_output.iloc[i + n, 0])
    #   插入成本单价
    print(process_output.columns)
    cost = read('sale', cost_price)
    process_output[index_2] = process_output[index_2].astype('int')
    cost[index_2] = cost[index_2].astype('int')
    process_output = pd.merge(process_output, cost, left_on=index_2, right_on=index_2, how='left')
    process_output = process_output.where(process_output.notnull(), 0)
    process_output.loc[process_output['type'] == 'wayfair', '成本单价'] = process_output.loc[
        process_output['type'] == 'wayfair', 'wayfair 销售成本（不含平台费）']
    process_output.loc[process_output['type'] == 'Amazon', '成本单价'] = process_output.loc[
        process_output['type'] == 'Amazon', 'FBA销售成本（不含平台费）']
    process_output.loc[process_output['type'] == 'Merchant', '成本单价'] = process_output.loc[
        process_output['type'] == 'Merchant', 'FBM销售成本（不含平台费）']
    process_output.loc[process_output['type'] == 'FBM', '成本单价'] = process_output.loc[
        process_output['type'] == 'FBM', 'FBM销售成本（不含平台费）']

    # print(process_output.columns,"\n")
    # print(process_output, '\n---------------------------------------\n')

    # 整合index_2 'a'分开后的数据
    # final = process_output.pivot_table(values=[u'销量', u'coupon', u'销售额', u'成本单价', u'Merchant', u'Amazon'], index=[ 'date',index_2],
    #                                     fill_value=0,aggfunc={u'Merchant': np.sum,u'Amazon': np.sum,u'销量': np.sum, u'coupon': np.sum, u'销售额': np.sum, u'成本单价': np.mean})
    if  '自发货' in a_s.columns.tolist():
        final = process_output.pivot_table(values=[u'销量', u'coupon', u'销售额', u'成本单价', u'自发货', u'平台发货'],
                                           index=['date', index_2], fill_value=0,
                                           aggfunc={u'自发货': np.sum, u'平台发货': np.sum, u'销量': np.sum,
                                                    u'coupon': np.sum, u'销售额': np.sum, u'成本单价': np.mean})
        final = final.reset_index()  # 重建索引
        # 按照order规范列
        order = ['sku序号', u'销量', u'coupon', u'销售额', 'date', u'成本单价', u'自发货', u'平台发货']
        final = final[order]
        final = final.where(final.notnull(), 0)
        final['coupon'] = - final['coupon']
    else:
        final = process_output.pivot_table(values=[u'销量', u'coupon', u'销售额', u'成本单价'],
                                           index=['date', index_2], fill_value=0,
                                           aggfunc={ u'销量': np.sum, u'coupon': np.sum, u'销售额': np.sum,
                                                     u'成本单价': np.mean})
        final = final.reset_index()  # 重建索引

        # 按照order规范列
        order = ['sku序号', u'销量', u'coupon', u'销售额', 'date', u'成本单价']
        final = final[order]
        final = final.where(final.notnull(), 0)
        final['coupon'] = - final['coupon']
        final['自发货'] = final['销量']
        final['平台发货'] = 0

    final = final.drop("成本单价", axis=1)
    # print(final, '\n---------------------------------------')
    # final = final.reset_index()  # 重建索引
    # # 按照order规范列
    # order = ['sku序号', u'销量', u'coupon', u'销售额', 'date', u'成本单价',u'Merchant', u'Amazon']
    # final = final[order]
    # final = final.where(final.notnull(), 0)
    # final['coupon'] = - final['coupon']
    # print(final, '\n---------------------------------------')
    #final = final.sort_values(by=['date', index_2])
    return final


# index_1是指ASIN映射到匹配表的字段
# index_2是指匹配表里的最后用到的识别字段
# value是指最后要输出的变量
def ebaymodel_to_output(origin, match, index_1, index_2, cost_price, suggest_price):
    # 第一次处理asin并合并数量（相当于excel透视）
    match = match.where(match.notnull(), 0)
    origin[index_1] = origin[index_1].apply(str)
    match['ASIN'] = match['ASIN'].apply(str)
    origin['sku序号'] = ''
    # 用index_1匹配index_2
    for i in range(len(origin)):

        index = match[match['ASIN'].str.contains(origin.loc[i, index_1])].index.tolist()
        # print(index)
        # print(match.loc[index[0], index_2])
        origin.loc[i, index_2] = match.loc[index[0], index_2]

    a_s = origin.drop(index_1, axis=1)
    # 处理index_2并合并数量
    a_s = a_s.pivot_table(values=[u'销量', u'coupon', u'销售额'], index=[index_2, 'date', 'type'], aggfunc='sum')
    df = a_s
    df = df.reset_index()  # 重建索引
    order = [index_2, u'销量', u'coupon', u'销售额', 'date', 'type']
    df = df[order]
    n = 0
    # AB箱通过建议售价判断销售占比
    suggest = read('sale', suggest_price)
    # 判断a是否存在于index_2中，并进行处理
    process_output = pd.DataFrame()
    for i in range(len(df)):
        process_output = process_output.append(df.iloc[i], ignore_index=True)
        process_output = process_output[order]
        if 'a' in str(df.iloc[i, 0]):
            match_list = list(map(int, df.iat[i, 0].split("a")))
            unique_list = np.unique(match_list)
            suggest_total = 0
            suggest_item_price = [0] * len(unique_list)
            for ix in range(len(unique_list)):
                if process_output.loc[i + n, 'type'] == 'wayfair':
                    suggest_item_price[ix] = int(suggest.loc[suggest['sku序号'] == int(unique_list[ix]), 'wayfair 建议售价'])
                elif process_output.loc[i + n, 'type'] == 'Amazon':
                    suggest_item_price[ix] = int(suggest.loc[suggest['sku序号'] == int(unique_list[ix]), 'FBA建议售价'])
                elif process_output.loc[i + n, 'type'] == 'Merchant':
                    suggest_item_price[ix] = int(suggest.loc[suggest['sku序号'] == int(unique_list[ix]), 'FBM建议售价'])
                else:
                    suggest_item_price[ix] = int(suggest.loc[suggest['sku序号'] == int(unique_list[ix]), 'FBM建议售价'])
                suggest_total += suggest_item_price[ix]
            for ix in range(len(unique_list)):
                process_output.iloc[i + n, 0] = int(unique_list[ix])
                num = match_list.count(unique_list[ix])
                process_output.iloc[i + n, 1] = process_output.iloc[i + n, 1] * num
                # 判断是否有查询到建议售价，如果没有建议售价，价格平摊
                if (suggest_item_price[ix] == 0) | (suggest_total == 0):
                    process_output.iloc[i + n, 3] = process_output.iloc[i + n, 3] / len(match_list) * num
                    process_output.iloc[i + n, 2] = process_output.iloc[i + n, 2] / len(match_list) * num
                else:
                    process_output.iloc[i + n, 3] = process_output.iloc[i + n, 3] * (suggest_item_price[ix] / suggest_total) * num
                    process_output.iloc[i + n, 2] = process_output.iloc[i + n, 2] * (suggest_item_price[ix] / suggest_total) * num
                if len(unique_list) - 1 - ix > 0:
                    n += 1
                    process_output = process_output.append(df.iloc[i], ignore_index=True)
        else:
            process_output.iloc[i + n, 0] = int(process_output.iloc[i + n, 0])
    #   插入成本单价
    cost = read('sale', cost_price)
    process_output[index_2] = process_output[index_2].astype('int')
    cost[index_2] = cost[index_2].astype('int')
    process_output = pd.merge(process_output, cost, left_on=index_2, right_on=index_2, how='left')
    process_output = process_output.where(process_output.notnull(), 0)
    process_output['成本单价'] = process_output['FBM销售成本（不含平台费）']

    final = process_output.pivot_table(values=[u'销量', u'coupon', u'销售额', u'成本单价'], index=[u'sku序号', 'date'],
                                        aggfunc={u'销量': np.sum, u'coupon': np.sum, u'销售额': np.sum, u'成本单价': np.mean})
    final = final.reset_index()  # 重建索引
    # 按照order规范列
    order = ['sku序号', u'销量', u'coupon', u'销售额', 'date', u'成本单价']
    final = final[order]
    final = final.where(final.notnull(), 0)
    final['coupon'] = - final['coupon']
    final['自发货'] = final['销量']
    final['平台发货'] = 0
    final = final.sort_values(by=['date', 'sku序号'])
    final = final.drop("成本单价", axis=1)

    return final
