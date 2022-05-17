from sqlalchemy import create_engine
import pymysql
import pandas as pd
import datetime
from dataprocess import read, table_exists, creat_id
from pathlib import Path
import numpy as np
from sqlalchemy.types import VARCHAR, Float, Integer, Date, Numeric, Text


def sale_sql_temp(amazon, wayfair, walmart, ebay, shopify, total, AdDate):
    # 将ODS层的数据存入数据库
    for i in range(len(amazon)):
        s_temp = []
        # 处理销量、广告、sd广告
        print('%s销售' % amazon[i])
        finalname = '%s%s销售' % (amazon[i], AdDate)

        if (Path('output/sku/%ssp广告sku.xlsx' % amazon[i]).exists()) | (
                Path('output/sku/%ssd广告sku.xlsx' % amazon[i]).exists()) | (
                Path('output/sku/%s销售sku.xlsx' % amazon[i]).exists()):
            if Path('output/sku/%s销售sku.xlsx' % amazon[i]).exists():
                origin = pd.read_excel('output/sku/%s销售sku.xlsx' % amazon[i])
                origin = origin.drop(origin[(origin['销售额'] == 0)].index)
            if Path('output/sku/%ssp广告sku.xlsx' % amazon[i]).exists():
                origin_sp = pd.read_excel('output/sku/%ssp广告sku.xlsx' % amazon[i])
                origin_sp = origin_sp.drop('销量', axis=1)
                origin_sp = origin_sp.drop('coupon', axis=1)
                origin_sp = origin_sp.rename(columns={'销售额': 'sp广告'})
                if Path('output/sku/%s销售sku.xlsx' % amazon[i]).exists():
                    s_temp = pd.merge(origin, origin_sp, on=['sku序号', 'date'], how='outer')
                    # s_temp['成本单价'] = s_temp['成本单价_x']
                else:
                    s_temp = origin_sp
                    s_temp["销量"] = 0
                    s_temp["销售额"] = 0
                    s_temp["coupon"] = 0
            else:
                s_temp = origin
                s_temp["sp广告"] = 0

            if Path('output/sku/%ssd广告sku.xlsx' % amazon[i]).exists():
                origin_sd = pd.read_excel('output/sku/%ssd广告sku.xlsx' % amazon[i])
                origin_sd = origin_sd.drop('销量', axis=1)
                origin_sd = origin_sd.drop('coupon', axis=1)
                origin_sd = origin_sd.rename(columns={'销售额': 'sd广告'})
                s_temp = pd.merge(s_temp, origin_sd, on=['sku序号', 'date'], how='outer')
                s_temp = s_temp.rename(columns={'成本单价_x': '成本单价'})
            else:
                s_temp["sd广告"] = 0
            s_temp = s_temp.drop(s_temp[(s_temp['销售额'] == 0) & (s_temp['sp广告'] == 0) & (s_temp['sd广告'] == 0)].index)
            s_temp = s_temp.where(s_temp.notnull(), 0)
            engine = create_engine(
                'mysql+pymysql://test_admin:123456@rm-bp15wb9k07q3p10q6yo.mysql.rds.aliyuncs.com/sale_temporary')
            s_temp.loc[s_temp['销量'] == 0, '成本单价'] = 0
            s_temp = s_temp[['sku序号', '销量', 'coupon', '销售额', 'sp广告', 'sd广告', 'date', '成本单价']]
            s_temp.to_sql(finalname, con=engine, if_exists='replace', index=False)
        else:
            print(amazon[i], '都为0')

    for i in range(len(walmart)):
        s_temp = []
        print('%s%s销售' % (walmart[i], AdDate))
        finalname = '%s%s销售' % (walmart[i], AdDate)
        if (Path('output/sku/%s广告sku.xlsx' % walmart[i]).exists()) | (
                Path('output/sku/%s销售sku.xlsx' % walmart[i]).exists()):
            if Path('output/sku/%s销售sku.xlsx' % walmart[i]).exists():
                origin = pd.read_excel('output/sku/%s销售sku.xlsx' % walmart[i])
                origin = origin.drop(origin[(origin['销售额'] == 0)].index)
            if Path('output/sku/%ssp广告sku.xlsx' % walmart[i]).exists():
                origin_sp = pd.read_excel('output/sku/%ssp广告sku.xlsx' % walmart[i])
                try:
                    origin_sp = origin_sp.drop('销量', axis=1)
                    origin_sp = origin_sp.drop('coupon', axis=1)
                    origin_sp = origin_sp.rename(columns={'销售额': 'sp广告'})
                    if Path('output/sku/%s销售sku.xlsx' % walmart[i]).exists():
                        s_temp = pd.merge(origin, origin_sp, on=['sku序号', 'date'], how='outer')
                        s_temp = s_temp.rename(columns={'成本单价_x': '成本单价'})
                    else:
                        s_temp = origin_sp
                        s_temp['销量'] = 0
                        s_temp['销售额'] = 0

                except ValueError:
                    print(ValueError)
            else:
                print('output/sku/%ssp广告sku.xlsx' % walmart[i])
                s_temp = origin
                s_temp['sp广告'] = 0
            s_temp['sd广告'] = 0
            # 处理销量、广告、sd广告
            s_temp = s_temp.drop(s_temp[(s_temp['销售额'] == 0) & (s_temp['sp广告'] == 0) & (s_temp['sd广告'] == 0)].index)
            s_temp = s_temp.where(s_temp.notnull(), 0)
            engine = create_engine(
                'mysql+pymysql://test_admin:123456@rm-bp15wb9k07q3p10q6yo.mysql.rds.aliyuncs.com/sale_temporary')
            s_temp.loc[s_temp['销量'] == 0, '成本单价'] = 0
            s_temp = s_temp[['sku序号', '销量', 'coupon', '销售额', 'sp广告', 'sd广告', 'date', '成本单价']]
            s_temp.to_sql(finalname, con=engine, if_exists='replace', index=False)
        else:
            print(walmart[i], '都为0')

    for i in range(len(wayfair)):
        s_temp = []
        print('%s销售' % wayfair[i])
        finalname = '%s%s销售' % (wayfair[i], AdDate)
        s_temp = pd.read_excel('output/sku/%s销售sku.xlsx' % wayfair[i])
        s_temp = s_temp.drop(s_temp[(s_temp['销售额'] == 0)].index)
        try:
            # 处理销量、广告、sd广告
            s_temp['sp广告'] = 0
            s_temp['sd广告'] = 0
            s_temp = s_temp.drop(s_temp[(s_temp['销售额'] == 0) & (s_temp['sp广告'] == 0) & (s_temp['sd广告'] == 0)].index)
            s_temp = s_temp.where(s_temp.notnull(), 0)
            engine = create_engine(
                'mysql+pymysql://test_admin:123456@rm-bp15wb9k07q3p10q6yo.mysql.rds.aliyuncs.com/sale_temporary')
            s_temp.loc[s_temp['销量'] == 0, '成本单价'] = 0
            s_temp = s_temp[['sku序号', '销量', 'coupon', '销售额', 'sp广告', 'sd广告', 'date', '成本单价']]
            s_temp.to_sql(finalname, con=engine, if_exists='replace', index=False)
        except ValueError:
            print(ValueError)

    # ebay
    for i in range(len(ebay)):
        s_temp = []
        print('%s销售' % ebay[i])
        finalname = '%s%s销售' % (ebay[i], AdDate)
        if (Path('output/sku/%s广告sku.xlsx' % ebay[i]).exists()) | (
                Path('output/sku/%s销售sku.xlsx' % ebay[i]).exists()):
            if Path('output/sku/%s销售sku.xlsx' % ebay[i]).exists():
                origin = pd.read_excel('output/sku/%s销售sku.xlsx' % ebay[i])
                origin = origin.drop(origin[(origin['销售额'] == 0)].index)
            if Path('output/sku/%ssp广告sku.xlsx' % ebay[i]).exists():
                origin_sp = pd.read_excel('output/sku/%ssp广告sku.xlsx' % ebay[i])
                try:
                    origin_sp = origin_sp.drop('销量', axis=1)
                    origin_sp = origin_sp.drop('coupon', axis=1)
                    origin_sp = origin_sp.rename(columns={'销售额': 'sp广告'})
                    if Path('output/sku/%s销售sku.xlsx' % ebay[i]).exists():
                        s_temp = pd.merge(origin, origin_sp, on=['sku序号', 'date'], how='outer')
                        s_temp = s_temp.rename(columns={'成本单价_x': '成本单价'})
                    else:
                        s_temp = origin_sp
                        s_temp['销量'] = 0
                        s_temp['销售额'] = 0

                except ValueError:
                    print(ValueError)
            else:
                print('output/sku/%ssp广告sku.xlsx' % walmart[i])
                s_temp = origin
                s_temp['sp广告'] = 0
            s_temp['sd广告'] = 0
            # 处理销量、广告、sd广告
            s_temp = s_temp.drop(s_temp[(s_temp['销售额'] == 0) & (s_temp['sp广告'] == 0) & (s_temp['sd广告'] == 0)].index)
            s_temp = s_temp.where(s_temp.notnull(), 0)
            engine = create_engine(
                'mysql+pymysql://test_admin:123456@rm-bp15wb9k07q3p10q6yo.mysql.rds.aliyuncs.com/sale_temporary')
            s_temp.loc[s_temp['销量'] == 0, '成本单价'] = 0
            s_temp = s_temp[['sku序号', '销量', 'coupon', '销售额', 'sp广告', 'sd广告', 'date', '成本单价']]
            s_temp.to_sql(finalname, con=engine, if_exists='replace', index=False)
        else:
            print('output/sku/%s销售sku.xlsx不存在' % ebay[i])

    for i in range(len(shopify)):
        s_temp = []
        print('%s销售' % shopify[i])
        finalname = '%s%s销售' % (shopify[i], AdDate)
        if (Path('output/sku/%s广告sku.xlsx' % shopify[i]).exists()) | (
                Path('output/sku/%s销售sku.xlsx' % shopify[i]).exists()):
            if Path('output/sku/%s销售sku.xlsx' % shopify[i]).exists():
                origin = pd.read_excel('output/sku/%s销售sku.xlsx' % shopify[i])
                origin = origin.drop(origin[(origin['销售额'] == 0)].index)
            if Path('output/sku/%ssp广告sku.xlsx' % shopify[i]).exists():
                origin_sp = pd.read_excel('output/sku/%ssp广告sku.xlsx' % shopify[i])
                try:
                    origin_sp = origin_sp.drop('销量', axis=1)
                    origin_sp = origin_sp.drop('coupon', axis=1)
                    origin_sp = origin_sp.rename(columns={'销售额': 'sp广告'})
                    if Path('output/sku/%s销售sku.xlsx' % shopify[i]).exists():
                        s_temp = pd.merge(origin, origin_sp, on=['sku序号', 'date'], how='outer')
                        s_temp = s_temp.rename(columns={'成本单价_x': '成本单价'})
                    else:
                        s_temp = origin_sp
                        s_temp['销量'] = 0
                        s_temp['销售额'] = 0

                except ValueError:
                    print(ValueError)
            else:
                print('output/sku/%ssp广告sku.xlsx' % walmart[i])
                s_temp = origin
                s_temp['sp广告'] = 0
            s_temp['sd广告'] = 0
            # 处理销量、广告、sd广告
            s_temp = s_temp.drop(s_temp[(s_temp['销售额'] == 0) & (s_temp['sp广告'] == 0) & (s_temp['sd广告'] == 0)].index)
            s_temp = s_temp.where(s_temp.notnull(), 0)
            s_temp.loc[s_temp['销量'] == 0, '成本单价'] = 0
            engine = create_engine(
                'mysql+pymysql://test_admin:123456@rm-bp15wb9k07q3p10q6yo.mysql.rds.aliyuncs.com/sale_temporary')
            s_temp = s_temp[['sku序号', '销量', 'coupon', '销售额', 'sp广告', 'sd广告', 'date', '成本单价']]
            s_temp.to_sql(finalname, con=engine, if_exists='replace', index=False)
        else:
            print(shopify[i], '都为0')


def sale_sql_total(amazon, wayfair, walmart, ebay, shopify, total, AdDate, CurrentAdDate):
    # 将sale_temporary表中的内容写入总表
    for i in range(len(total)):
        print(AdDate)
        print(total[i])
        if table_exists('sale_temporary', '%s%s销售' % (total[i], AdDate)):
            finalname = '%s销售sku2022' % total[i]
            engine = create_engine(
                'mysql+pymysql://test_admin:123456@rm-bp15wb9k07q3p10q6yo.mysql.rds.aliyuncs.com/sale')
            origin = read('sale_temporary', '%s%s销售' % (total[i], AdDate))
            origin['date'] = pd.to_datetime(origin['date'])
            for x in range(len(origin)):
                origin.loc[x, 'date'] = origin.loc[x, 'date'].date()
            origin = origin[(origin['date'] >= AdDate) & (origin['date'] <= CurrentAdDate)]
            origin['sb广告'] = 0
            origin['sbv广告'] = 0
            origin = origin.sort_values(by=['date', 'sku序号'])
            dtypedict = {
                "sku序号": Integer, "销量": Integer, "coupon": Float(2), "销售额": Float(2), "sp广告": Float(2),
                "sd广告": Float(2), "sb广告": Float(2), "sd广告": Float(2), "date": Date, "成本单价": Float(4),
            }
            origin.to_sql(finalname, con=engine, if_exists='append', index=False, dtype=dtypedict)
            # creat_id(engine, 'sale', finalname)
        else:
            print('%s%s销售不存在' % (total[i], AdDate))

    # # 将平台店铺导入平台
    # times = 0
    # finalname = 'walmart销售sku2022'
    # for i in range(len(walmart)):
    #     if table_exists('sale_temporary', '%s%s销售' % (walmart[i], AdDate)):
    #         times += 1
    # frames = [0] * times
    # times = 0
    # for i in range(len(walmart)):
    #     print('sale_temporary', '%s%s销售' % (walmart[i], AdDate))
    #     if table_exists('sale_temporary', '%s%s销售' % (walmart[i], AdDate)):
    #         frames[times] = read('sale_temporary', '%s%s销售' % (walmart[i], AdDate))
    #         frames[times] = frames[times].fillna(0)
    #         times += 1
    # total = pd.concat(frames, keys=walmart)
    # total = total.reset_index(drop=True)
    # total['成本单价'] = total['成本单价'] * total['销量']
    # total['date'] = pd.to_datetime(total['date'])
    # total_sum = total.pivot_table(index=['sku序号', 'date'],
    #                               values=['销量', 'coupon', '销售额', 'sp广告', 'sd广告', '成本单价'],
    #                               aggfunc={u'销量': np.sum, u'coupon': np.sum, u'销售额': np.sum,
    #                                        'sp广告': np.sum, 'sd广告': np.sum, u'成本单价': np.sum})
    # total_sum = total_sum.reset_index()
    # total_sum['成本单价'] = total_sum['成本单价'] / total_sum['销量']
    # total_sum = total_sum.sort_values(by=['date', 'sku序号'])
    # total_sum = total_sum.reset_index()
    # total_sum = total_sum[['sku序号', 'date', '销量', 'coupon', '销售额', 'sp广告', 'sd广告', '成本单价']]
    # engine = create_engine(
    #     'mysql+pymysql://test_admin:123456@rm-bp15wb9k07q3p10q6yo.mysql.rds.aliyuncs.com/sale')
    # total_sum.to_sql(finalname, con=engine, if_exists='append', index=False)
    #
    # # 将平台店铺导入平台
    # times = 0
    # finalname = 'wayfair销售sku2022'
    # for i in range(len(wayfair)):
    #     if table_exists('sale_temporary', '%s%s销售' % (wayfair[i], AdDate)):
    #         times += 1
    # frames = [0] * times
    # times = 0
    # for i in range(len(wayfair)):
    #     print('sale_temporary', '%s%s销售' % (wayfair[i], AdDate))
    #     if table_exists('sale_temporary', '%s%s销售' % (wayfair[i], AdDate)):
    #         frames[times] = read('sale_temporary', '%s%s销售' % (wayfair[i], AdDate))
    #         frames[times] = frames[times].fillna(0)
    #         times += 1
    # total = pd.concat(frames, keys=wayfair)
    # total['成本单价'] = total['成本单价'] * total['销量']
    # total['date'] = pd.to_datetime(total['date'])
    # total_sum = total.pivot_table(index=['sku序号', 'date'],
    #                               values=['销量', 'coupon', '销售额', 'sp广告', 'sd广告', '成本单价'],
    #                               aggfunc={u'销量': np.sum, u'coupon': np.sum, u'销售额': np.sum,
    #                                        'sp广告': np.sum, 'sd广告': np.sum, u'成本单价': np.sum})
    # total_sum = total_sum.reset_index()
    # total_sum['成本单价'] = total_sum['成本单价'] / total_sum['销量']
    # total_sum = total_sum.sort_values(by=['date', 'sku序号'])
    # total_sum = total_sum.reset_index()
    # total_sum = total_sum[['sku序号', 'date', '销量', 'coupon', '销售额', 'sp广告', 'sd广告', '成本单价']]
    # engine = create_engine(
    #     'mysql+pymysql://test_admin:123456@rm-bp15wb9k07q3p10q6yo.mysql.rds.aliyuncs.com/sale')
    # total_sum.to_sql(finalname, con=engine, if_exists='append', index=False)
    #
    # # 将平台店铺导入平台
    # times = 0
    # finalname = 'ebay销售sku2022'
    # for i in range(len(ebay)):
    #     if table_exists('sale_temporary', '%s%s销售' % (ebay[i], AdDate)):
    #         times += 1
    # frames = [0] * times
    # times = 0
    # for i in range(len(ebay)):
    #     print('sale_temporary', '%s%s销售' % (ebay[i], AdDate))
    #     if table_exists('sale_temporary', '%s%s销售' % (ebay[i], AdDate)):
    #         frames[times] = read('sale_temporary', '%s%s销售' % (ebay[i], AdDate))
    #         frames[times] = frames[times].fillna(0)
    #         times += 1
    # total = pd.concat(frames, keys=ebay)
    # total['成本单价'] = total['成本单价'] * total['销量']
    # total['date'] = pd.to_datetime(total['date'])
    # total_sum = total.pivot_table(index=['sku序号', 'date'],
    #                               values=['销量', 'coupon', '销售额', 'sp广告', 'sd广告', '成本单价'],
    #                               aggfunc={u'销量': np.sum, u'coupon': np.sum, u'销售额': np.sum,
    #                                        'sp广告': np.sum, 'sd广告': np.sum, u'成本单价': np.sum})
    # total_sum['成本单价'] = total_sum['成本单价'] / total_sum['销量']
    # total_sum = total_sum.reset_index()
    # total_sum = total_sum.sort_values(by=['date', 'sku序号'])
    # total_sum = total_sum.reset_index()
    # total_sum = total_sum[['sku序号', 'date', '销量', 'coupon', '销售额', 'sp广告', 'sd广告', '成本单价']]
    # engine = create_engine(
    #     'mysql+pymysql://test_admin:123456@rm-bp15wb9k07q3p10q6yo.mysql.rds.aliyuncs.com/sale')
    # total_sum.to_sql(finalname, con=engine, if_exists='append', index=False)
