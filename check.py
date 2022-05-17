import pandas as pd
import datetime
from pathlib import Path


def check(amazon, walmart, wayfair, ebay, Length):
    item = amazon + walmart + wayfair + ebay
    columns = ['店铺', '日期', '销售额', '广告', 'sd广告']
    total = pd.DataFrame(columns=columns)
    Length = Length.days
    times = 0
    for i in range(len(item)):
        if Path('output/sku/%s销售sku.xlsx' % item[i]).exists():
            origin = pd.read_excel("output/sku/%s销售sku.xlsx" % item[i], sheet_name=0)
            date_num = origin['date'].unique()
            if len(origin) > 0:
                for ix in range(len(date_num)):
                    total.loc[times + ix, '店铺'] = item[i]
                    total.loc[times + ix, '日期'] = pd.to_datetime(date_num[ix]).date()
                    total.loc[times + ix, '销量'] = origin[origin['date'] == date_num[ix]]['销量'].sum()
                    total.loc[times + ix, '销售额'] = origin[origin['date'] == date_num[ix]]['销售额'].sum()
        else:
            print('output/sku/%s销售sku.xlsx不存在' % item[i])

        if Path('output/sku/%ssp广告sku.xlsx' % item[i]).exists():
            origin = pd.read_excel("output/sku/%ssp广告sku.xlsx" % item[i], sheet_name=0)
            if Path('output/sku/%s销售sku.xlsx' % item[i]).exists():
                pass
            else:
                date_num = origin['date'].unique()
            if len(origin) > 0:
                for ix in range(len(date_num)):
                    total.loc[times + ix, '广告'] = origin[origin['date'] == date_num[ix]]['销售额'].sum()
        else:
            print('output/sku/%s广告sku.xlsx不存在' % item[i])

        if Path("output/sku/%ssd广告sku.xlsx" % item[i]).exists():
            origin = pd.read_excel("output/sku/%ssd广告sku.xlsx" % item[i], sheet_name=0)
            if len(origin) > 0:
                total.loc[times + ix, 'sd广告'] = origin[origin['date'] == date_num[ix]]['销售额'].sum()
        else:
            print('output/sku/%ssd广告sku.xlsx不存在' % item[i])

        times += len(date_num)

    total = total.where(total.notnull(), 0)
    total.to_excel(excel_writer="output/校准(处理后).xlsx",
                   index=False,
                   encoding='utf-8')