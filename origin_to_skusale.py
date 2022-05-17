import pandas as pd
import pymysql
from dataprocess import amazon_to_model, ad_to_model, model_to_output, wayfair_to_model, walmart_to_model, ebay_to_model \
    , ebayad_to_model, walmartad_to_model, shopify_to_model, shopifyad_to_model, ebaymodel_to_output, read
import datetime
from pathlib import Path
from dateutil.parser import parse


# 变量
import warnings
warnings.filterwarnings("ignore")

def origin_to_skusale(amazon, walmart, ebay, wayfair, shopify, sale_url,
                      XsDate, AdDate, CurrentXsDate, CurrentAdDate, id_sort,
                      amazon_select, amazon_ad_select,
                      wayfair_select, walmart_select, walmart_ad, ebay_select,
                      ebay_ad, shopify_select, shopify_ad, cost_price, match_origin):

    # PART--亚马逊销售
    print('亚马逊销售')
    for i in range(len(amazon)):
        match = match_origin[match_origin['店铺'] == amazon[i]]
        match = match.drop_duplicates(subset=['渠道sku'])
        match = match.reset_index(drop=True)
        print(amazon[i])
        if Path(sale_url + '%s销售.xlsx' % amazon[i]).exists():
            origin = pd.read_excel(sale_url + '%s销售.xlsx' % amazon[i], sheet_name=0, usecols=amazon_select)
            if len(origin) > 0:
                for x in range(len(origin)):
                    origin.loc[x, 'purchase-date'] = parse(origin.loc[x, 'purchase-date']) - datetime.timedelta(hours=7)
                    origin.loc[x, 'purchase-date'] = (origin.loc[x, 'purchase-date']).date()
                origin = origin[(origin['purchase-date'] <= CurrentXsDate) & (origin['purchase-date'] >= AdDate)]

                amazon_model = amazon_to_model(origin, id_sort[4])
                amazon_model.to_excel(excel_writer="output/新建文件夹/%s销售model.xlsx" % amazon[i], index=False,
                                          encoding='utf-8')
                # print(amazon_model.columns)
                # print(amazon_model.head())
                model_to_output1 = model_to_output(amazon_model, match, id_sort[4], id_sort[1], cost_price[0], cost_price[2])
                model_to_output1.to_excel(excel_writer="output/sku/%s销售sku.xlsx" % amazon[i], index=False,
                                          encoding='utf-8')
        else:
            print(sale_url + '%s销售sku.xlsx不存在' % amazon[i])

    # PART--亚马逊广告
    print('亚马逊广告')
    for i in range(len(amazon)):
        match = match_origin[match_origin['店铺'] == amazon[i]]
        match = match.drop_duplicates(subset=['渠道sku'])
        match = match.reset_index(drop=True)
        print(amazon[i])
        if Path(sale_url + '%s广告.xlsx' % amazon[i]).exists():
            origin = pd.read_excel(sale_url + '%s广告.xlsx' % amazon[i], sheet_name=0, usecols=amazon_ad_select)
            if len(origin) > 0:
                for x in range(len(origin)):
                    origin.loc[x, 'Date'] = (origin.loc[x, 'Date']).date()
                origin = origin[(origin['Date'] <= CurrentAdDate) & (origin['Date'] >= AdDate)]
                sp_model = ad_to_model(origin, id_sort[4])
                sp_model.to_excel(excel_writer="output/新建文件夹/%ssp广告model.xlsx" % amazon[i],
                                          index=False,
                                          encoding='utf-8')
                # 判断是否为空
                if sp_model.empty:
                    print(' %s 空!!!!!' % amazon[i])
                    continue
                model_to_output1 = model_to_output(sp_model, match, id_sort[4], id_sort[1], cost_price[0], cost_price[2])
                model_to_output1.to_excel(excel_writer="output/sku/%ssp广告sku.xlsx" % amazon[i],
                                          index=False,
                                          encoding='utf-8')
        else:
            print(sale_url + '%s广告.xlsx不存在' % amazon[i])

    # PART--亚马逊sd广告
    print('亚马逊sd广告')
    for i in range(len(amazon)):
        match = match_origin[match_origin['店铺'] == amazon[i]]
        match = match.drop_duplicates(subset=['渠道sku'])
        match = match.reset_index(drop=True)
        print(amazon[i])
        if Path(sale_url + '%ssd广告.xlsx' % amazon[i]).exists():
            origin = pd.read_excel(sale_url + '%ssd广告.xlsx' % amazon[i], sheet_name=0,
                                   usecols=amazon_ad_select)
            if len(origin) > 0:
                for x in range(len(origin)):
                    origin.loc[x, 'Date'] = (origin.loc[x, 'Date']).date()
                origin = origin[(origin['Date'] <= CurrentAdDate) & (origin['Date'] >= AdDate)]
                sd_model = ad_to_model(origin, id_sort[4])
                sd_model.to_excel(excel_writer="output/新建文件夹/%ssd广告model.xlsx" % amazon[i],
                                          index=False,
                                          encoding='utf-8')
                # 判断是否为空
                if sd_model.empty:
                    print(' %s 空!!!!!' % amazon[i])
                    continue

                model_to_output1 = model_to_output(sd_model, match, id_sort[4], id_sort[1], cost_price[0], cost_price[2])
                model_to_output1.to_excel(excel_writer="output/sku/%ssd广告sku.xlsx" % amazon[i],
                                          index=False,
                                          encoding='utf-8')
        else:
            print(sale_url + '%ssd广告.xlsx不存在' % amazon[i])


    # PART--WAYFAIR销售
    for i in range(len(wayfair)):
        print(wayfair[i], XsDate)
        match = match_origin[match_origin['店铺'] == wayfair[i]]
        match = match.drop_duplicates(subset=['渠道sku'])
        match = match.reset_index(drop=True)
        if Path(sale_url + '%s销售.xlsx' % wayfair[i]).exists():
            origin = pd.read_excel(sale_url + '%s销售.xlsx' % wayfair[i], sheet_name=0, usecols=wayfair_select)
            if len(origin) > 0:
                origin["PO Date"] = pd.to_datetime(origin["PO Date"])
                for x in range(len(origin)):
                    origin.loc[x, "PO Date"] = origin.loc[x, "PO Date"].date()
                origin = origin[(origin['PO Date'] <= CurrentXsDate) & (origin['PO Date'] >= AdDate)]
                wayfair_model = wayfair_to_model(origin, id_sort[4])
                wayfair_model.to_excel(excel_writer="output/新建文件夹/%s销售model.xlsx" % wayfair[i], index=False,
                                      encoding='utf-8')
                # print(wayfair_model.columns)
                # print(wayfair_model.head())
                model_to_output1 = model_to_output(wayfair_model, match, id_sort[4], id_sort[1], cost_price[0], cost_price[2])
                model_to_output1.to_excel(excel_writer="output/sku/%s销售sku.xlsx" % wayfair[i], index=False,
                                          encoding='utf-8')
        else:
            print(sale_url + 'wayfair%s销售.xlsx不存在' % XsDate)

    # PART--WALMART销售
    for i in range(len(walmart)):
        print(walmart[i], XsDate)
        match = match_origin[match_origin['店铺'] == walmart[i]]
        match = match.drop_duplicates(subset=['渠道sku', 'ASIN'])
        match = match.reset_index(drop=True)
        if Path(sale_url + '%s销售.xlsx' % walmart[i]).exists():
            origin = pd.read_excel(sale_url + '%s销售.xlsx' % walmart[i], sheet_name=0, usecols=walmart_select)
            if len(origin) > 0:
                origin['Order Date'] = pd.to_datetime(origin['Order Date'])
                for x in range(len(origin)):
                    origin.loc[x, 'Order Date'] = origin.loc[x, 'Order Date'].date()
                origin = origin[(origin['Order Date'] <= CurrentXsDate) & (origin['Order Date'] >= AdDate)]
                walmart_model = walmart_to_model(origin, id_sort[4])
                walmart_model.to_excel(excel_writer="output/新建文件夹/%s销售model.xlsx" % walmart[i], index=False,
                                      encoding='utf-8')
                # print(walmart_model.columns)
                # print(walmart_model.head())
                model_to_output1 = model_to_output(walmart_model, match, id_sort[4], id_sort[1], cost_price[0], cost_price[2])
                model_to_output1.to_excel(excel_writer="output/sku/%s销售sku.xlsx" % walmart[i], index=False,
                                          encoding='utf-8')
        else:
            print(sale_url + '沃尔玛%s销售.xlsx不存在' % XsDate)

    # PART--WALMART广告
    print('walmart广告')
    for i in range(len(walmart)):
        print(walmart[i], AdDate)
        match = match_origin[match_origin['店铺'] == walmart[i]]
        match = match.drop_duplicates(subset=['渠道sku', 'ASIN'])
        match = match.reset_index(drop=True)
        if Path(sale_url + '%s广告.xlsx' % walmart[i]).exists():
            origin = pd.read_excel(sale_url + '%s广告.xlsx' % walmart[i], sheet_name=0,
                                   usecols=walmart_ad)
            if len(origin) > 0:
                for x in range(len(origin)):
                    origin.loc[x, 'Date'] = (origin.loc[x, 'Date']).date()
                origin = origin[(origin['Date'] <= CurrentAdDate) & (origin['Date'] >= AdDate)]
                sd_model = walmartad_to_model(origin, id_sort[2])
                sd_model.to_excel(excel_writer="output/新建文件夹/%ssp广告model.xlsx" % walmart[i], index=False, encoding='utf-8')
                model_to_output1 = model_to_output(sd_model, match, id_sort[2], id_sort[1], cost_price[0], cost_price[2])
                model_to_output1.to_excel(excel_writer="output/sku/%ssp广告sku.xlsx" % walmart[i],
                                          index=False,encoding='utf-8')
        else:
            print(sale_url + '沃尔玛%ssd广告.xlsx不存在' % XsDate)

    # PART--Ebay销售
    print("Ebay销售")
    for i in range(len(ebay)):
        print(ebay[i], XsDate)
        match = match_origin[match_origin['店铺'] == ebay[i]]
        match = match.drop_duplicates(subset=['渠道sku', 'ASIN'])
        match = match.reset_index(drop=True)
        if Path(sale_url + '%s销售.xlsx' % ebay[i]).exists():
            origin = pd.read_excel(sale_url + '%s销售.xlsx' % ebay[i], sheet_name=0, usecols=ebay_select)
            if len(origin) > 0:
                for x in range(len(origin)):
                    origin.loc[x, 'Sale Date'] = datetime.datetime.strptime(origin.loc[x, 'Sale Date'],
                                                                            '%b-%d-%y').date()
                origin = origin[(origin['Sale Date'] <= CurrentXsDate) & (origin['Sale Date'] >= AdDate)]
                ebay_model = ebay_to_model(origin, id_sort[4])
                ebay_model.to_excel(excel_writer="output/新建文件夹/%s销售model.xlsx" % ebay[i], index=False,
                                      encoding='utf-8')
                # print(ebay_model.columns)
                # print(ebay_model.head())
                model_to_output1 = model_to_output(ebay_model, match, id_sort[4], id_sort[1], cost_price[0], cost_price[2])
                model_to_output1.to_excel(excel_writer="output/sku/%s销售sku.xlsx" % ebay[i], index=False,
                                          encoding='utf-8')
        else:
            print(sale_url + 'ebay%s销售.xlsx不存在' % XsDate)

    # PART--ebay广告
    print('ebay广告')
    for i in range(len(ebay)):
        print(ebay[i], AdDate)
        match = match_origin[match_origin['店铺'] == ebay[i]]
        match = match.drop_duplicates(subset=['渠道sku', 'ASIN'])
        match = match.reset_index(drop=True)
        if Path(sale_url + '%s广告.xlsx' % ebay[i]).exists():
            origin = pd.read_excel(sale_url + '%s广告.xlsx' % ebay[i], sheet_name=0,
                                   usecols=ebay_ad)
            if len(origin) > 0:
                for x in range(len(origin)):
                    origin.loc[x, 'Date sold'] = datetime.datetime.strptime(origin.loc[x, 'Date sold'],
                                                                            '%b %d, %Y %I:%M %p -07:00').date()
                origin = origin[(origin['Date sold'] <= CurrentAdDate) & (origin['Date sold'] >= AdDate)]
                sd_model = ebayad_to_model(origin, id_sort[2])
                sd_model.to_excel(excel_writer="output/新建文件夹/%ssp广告model.xlsx" % ebay[i], index=False,
                                    encoding='utf-8')
                model_to_output1 = ebaymodel_to_output(sd_model, match, id_sort[2], id_sort[1], cost_price[0], cost_price[2])
                model_to_output1.to_excel(excel_writer="output/sku/%ssp广告sku.xlsx" % ebay[i], index=False,
                                          encoding='utf-8')
        else:
            print(sale_url + '%s广告.xlsx不存在' % ebay[i])

    # PART--shopify销售
    for i in range(len(shopify)):
        print(shopify[i], XsDate)
        match = match_origin[match_origin['店铺'] == shopify[i]]
        # 删除重复项
        match = match.drop_duplicates(subset=['渠道sku', 'ASIN'])
        match = match.reset_index(drop=True)
        if Path(sale_url + '%s销售.xlsx' % shopify[i]).exists():
            origin = pd.read_excel(sale_url + '%s销售.xlsx' % shopify[i], sheet_name=0, usecols=shopify_select)
            if len(origin) > 0:
                for x in range(len(origin)):
                    origin.loc[x, 'Created at'] = datetime.datetime.strptime(origin.loc[x, 'Created at'],
                                                                             '%Y-%m-%d %H:%M:%S -0700').date()
                origin = origin[(origin['Created at'] <= CurrentXsDate) & (origin['Created at'] >= AdDate)]
                shopify_model = shopify_to_model(origin, id_sort[4])
                shopify_model.to_excel(excel_writer="output/新建文件夹/%s销售model.xlsx" % shopify[i], index=False,
                                    encoding='utf-8')
                model_to_output1 = model_to_output(shopify_model, match, id_sort[4], id_sort[1], cost_price[0], cost_price[2])
                model_to_output1.to_excel(excel_writer="output/sku/%s销售sku.xlsx" % shopify[i], index=False,
                                          encoding='utf-8')
        else:
            print(sale_url + 'shopify%s销售.xlsx不存在' % XsDate)

    # PART--shopify广告
    print('shopify广告')
    for i in range(len(shopify)):
        print(shopify[i], AdDate)
        match = match_origin[match_origin['店铺'] == shopify[i]]
        match = match.drop_duplicates(subset=['渠道sku', 'ASIN'])
        match = match.reset_index(drop=True)
        if Path(sale_url + '%s广告.xlsx' % shopify[i]).exists():
            origin = pd.read_excel(sale_url + '%s广告.xlsx' % shopify[i], sheet_name=0,
                                   usecols=shopify_ad)
            if len(origin) > 0:
                for x in range(len(origin)):
                    origin.loc[x, 'Date'] = origin.loc[x, 'Date'].date()
                origin = origin[(origin['Date'] <= CurrentAdDate) & (origin['Date'] >= AdDate)]
                sd_model = shopifyad_to_model(origin, id_sort[4])
                sd_model.to_excel(excel_writer="output/新建文件夹/%ssp广告model.xlsx" %shopify[i], index=False,
                                    encoding='utf-8')
                model_to_output1 = model_to_output(sd_model, match, id_sort[4], id_sort[1], cost_price[0], cost_price[2])
                model_to_output1.to_excel(excel_writer="output/sku/%ssp广告sku.xlsx" % shopify[i],
                                          index=False,
                                          encoding='utf-8')
        else:
            print(sale_url + 'shopify%ssp广告.xlsx不存在' % XsDate)
