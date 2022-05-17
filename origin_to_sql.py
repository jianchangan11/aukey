from sqlalchemy import create_engine
import pandas as pd
from pathlib import Path
from dataprocess import read, table_exists
from dateutil.parser import parse
import datetime

# 将原始数据写入数据库
# XsDate = '11.7'
# AdDate = '11.6'
amazon_order = ['amazon-order-id', 'merchant-order-id', 'purchase-date', 'last-updated-date', 'order-status',
                'fulfillment-channel', 'sales-channel', 'order-channel', 'url', 'ship-service-level', 'product-name',
                'sku', 'asin', 'item-status', 'quantity', 'currency', 'item-price', 'item-tax', 'shipping-price',
                'shipping-tax', 'gift-wrap-price', 'gift-wrap-tax', 'item-promotion-discount',
                'ship-promotion-discount', 'ship-city', 'ship-state', 'ship-country',
                'promotion-ids']
amazonad_order = ['Date', 'Portfolio name', 'Currency', 'Campaign Name', 'Ad Group Name', 'Advertised SKU',
                  'Advertised ASIN', 'Impressions', 'Clicks', 'Click-Thru Rate (CTR)', 'Cost Per Click (CPC)', 'Spend',
                  '7 Day Total Sales', '7 Day Total Orders (#)', '7 Day Total Units (#)',
                  '7 Day Conversion Rate', '7 Day Advertised SKU Units (#)', '7 Day Other SKU Units (#)',
                  '7 Day Advertised SKU Sales', '7 Day Other SKU Sales']
amazonsd_order = ['Date', 'Currency', 'Campaign Name', 'Ad Group Name', 'Bid Optimization',
                  'Advertised SKU', 'Advertised ASIN', 'Impressions', 'Viewable Impressions', 'Clicks',
                  'Click-Thru Rate (CTR)', 'Spend', 'Cost Per Click (CPC)',
                  'Cost per 1,000 viewable impressions (VCPM)', 'Total Advertising Cost of Sales (ACOS)',
                  'Total Return on Advertising Spend (ROAS)', '14 Day Total Orders (#)', '14 Day Total Units (#)',
                  '14 Day Total Sales', '14 Day Conversion Rate', '14 Day New-to-brand Orders (#)',
                  '14 Day New-to-brand Sales', '14 Day New-to-brand Units (#)']
amazonsb_order = ['Date', 'Portfolio name', 'Currency', 'Campaign Name', 'Impressions', 'Clicks', 'Click-Thru Rate (CTR)',
                  'Cost Per Click (CPC)', 'Spend']
wayfair_order = ['Warehouse Name', 'Store Name', 'PO Number', 'PO Date', 'Must Ship By', 'Backorder Date',
                 'Order Status', 'Item Number', 'Item Name', 'Quantity', 'Wholesale Price', 'Ship Method',
                 'Carrier Name', 'Shipping Account Number', 'Ship To Name', 'Ship To Address', 'Ship To Address 2',
                 'Ship To City', 'Ship To State', 'Ship To Zip', 'Ship To Phone', 'Inventory at PO Time',
                 'Inventory Send Date', 'Ship Speed', 'PO Date & Time', 'Registered Timestamp', 'Customization Text',
                 'Event Name', 'Event ID', 'Event Start Date', 'Event End Date', 'Event Type', 'Backorder Reason',
                 'Original Product ID', 'Original Product Name', 'Event Inventory Source', 'Packing Slip URL',
                 'Tracking Number', 'Ready for Pickup Date', 'SKU', 'Destination Country', 'Depot ID', 'Depot Name',
                 'Wholesale Event Source', 'Wholesale Event Store Source', 'B2BOrder', 'Composite Wood Product',
                 'Sales Channel']
ebay_order = ['Sales Record Number', 'Order Number', 'Buyer Username', 'Buyer Name', 'Buyer Email', 'Buyer Note',
              'Buyer Address 1', 'Buyer Address 2', 'Buyer City', 'Buyer State', 'Buyer Zip', 'Buyer Country',
              'Buyer Tax Identifier Name', 'Buyer Tax Identifier Value', 'Ship To Name', 'Ship To Phone',
              'Ship To Address 1', 'Ship To Address 2', 'Ship To City', 'Ship To State', 'Ship To Zip',
              'Ship To Country', 'Item Number', 'Item Title', 'Custom Label', 'Sold Via Promoted Listings', 'Quantity',
              'Sold For', 'Shipping And Handling', 'Item Location', 'Item Zip Code', 'Item Country',
              'eBay Collect And Remit Tax Rate', 'eBay Collect And Remit Tax Type', 'eBay Reference Name',
              'eBay Reference Value', 'Tax Status', 'Seller Collected Tax', 'eBay Collected Tax',
              'Electronic Waste Recycling Fee', 'Mattress Recycling Fee', 'Battery Recycling Fee',
              'White Goods Disposal Tax', 'Tire Recycling Fee', 'Additional Fee', 'Total Price',
              'eBay Collected Tax and Fees Included in Total', 'Payment Method', 'Sale Date', 'Paid On Date',
              'Ship By Date', 'Minimum Estimated Delivery Date', 'Maximum Estimated Delivery Date', 'Shipped On Date',
              'Feedback Left', 'Feedback Received', 'My Item Note', 'PayPal Transaction ID', 'Shipping Service',
              'Tracking Number', 'Transaction ID', 'Variation Details', 'Global Shipping Program',
              'Global Shipping Reference ID', 'Click And Collect', 'Click And Collect Reference Number', 'eBay Plus',
              'Authenticity Verification Program', 'Authenticity Verification Status',
              'Authenticity Verification Outcome Reason', 'eBay Fulfillment Program', 'Tax City', 'Tax State',
              'Tax Zip', 'Tax Country']
walmart_order = ['PO#', 'Order#', 'Order Date', 'Ship By', 'Delivery Date', 'Customer Name',
                 'Customer Shipping Address', 'Customer Phone Number', 'Ship to Address 1',
                 'Ship to Address 2', 'City', 'State', 'Zip', 'Segment', 'FLIDS', 'Line#', 'UPC',
                 'Status', 'Item Description', 'Shipping Method', 'Shipping Tier', 'Shipping SLA',
                 'Shipping Config SOurce', 'Qty', 'SKU', 'Item Cost', 'Shipping Cost',
                 'Tax', 'Update Status', 'Update Qty', 'Carrier', 'Tracking Number',
                 'Tracking Url', 'Seller Order NO', 'Fulfillment Entity', 'Replacement Order',
                 'Original Customer Order Id']
walmart_ad_order = ['Date', 'Campaign Name', 'Campaign Id', 'Ad Group Name', 'Ad Group Id',
                    'Item Id', 'Item Name', 'Impressions', 'Clicks', 'CTR', 'Ad Spend',
                    'Conversion Rate', 'Total Attributed Sales', 'Units Sold', 'Attributed Direct View Sales',
                    'Attributed Brand View Sales']
ebay_ad_order = ['Item ID', 'Item title', 'Date sold', 'Sold qty', 'Sold for', 'Ad fees']


def origin_to_sql(amazon, walmart, ebay, wayfair, sale_url, AdDate, CurrentAdDate):
    for i in range(len(amazon)):
        print(amazon[i])
        if Path(sale_url + '%s销售.xlsx' % amazon[i]).exists():
            finalname = '%s销售%s' % (amazon[i], AdDate)
            origin = pd.read_excel(sale_url+'%s销售.xlsx' % amazon[i])
            for x in range(len(origin)):
                origin.loc[x, 'purchase-date'] = parse(origin.loc[x, 'purchase-date']) - datetime.timedelta(hours=8)
                origin.loc[x, 'purchase-date'] = (origin.loc[x, 'purchase-date']).date()
            origin = origin[(origin['purchase-date'] <= CurrentAdDate) & (origin['purchase-date'] >= AdDate)]
            origin = origin.rename(columns={'price-designation ': 'price-designation'})
            origin = origin.rename(columns={'promotion-ids ': 'promotion-ids'})
            origin = origin[amazon_order]
            engine = create_engine(
                'mysql+pymysql://test_admin:123456@rm-bp15wb9k07q3p10q6yo.mysql.rds.aliyuncs.com/temporary')
            if len(origin) > 0:
                origin.to_sql(finalname, con=engine, if_exists='replace', index=False)
        else:
            print(sale_url+'%s销售.xlsx不存在' % amazon[i])

    for i in range(len(amazon)):
        if Path(sale_url+'%s广告.xlsx' % amazon[i]).exists():
            finalname = '%s广告%s' % (amazon[i], AdDate)
            origin = pd.read_excel(sale_url+'%s广告.xlsx' % amazon[i])
            for x in range(len(origin)):
                origin.loc[x, 'Date'] = (origin.loc[x, 'Date']).date()
            origin = origin[(origin['Date'] <= CurrentAdDate) & (origin['Date'] >= AdDate)]
            origin = origin.rename(columns={'7 Day Total Sales ': '7 Day Total Sales'})
            origin = origin.rename(
                columns={'Total Advertising Cost of Sales (ACoS) ': 'Total Advertising Cost of Sales (ACoS)'})
            origin = origin.rename(columns={'7 Day Advertised SKU Sales ': '7 Day Advertised SKU Sales'})
            origin = origin.rename(columns={'7 Day Other SKU Sales ': '7 Day Other SKU Sales'})
            origin = origin[amazonad_order]
            engine = create_engine(
                'mysql+pymysql://test_admin:123456@rm-bp15wb9k07q3p10q6yo.mysql.rds.aliyuncs.com/temporary')
            if len(origin) > 0:
                origin.to_sql(finalname, con=engine, if_exists='replace', index=False)
        else:
            print(sale_url+'%s广告不存在' % amazon[i])

    for i in range(len(amazon)):
        if Path(sale_url+'%ssd广告.xlsx' % amazon[i]).exists():
            finalname = '%ssd广告%s' % (amazon[i], AdDate)
            origin = pd.read_excel(sale_url+'%ssd广告.xlsx' % amazon[i])
            for x in range(len(origin)):
                origin.loc[x, 'Date'] = (origin.loc[x, 'Date']).date()
            origin = origin[(origin['Date'] <= CurrentAdDate) & (origin['Date'] >= AdDate)]
            origin = origin.rename(
                columns={'Total Advertising Cost of Sales (ACOS) ': 'Total Advertising Cost of Sales (ACOS)'})
            origin = origin.rename(columns={'14 Day Total Sales ': '14 Day Total Sales'})
            origin = origin[amazonsd_order]
            engine = create_engine(
                'mysql+pymysql://test_admin:123456@rm-bp15wb9k07q3p10q6yo.mysql.rds.aliyuncs.com/temporary')
            if len(origin) > 0:
                origin.to_sql(finalname, con=engine, if_exists='replace', index=False)
        else:
            print(sale_url+'%ssd广告.xlsx不存在' % amazon[i])

    for i in range(len(amazon)):
        if Path(sale_url+'%ssb广告.xlsx' % amazon[i]).exists():
            finalname = '%ssb广告%s' % (amazon[i], AdDate)
            origin = pd.read_excel(sale_url+'%ssb广告.xlsx' % amazon[i])
            for x in range(len(origin)):
                origin.loc[x, 'Date'] = (origin.loc[x, 'Date']).date()
            origin = origin[(origin['Date'] <= CurrentAdDate) & (origin['Date'] >= AdDate)]
            origin = origin[amazonsb_order]
            engine = create_engine(
                'mysql+pymysql://test_admin:123456@rm-bp15wb9k07q3p10q6yo.mysql.rds.aliyuncs.com/temporary')
            if len(origin) > 0:
                origin.to_sql(finalname, con=engine, if_exists='replace', index=False)
        else:
            print(sale_url+'%ssb广告.xlsx不存在' % amazon[i])

    for i in range(len(amazon)):
        if Path(sale_url+'%ssbv广告.xlsx' % amazon[i]).exists():
            finalname = '%ssbv广告%s' % (amazon[i], AdDate)
            origin = pd.read_excel(sale_url+'%ssbv广告.xlsx' % amazon[i])
            for x in range(len(origin)):
                origin.loc[x, 'Date'] = (origin.loc[x, 'Date']).date()
            origin = origin[(origin['Date'] <= CurrentAdDate) & (origin['Date'] >= AdDate)]
            origin = origin[amazonsb_order]
            engine = create_engine(
                'mysql+pymysql://test_admin:123456@rm-bp15wb9k07q3p10q6yo.mysql.rds.aliyuncs.com/temporary')
            if len(origin) > 0:
                origin.to_sql(finalname, con=engine, if_exists='replace', index=False)
        else:
            print(sale_url+'%ssb广告.xlsx不存在' % amazon[i])

    for i in range(len(wayfair)):
        if Path(sale_url+'%s销售.xlsx' % wayfair[i]).exists():
            finalname = '%s销售%s' % (wayfair[i], AdDate)
            origin = pd.read_excel(sale_url+'%s销售.xlsx' % wayfair[i])
            origin["PO Date"] = pd.to_datetime(origin["PO Date"])
            for x in range(len(origin)):
                origin.loc[x, "PO Date"] = origin.loc[x, "PO Date"].date()
            origin = origin[(origin['PO Date'] <= CurrentAdDate) & (origin['PO Date'] >= AdDate)]
            origin = origin[wayfair_order]
            engine = create_engine(
                'mysql+pymysql://test_admin:123456@rm-bp15wb9k07q3p10q6yo.mysql.rds.aliyuncs.com/temporary')
            if len(origin) > 0:
                origin.to_sql(finalname, con=engine, if_exists='replace', index=False)
        else:
            print(sale_url+'%s销售.xlsx不存在' % wayfair[i])

    for i in range(len(ebay)):
        if Path(sale_url+'%s销售.xlsx' % ebay[i]).exists():
            finalname = '%s销售%s' % (ebay[i], AdDate)
            origin = pd.read_excel(sale_url+'%s销售.xlsx' % ebay[i])
            for x in range(len(origin)):
                origin.loc[x, 'Sale Date'] = datetime.datetime.strptime(origin.loc[x, 'Sale Date'],
                                                                        '%b-%d-%y').date()
            origin = origin[(origin['Sale Date'] <= CurrentAdDate) & (origin['Sale Date'] >= AdDate)]
            origin = origin[ebay_order]
            engine = create_engine(
                'mysql+pymysql://test_admin:123456@rm-bp15wb9k07q3p10q6yo.mysql.rds.aliyuncs.com/temporary')
            if len(origin) > 0:
                origin.to_sql(finalname, con=engine, if_exists='replace', index=False)
        else:
            print(sale_url+'%s销售.xlsx不存在' % ebay[i])

    for i in range(len(ebay)):
        if Path(sale_url+'%s广告.xlsx' % ebay[i]).exists():
            finalname = '%s广告%s' % (ebay[i], AdDate)
            origin = pd.read_excel(sale_url+'%s广告.xlsx' % ebay[i])
            for x in range(len(origin)):
                origin.loc[x, 'Date sold'] = datetime.datetime.strptime(origin.loc[x, 'Date sold'],
                                                                        '%b %d, %Y %I:%M %p -08:00').date()
            origin = origin[(origin['Date sold'] <= CurrentAdDate) & (origin['Date sold'] >= AdDate)]
            origin = origin[ebay_ad_order]
            engine = create_engine(
                'mysql+pymysql://test_admin:123456@rm-bp15wb9k07q3p10q6yo.mysql.rds.aliyuncs.com/temporary')
            if len(origin) > 0:
                origin.to_sql(finalname, con=engine, if_exists='replace', index=False)
        else:
            print(sale_url+'%s广告.xlsx不存在' % ebay[i])

    for i in range(len(walmart)):
        if Path(sale_url+'%s销售.xlsx' % walmart[i]).exists():
            finalname = '%s销售%s' % (walmart[i], AdDate)
            origin = pd.read_excel(sale_url+'%s销售.xlsx' % walmart[i])
            origin['Order Date'] = pd.to_datetime(origin['Order Date'])
            for x in range(len(origin)):
                origin.loc[x, 'Order Date'] = origin.loc[x, 'Order Date'].date()
            origin = origin[(origin['Order Date'] <= CurrentAdDate) & (origin['Order Date'] >= AdDate)]
            origin = origin[walmart_order]
            engine = create_engine(
                'mysql+pymysql://test_admin:123456@rm-bp15wb9k07q3p10q6yo.mysql.rds.aliyuncs.com/temporary')
            if len(origin) > 0:
                origin.to_sql(finalname, con=engine, if_exists='replace', index=False)
        else:
            print(sale_url+'%s销售.xlsx不存在' % walmart[i])

    for i in range(len(walmart)):
        if Path(sale_url+'%s广告.xlsx' % walmart[i]).exists():
            finalname = '%s广告%s' % (walmart[i], AdDate)
            origin = pd.read_excel(sale_url+'%s广告.xlsx' % walmart[i])
            for x in range(len(origin)):
                origin.loc[x, 'Date'] = (origin.loc[x, 'Date']).date()
            origin = origin[(origin['Date'] <= CurrentAdDate) & (origin['Date'] >= AdDate)]
            origin = origin[walmart_ad_order]
            engine = create_engine(
                'mysql+pymysql://test_admin:123456@rm-bp15wb9k07q3p10q6yo.mysql.rds.aliyuncs.com/temporary')
            if len(origin) > 0:
                origin.to_sql(finalname, con=engine, if_exists='replace', index=False)
        else:
            print(sale_url+'%s销售.xlsx不存在' % walmart[i])


def origin_sql_total(amazon, walmart, ebay, wayfair, AdDate):
    for i in range(len(amazon)):
        print(amazon[i])
        if table_exists('temporary', '%s销售%s' % (amazon[i], AdDate)):
            print('temporary', '%s销售' % amazon[i])
            finalname = '%s' % (amazon[i] + '销售2022')
            origin = read('temporary', '%s销售%s' % (amazon[i], AdDate))
            engine = create_engine('mysql+pymysql://test_admin:123456@rm-bp1530atrq39n7od4qo.mysql.rds.aliyuncs'
                                   '.com/origin')
            if len(origin) > 0:
                origin.to_sql(finalname, con=engine, if_exists='replace', index=False)
        else:
            print('%s销售导入失败' % amazon[i])

    for i in range(len(amazon)):
        if table_exists('temporary', '%s广告%s' % (amazon[i], AdDate)):
            print('temporary', '%s广告%s' % (amazon[i], AdDate))
            finalname = '%s' % (amazon[i] + '广告2022')
            origin = read('temporary', '%s广告%s' % (amazon[i], AdDate))
            engine = create_engine(
                'mysql+pymysql://test_admin:123456@rm-bp15wb9k07q3p10q6yo.mysql.rds.aliyuncs.com/origin')
            if len(origin) > 0:
                origin.to_sql(finalname, con=engine, if_exists='replace', index=False)
        else:
            print('%s广告导入失败' % amazon[i])

    for i in range(len(amazon)):
        if table_exists('temporary', '%ssd广告%s' % (amazon[i], AdDate)):
            print('temporary', '%ssd广告%s' % (amazon[i], AdDate))
            finalname = '%s' % (amazon[i] + 'sd广告2022')
            origin = read('temporary', '%ssd广告%s' % (amazon[i], AdDate))
            engine = create_engine(
                'mysql+pymysql://test_admin:123456@rm-bp15wb9k07q3p10q6yo.mysql.rds.aliyuncs.com/origin')
            if len(origin) > 0:
                origin.to_sql(finalname, con=engine, if_exists='replace', index=False)
        else:
            print('%ssd广告导入失败' % amazon[i])

    for i in range(len(amazon)):
        if table_exists('temporary', '%ssb广告%s' % (amazon[i], AdDate)):
            print('temporary', '%ssb广告%s' % (amazon[i], AdDate))
            finalname = '%s' % (amazon[i] + 'sb广告2022')
            origin = read('temporary', '%ssb广告%s' % (amazon[i], AdDate))
            engine = create_engine(
                'mysql+pymysql://test_admin:123456@rm-bp15wb9k07q3p10q6yo.mysql.rds.aliyuncs.com/origin')
            if len(origin) > 0:
                origin.to_sql(finalname, con=engine, if_exists='replace', index=False)
        else:
            print('%ssb广告导入失败' % amazon[i])

    for i in range(len(amazon)):
        if table_exists('temporary', '%ssbv广告%s' % (amazon[i], AdDate)):
            print('temporary', '%ssbv广告%s' % (amazon[i], AdDate))
            finalname = '%s' % (amazon[i] + 'sbv广告2022')
            origin = read('temporary', '%ssbv广告%s' % (amazon[i], AdDate))
            engine = create_engine(
                'mysql+pymysql://test_admin:123456@rm-bp15wb9k07q3p10q6yo.mysql.rds.aliyuncs.com/origin')
            if len(origin) > 0:
                origin.to_sql(finalname, con=engine, if_exists='replace', index=False)
        else:
            print('%ssbv广告导入失败' % amazon[i])

    for i in range(len(wayfair)):
        if table_exists('temporary', '%s销售%s' % (wayfair[i], AdDate)):
            print('temporary', '%s销售' % wayfair[i])
            finalname = '%s' % (wayfair[i] + '销售2022')
            origin = read('temporary', '%s销售%s' % (wayfair[i], AdDate))
            engine = create_engine(
                'mysql+pymysql://test_admin:123456@rm-bp15wb9k07q3p10q6yo.mysql.rds.aliyuncs.com/origin')
            if len(origin) > 0:
                origin.to_sql(finalname, con=engine, if_exists='replace', index=False)
        else:
            print('%s销售导入失败' % wayfair[i])

    for i in range(len(ebay)):
        if table_exists('temporary', '%s销售%s' % (ebay[i], AdDate)):
            print('temporary', '%s销售' % ebay[i])
            finalname = '%s' % (ebay[i] + '销售2022')
            origin = read('temporary', '%s销售%s' % (ebay[i], AdDate))
            engine = create_engine(
                'mysql+pymysql://test_admin:123456@rm-bp15wb9k07q3p10q6yo.mysql.rds.aliyuncs.com/origin')
            if len(origin) > 0:
                origin.to_sql(finalname, con=engine, if_exists='replace', index=False)
        else:
            print('%s销售导入失败' % ebay[i])

    for i in range(len(ebay)):
        if table_exists('temporary', '%s广告%s' % (ebay[i], AdDate)):
            print('temporary', '%s广告' % ebay[i])
            finalname = '%s' % (ebay[i] + '广告2022')
            origin = read('temporary', '%s广告%s' % (ebay[i], AdDate))
            engine = create_engine(
                'mysql+pymysql://test_admin:123456@rm-bp15wb9k07q3p10q6yo.mysql.rds.aliyuncs.com/origin')
            if len(origin) > 0:
                origin.to_sql(finalname, con=engine, if_exists='replace', index=False)
        else:
            print('%s广告导入失败' % ebay[i])

    for i in range(len(walmart)):
        if table_exists('temporary', '%s销售%s' % (walmart[i], AdDate)):
            print('temporary', '%s销售' % walmart[i])
            finalname = '%s' % (walmart[i] + '销售2022')
            origin = read('temporary', '%s销售%s' % (walmart[i], AdDate))
            engine = create_engine(
                'mysql+pymysql://test_admin:123456@rm-bp15wb9k07q3p10q6yo.mysql.rds.aliyuncs.com/origin')
            if len(origin) > 0:
                origin.to_sql(finalname, con=engine, if_exists='replace', index=False)
        else:
            print('%s销售%s fail' % walmart[i])

    for i in range(len(walmart)):
        print('temporary', '%s广告' % walmart[i])
        if table_exists('temporary', '%s广告%s' % (walmart[i], AdDate)):
            finalname = '%s' % (walmart[i] + '广告2022')
            origin = read('temporary', '%s广告%s' % (walmart[i], AdDate))
            engine = create_engine(
                'mysql+pymysql://test_admin:123456@rm-bp15wb9k07q3p10q6yo.mysql.rds.aliyuncs.com/origin')
            if len(origin) > 0:
                origin.to_sql(finalname, con=engine, if_exists='replace', index=False)
        else:
            print('%s广告%s fail' % walmart[i])
