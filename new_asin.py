from dataprocess import amazon_change, add
from pathlib import Path

# 匹配表匹配值
attribute = ['渠道sku', 'ASIN']
# 各店铺匹配值
amazon_attribute = ["sku", "asin"]
amazon_ad_attribute = ["Advertised SKU", "Advertised ASIN"]
wayfair_attribute = ["Item Number"]
walmart_attribute = ["SKU"]
walmart_ad_attribute = ["Item Id"]
ebay_attribute = ["Custom Label", "Item Number"]
ebay_ad_attribute = ["Item ID"]
shopify_attribute = ["Lineitem sku"]
shopify_ad_attribute = ["SKU"]


# 新增asin

def new_asin(amazon, walmart, ebay, wayfair, shopify, sale_url, match_origin):
    for i in range(len(amazon)):

        if Path(sale_url + '%s销售.txt' % amazon[i]).exists():
            amazon_change(amazon, sale_url)
            add(amazon[i], sale_url, amazon_attribute, amazon_attribute[0], attribute, attribute[0], '销售', match_origin )
        else:
            print(sale_url + '%s销售.txt不存在' % amazon[i])
        if Path(sale_url + '%s广告.xlsx' % amazon[i]).exists():
            add(amazon[i], sale_url, amazon_ad_attribute, amazon_ad_attribute[0], attribute, attribute[0], '广告', match_origin )
        else:
            print(sale_url + '%s广告.xlsx不存在' % amazon[i])

    for i in range(len(wayfair)):
        if Path(sale_url + '%s销售.xlsx' % wayfair[i]).exists():
            add(wayfair[i], sale_url, wayfair_attribute, wayfair_attribute[0], attribute, attribute[0], '销售', match_origin )
        else:
            print(sale_url + '%s销售.xlsx不存在' % wayfair[i])

    for i in range(len(walmart)):
        if Path(sale_url + '%s销售.xlsx' % walmart[i]).exists():
            add(walmart[i], sale_url, walmart_attribute, walmart_attribute[0], attribute, attribute[0], '销售', match_origin )
        else:
            print(sale_url + '%s销售.xlsx不存在' % walmart[i])
        if Path(sale_url + '%s广告.xlsx' % walmart[i]).exists():
            add(walmart[i], sale_url, walmart_ad_attribute, walmart_ad_attribute[0], attribute, attribute[1], '广告', match_origin )
        else:
            print(sale_url + '%s广告.xlsx不存在' % walmart[i])

    for i in range(len(ebay)):
        if Path(sale_url + '%s销售.xlsx' % ebay[i]).exists():
            add(ebay[i], sale_url, ebay_attribute, ebay_attribute[0], attribute, attribute[0], '销售', match_origin )
        else:
            print(sale_url + '%s销售.xlsx不存在' % ebay[i])

        if Path(sale_url + '%s广告.xlsx' % ebay[i]).exists():
            add(ebay[i], sale_url, ebay_ad_attribute, ebay_ad_attribute[0], attribute, attribute[1], '广告', match_origin )
        else:
            print(sale_url + '%s广告.xlsx不存在' % ebay[i])

    for i in range(len(shopify)):
        if Path(sale_url + '%s销售.xlsx' % shopify[i]).exists():
            add(shopify[i], sale_url, shopify_attribute, shopify_attribute[0], attribute, attribute[0], '销售', match_origin )
        else:
            print(sale_url + '%s销售.xlsx不存在' % shopify[i])
        if Path(sale_url + '%s广告.xlsx' % shopify[i]).exists():
            add(shopify[i], sale_url, shopify_ad_attribute, shopify_ad_attribute[0], attribute, attribute[0], '广告', match_origin )
        else:
            print(sale_url + '%s广告.xlsx不存在' % shopify[i])
