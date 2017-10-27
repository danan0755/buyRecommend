# encoding=utf-8
# -*- coding: UTF-8 -*-
import pymysql
from langdetect import detect
from langdetect import detect_langs

db = pymysql.connect(host='dev.forcastdbm.kokoerp.com',user='forcast',passwd='V6PfT7Y1mE2sF2M2', db='forcast', charset="utf8")
str = ""
# 使用cursor()方法获取操作游标
cursor = db.cursor()
sql = "SELECT  `id` , `description`,`bannertext` FROM  `ebay_sku_onsale_out_1024` WHERE desc_langid= ''"
cursor.execute(sql)
results = cursor.fetchall()
# print(results)
for row in results:
    id = row[0]
    description = row[1]
    bannertext = row[2]
    print( description )
    print( bannertext )
    description_language = "no:0"
    bannertext_language = "no:0"
    if description!="":
        description_language = repr( detect_langs( description )[0] )
    if bannertext!="":
        bannertext_language = repr(detect_langs(bannertext)[0])
    desc_lang = description_language.split(":")[0]
    desc_prob = description_language.split(":")[1]
    banner_lang = bannertext_language.split(":")[0]
    banner_prob = bannertext_language.split(":")[1]
    print("description_language:"+ description_language)
    print("bannertext_language:"+ bannertext_language)

    # str_lang = description_language[0]+"+"+bannertext_language[0]
    print("----------->>>-------->>>" + desc_lang+"---------->>desc_prob:"+desc_prob)
    print("----------->>>-------->>>" + banner_lang + "---------->>banner_prob:" + banner_prob)
    sql2 = "UPDATE `ebay_sku_onsale_out_1024` SET `desc_langid`='%s',`bannertext_langid`='%s',`desc_prob`='%s',`banner_prob`='%s' WHERE  `id` = '%d'" \
           % (desc_lang,banner_lang,desc_prob,banner_prob,id);
    print(".............",sql2)
    cursor.execute(sql2)
    db.commit()
db.close()