import pymysql
import pandas as pd
import re


def read(basename, tablename):
    connc = pymysql.Connect(
        host='rm-bp15wb9k07q3p10q6yo.mysql.rds.aliyuncs.com',
        user='onelux',
        password='123HUANGlinghong',
        database='%s' % basename,
        port=3306,
        charset='utf8'
    )

    cur = connc.cursor()
    sql = 'select * from %s;' % tablename
    cur.execute(sql)

    des = cur.description
    title = []
    for j in range(len(des)):
        title.append(des[j][0])

    origin = cur.fetchall()
    origin = pd.DataFrame(list(origin))
    origin.columns = title
    return origin


def creat_id(engine, db_name, table_name):
    # 增加主键
    with engine.connect() as con:
        con.execute("""ALTER TABLE `{}`.`{}` \
                ADD COLUMN `id` INT NOT NULL AUTO_INCREMENT FIRST, \
                ADD PRIMARY KEY (`id`);"""
                    .format(db_name, table_name))


def table_exists(basename, table_name):  # 这个函数用来判断表是否存在
    connect = pymysql.Connect(
        host='rm-bp15wb9k07q3p10q6yo.mysql.rds.aliyuncs.com',
        user='onelux',
        password='123HUANGlinghong',
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
