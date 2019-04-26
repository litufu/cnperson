import json
import os
import pypinyin
import sqlite3


def get_tables(db_file):
    # 获取sqlite中所有的表
    try:
        conn = sqlite3.connect(db_file)
        cur = conn.cursor()
        cur.execute("select name from sqlite_master where type='table' order by name")
        print(cur.fetchall())
    except sqlite3.Error as e:
            print(e)


def delete_table(name):
    # 删除表
    conn = sqlite3.connect('cnperson.db')
    cursor = conn.cursor()
    cursor.execute("DELETE from {}".format(name))
    cursor.close()
    conn.commit()
    conn.close()


def hp(word):
    # 将汉字字符串转化为汉语拼音
    s = ''
    for i in pypinyin.pinyin(word, style=pypinyin.NORMAL):
        s += ''.join(i)
    return s


def save_record(filename, record):
    fp = '{}.txt'.format(filename)
    if os.path.exists(fp):
        with open(fp, 'r') as f:
            data = json.load(f)
        if not isinstance(data, list):
            data = []
    else:
        data = []
    with open(fp, 'w', encoding='utf-8') as f:
        if isinstance(record, list):
            data.extend(record)
            json.dump(data, f)
        elif isinstance(record, str):
            data.append(record)
            json.dump(data, f)
        else:
            raise Exception("记录必须为一个字符串或列表")


def has_record(filename, record):
    if not isinstance(record, str):
        raise Exception("记录必须为一个字符串")
    fp = '{}.txt'.format(filename)
    if os.path.exists(fp):
        with open(fp, 'r') as f:
            data = json.load(f)
        return isinstance(data, list) and record in data
    else:
        return False

