#!/usr/bin/env python
# encoding: utf-8
from random import Random
import pandas as pd
from pandas import Series, DataFrame

df = pd.DataFrame({'a': [1, 2, 3], 'b': [11, 22, 33], 'c': [111, 222, 333]})

def trans_data_to_dict(df, use_for='graphic', **kwargs):
    data = []
    if use_for == 'graphic':
        values_key = kwargs.get('values_key', 'series')
        category_key = kwargs.get('category_key', 'categories')
        for col in df.columns:
            col_data = {'name': col, 'data': df[col].tolist()}
            data.append(col_data)
        return {values_key: data, category_key: df.index.tolist()}
    elif use_for == 'table':
        rename_columns = kwargs.get('rename_columns', df.columns.tolist())
        index_name = kwargs.get('index_name', 'row_name')
        df[index_name] = df.index
        column_dict = dict(zip(rename_columns + ['row_name'], df.columns.tolist()))
        # >>> a = zip(['class', 'name', 'price', 'index_name'], [1, 2, 3, 4])
        # >>> print(a)
        # < zip object at 0x0351BEE0 >
        # >>> b = dict(a)
        # >>> b
        # {'class': 1, 'name': 2, 'price': 3, 'index_name': 4}
        df.rename(columns=dict(zip(df.columns.tolist(), rename_columns + ['row_name'])), inplace=True)
        # df修改成传入的columns名+'row_name',形式为columns={"A": "a", "C": "c"}
        return {'data': df.to_dict(orient='records'), 'columns': column_dict}


def trans_data_to_dict(df, use_for='graphic', **kwargs):
    data = []
    if use_for == 'graphic':
        values_key = kwargs.get('values_key', 'series')
        category_key = kwargs.get('category_key', 'categories')
        for col in df.columns:
            col_data = {'name': col, 'data': df[col].tolist()}
            data.append(col_data)
        return {values_key: data, category_key: df.index.tolist()}
    elif use_for == 'table':
        rename_columns = kwargs.get('rename_columns', df.columns.tolist())
        index_name = kwargs.get('index_name', 'row_name')
        df[index_name] = df.index
        column_dict = dict(zip(rename_columns + ['row_name'], df.columns.tolist()))
        df.rename(columns=dict(zip(df.columns.tolist(), rename_columns + ['row_name'])), inplace=True)
        return {'data': df.to_dict(orient='records'), 'columns': column_dict}




def trans_data_to_dict(df, use_for='graphic', **kwargs):
    data = []
    if use_for == 'graphic':
        values_key = kwargs.get('values_key', 'series')
        category_key = kwargs.get('category_key', 'categories')
        for col in df.columns:
            col_data = {'name': col, 'data': df[col].tolist()}
            data.append(col_data)
        return {values_key: data, category_key: df.index.tolist()}
    elif use_for == 'table':
        rename_columns = kwargs.get('rename_columns', df.columns.tolist())
        index_name = kwargs.get('index_name', 'row_name')
        df[index_name] = df.index
        column_dict = dict(zip(rename_columns + ['row_name'], df.columns.tolist()))
        df.rename(columns=dict(zip(df.columns.tolist(), rename_columns + ['row_name'])), inplace=True)
        return {'data': df.to_dict(orient='records'), 'columns': column_dict}


def random_str(randomlength=8):
    strings = ''
    chars = 'AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz0123456789'
    length = len(chars) - 1
    random = Random()
    for i in range(randomlength):
        strings += chars[random.randint(0, length)]
    return strings

def random_str(randomlength=8):
    strings = ''
    chars = 'AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz0123456789'
    length = len(chars) - 1
    random = Random()
    for i in range(randomlength):
        strings += chars[random.randint(0, length)]
    return strings

def random_str(randomlength=8):
    strings = ''
    chars = 'AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz0123456789'
    length = len(chars) - 1
    random = Random()
    for i in range(randomlength):
        strings += chars[random.randint(0, length)]
    return strings


def md5encoder(strings):
    import hashlib
    m = hashlib.md5(strings.encode(encoding="utf-8"))
    return m.hexdigest()


def md5encoder(strings):
    import hashlib
    m = hashlib.md5(strings.encode(encoding="utf-8"))
    return m.hexdigest()


def md5encoder(strings):
    import hashlib
    m = hashlib.md5(strings.encode(encoding="utf-8"))
    return m.hexdigest()

def format_sql_value(value):
    if isinstance(value, list):
        return ",".join(map(lambda x: "'{}'".format(x), value))
    elif isinstance(value, str):
        return "'{}'".format(value)

#如果是列表，变成【'xx','xx','xx'】,如果是字符串，直接变成【'xx'】
def format_sql_value(value):
    if isinstance(value, list):
        return ",".join(map(lambda x: "'{}'".format(x), value))
    elif isinstance(value, str):
        return "'{}'".format(value)

#如果是列表，变成【'xx','xx','xx'】,如果是字符串，直接变成【'xx'】
def format_sql_value(value):
    if isinstance(value, list):
        return ",".join(map(lambda x: "'{}'".format(x), value))
    elif isinstance(value, str):
        return "'{}'".format(value)

def format_sql_columns(col, table=None):
    if table is not None:
        if isinstance(col, str):
            return '{}.{}'.format(table, col)
        elif isinstance(col, list):
            return ','.join(map(lambda x: '{}.{}'.format(table, x), col))
    else:
        if isinstance(col, str):
            return col
        elif isinstance(col, list):
            return ','.join(map(lambda x: '{}'.format(x), col))


def format_sql_columns(col, table=None):
    if table is not None:
        if isinstance(col, str):
            return '{}.{}'.format(table, col)
        # 如果是字符串，格式为table.col
        elif isinstance(col, list):
            return ','.join(map(lambda x: '{}.{}'.format(table, x), col))
            # >>> ss = ','.join(map(lambda x: '{}.{}'.format('x',x),[1,2,3,4]))
            # >>> ss
            # 'x.1,x.2,x.3,x.4'
            # map()是 Python 内置的高阶函数，它接收一个函数 f 和一个 list，并通过把函数 f 依次作用在 list 的每个元素上，得到一个新的 list 并返回。

            # >>> s = ','.join(['hello','good','boy','doiido'])
            # >>> s
            # 'hello,good,boy,doiido'
    else:
        if isinstance(col, str):
            return col
        elif isinstance(col, list):
            return ','.join(map(lambda x: '{}'.format(x), col))


def format_sql_columns(col, table=None):
    if table is not None:
        if isinstance(col, str):
            return '{}.{}'.format(table, col)
        # 如果是字符串，格式为table.col
        elif isinstance(col, list):
            return ','.join(map(lambda x: '{}.{}'.format(table, x), col))
            # >>> ss = ','.join(map(lambda x: '{}.{}'.format('x',x),[1,2,3,4]))
            # >>> ss
            # 'x.1,x.2,x.3,x.4'
            # map()是 Python 内置的高阶函数，它接收一个函数 f 和一个 list，并通过把函数 f 依次作用在 list 的每个元素上，得到一个新的 list 并返回。

            # >>> s = ','.join(['hello','good','boy','doiido'])
            # >>> s
            # 'hello,good,boy,doiido'
    else:
        if isinstance(col, str):
            return col
        elif isinstance(col, list):
            return ','.join(map(lambda x: '{}'.format(x), col))


def to_list(dic):
    return [[k, v] for k, v in dic.items()]
# >>> dic = {'a':'aa','b':'bb'}
# >>> dic.items()
# dict_items([('a', 'aa'), ('b', 'bb')])
# >>> [[k, v]for k, v in dic.items()]
# [['a', 'aa'], ['b', 'bb']]
# >>>

def reverse_dict(dic):
    return {v: k for k, v in dic.items()}
# >>> ai
# {'class': 11, 'name': 1, 'price': 111}
# >>> {v:k for k,v in ai.items()}
# {11: 'class', 1: 'name', 111: 'price'}