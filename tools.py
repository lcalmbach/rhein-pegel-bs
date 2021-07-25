"""
    Collection of useful functions.
"""

__author__ = "lcalmbach@gmail.com"

import config as cn
import streamlit as st
import pandas as pd
import numpy as np
import base64
from datetime import datetime


def get_cs_item_list(lst, separator=',', quote_string=""):
    result = ''
    for item in lst:
        result += quote_string + str(item) + quote_string + separator
    result = result[:-1]
    return result


def color_gradient(row: int, value_col: str, min_val: float, max_val: float, rgb: str) -> int:
    """
    Projects a value on a color gradient scale given the min and max value.
    the color gradient type is defined in the config, e.g. blue-green, red, blue etc.
    returns a string with rgb values
    """

    result = {'r': 0, 'g': 0, 'b': 0}
    if max_val - min_val != 0:
        x = int((row[value_col] - min_val) / (max_val - min_val) * 255)
    else:
        x = 0

    if cn.GRADIENT == 'blue-green':
        if row[value_col] > max_val:
            result['r'] = 255
        else:
            result['g'] = x
            result['b'] = abs(255 - x)
    return result[rgb]


def get_pivot_data(df, group_by):
    """
    Returns a pivot table from the raw data table. df must include the station name, the data column and the
    group by column. Example
    input:
    ¦Station¦date       ¦parameter  ¦value  ¦
    -----------------------------------------
    ¦MW1    ¦1/1/2001   ¦calcium    ¦10     ¦
    ¦MW1    ¦1/1/2001   ¦chloride   ¦21     ¦

    output:
    ¦Station¦date       ¦calcium    ¦chloride   ¦
    ---------------------------------------------
    ¦MW1    ¦1/1/2001   ¦10         ¦21         ¦

    :param df:          dataframe holding the data to be pivoted
    :param group_by:
    :return:
    """

    result = pd.pivot_table(df, values=cn.VALUES_VALUE_COLUMN, index=[cn.SAMPLE_DATE_COLUMN, cn.STATION_NAME_COLUMN,
                                                                      group_by], columns=[cn.PAR_NAME_COLUMN],
                            aggfunc=np.average)
    return result


def remove_nan_columns(df: pd.DataFrame):
    """
    Removes all empty columns from a data frame. This is used to reduce unnecessary columns when displaying tables.
    Since there is only one station table but different data collection may have different data fields, often not all
    fields are used in many cases. when displaying station or parameter information, empy columns can be excluded in
    order to make the table easier to read.

    :param df: dataframe from which empty dolumns should be removed
    :return:
    """

    lis = df.loc[:, df.isna().all()]
    for col in lis:
        del df[col]
    return df


def remove_columns(df: pd.DataFrame, lis: list) -> pd.DataFrame:
    """
    Removes columns specified in a list from a data frame. This is used to reduce unnecessary columns when
    displaying tables.

    :param lis: list of columns to remove from the dataframe
    :param df: dataframe with columns to be deleted
    :return: dataframe with deleted columns
    """

    for col in lis:
        del df[col]
    return df


def get_table_download_link(df: pd.DataFrame) -> str:
    """
    Generates a link allowing the data in a given panda dataframe to be downloaded

    :param df:  table with data
    :return:    link string including the data
    """

    csv = df.to_csv(index=False)
    b64 = base64.b64encode(csv.encode()).decode()  # some strings <-> bytes conversions necessary here
    href = f'<a href="data:file/csv;base64,{b64}">im csv Format herunterladen</a>'
    return href


def transpose_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transposes a dataframe that has exactly 1 row and n columns to a table that has 2 columns and n rows. column names
    become row headers.

    Parameters:
    -----------
    :param df:
    :return:

    :param df:  dataframe to be transposed
    :return:    transposed data frame having 2 columns and n rows
    """

    result = pd.DataFrame({"Field": [], "Value": []})
    for key, value in df.iteritems():
        df2 = pd.DataFrame({"Field": [key], "Value": [df.iloc[-1][key]]})
        result = result.append(df2)
    result = result.set_index('Field')
    return result

def dic2html_table(dic: dict, key_col_width_pct: int)-> str:
    """
    Converts a key value dictionary into a html table
    """
    html_table = '<table>'
    for x in dic:
        html_table += f'<tr><td style="width: {key_col_width_pct}%;">{x}</td><td>{dic[x]}</td>'
    html_table += '</table>'
    return html_table

def left(s, amount):
    return s[:amount]

def right(s, amount):
    return s[-amount:]

def mid(s, offset, amount):
    return s[offset:offset+amount]

def percentile(n):
        def percentile_(x):
            return np.percentile(x, n)
        percentile_.__name__ = 'percentile_%s' % n
        return percentile_

def add_time_columns(df):
    df['datum'] = pd.to_datetime(df['zeit']).dt.date
    df['woche'] = df['zeit'].dt.isocalendar().week
    df['mitte_woche_datum'] = pd.to_datetime(df['zeit']) - pd.to_timedelta(df['zeit'].dt.dayofweek % 7 - 2, unit='D')
    df['mitte_woche_datum'] = df['mitte_woche_datum'].dt.date 
    df['jahr'] = df['zeit'].dt.year    
    df['monat'] = df['zeit'].dt.month  
    return df


def get_table_settings(df:pd.DataFrame):
    row_height = 40
    max_height = 400

    if len(df) > 0:
        height = (len(df) + 1) * row_height
        
        if height > max_height:
            height = max_height
        return {"width":"50%",'height':height }
    else:
        return {}


def get_base64_encoded_image(image_path):
    """
    returns bytecode for an image file
    """
    with open(image_path, "rb") as img_file:
        return base64.b64encode(img_file.read()).decode('utf-8')


def add_time_columns(df_data):            
    df = df_data
    df['datum'] = pd.to_datetime(df['zeit']).dt.date
    df['datum'] = pd.to_datetime(df['datum'])
    df['woche'] = df_data['zeit'].dt.isocalendar().week
    df['jahr'] = df_data['zeit'].dt.year    
    df['monat'] = df_data['zeit'].dt.month 
    df['mitte_woche'] = pd.to_datetime(df_data['datum']) - pd.to_timedelta(df['zeit'].dt.dayofweek % 7 - 2, unit='D')
    df['mitte_monat'] = pd.to_datetime(df['datum']) - pd.to_timedelta(df['zeit'].dt.day + 14, unit='D')
    df['mitte_jahr'] = df['datum'] - pd.to_timedelta(df['zeit'].dt.dayofyear, unit='D') + pd.to_timedelta(364/2, unit='D')
    df['stunde'] = pd.to_datetime(df['zeit']).dt.hour
    df['tag'] = df['zeit'].dt.day
    return df

def is_valid_timeagg(gl_time_agg, settings_agg)->bool:
    """
    checks if guideline time aggregation fits aggreation of values in plot
    """

    dic = {'jahr':['jahr', 'mitte_jahr'],
        'monat': ['monat', 'mitte_monat'],
        'datum': ['monat', 'datum', 'tag'],
        'stunde': ['stunde', 'zeit'],
    }
    result = False
    if gl_time_agg in dic.keys():
        result = settings_agg in dic[gl_time_agg]
    return result
