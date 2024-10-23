#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
from io import BytesIO
import requests
from urllib.parse import urlencode
import urllib
from requests.auth import HTTPBasicAuth
from requests.exceptions import ChunkedEncodingError
import os
import json
import yadisk
from datetime import datetime, date, timedelta
import locale
from time import sleep
import shutil
import gc
import turbodbc
from turbodbc import connect
import gc
from pandas.api.types import is_string_dtype
import numpy as np
from sqlalchemy import create_engine
import pyodbc
import warnings
import re

import config
from yandex_disk_func import *
from parse_functions import *

# забираем Яндекс токен
yandex_token = config.yandex_token
# # указываем путь к основной папке, в которой храняться папки с флайтами
main_folder = config.main_folder

# забираем токен для подключения к гугл
service_key = config.service
gmail = config.gmail


# In[ ]:


warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')
pd.options.mode.chained_assignment = None


# In[ ]:


base_cols = ['supplier', 'report_name', 'sheet_name', 'brand', 'period', 'source', 'site/ssp', 'placement', 
                     'targeting', 'geo', 'soc_dem', 'ad copy format', 'rotation type', 
                     'unit price', 'frequency', 'reach', 'impressions', 'clicks', 
             'budget_without_nds', 'budget_nds', 'views', 'vtr, %']


# In[ ]:





# In[ ]:


# источник Beeline
# типы размещения Видео и Баннерная реклама
# Функция для обработки медиаплана 
# 1. Медиаплан для обработки находится на листе Plan_Media 
# 2. В столбике B  находится слово Brand справа от него в столбике C название Бренда
# 3. В столбике B  находится слово Period справа от него в столбике C название указан период медиаплана
# 4. В столбике B  должно находиться поле Source
# 5. Каждая таблица должна заканчиваться строкой итогов 

def get_beeline_mediaplan(data_link, network, report_name):
    sheet_name='Plan_Media'
    df = pd.read_excel(BytesIO(data_link), sheet_name=sheet_name, header=None)
    
    sheet_name = normalize_headers(sheet_name)
    
    # забираем индекс начала таблицы
    start_index = get_index_row(df, 1, 'source')
    # забираем название полей из файла
    col_names_list = df.iloc[start_index].fillna('').apply(normalize_headers)
    
    ffill_columns = [1, 2, 3, 4]
    df[ffill_columns] = df[ffill_columns].ffill(limit=1) # заполняем вниз
    df = df.fillna('')
    
    # сохраняем название бренда
    brand = df[2].loc[get_index_row(df, 1, 'brand')] 
    # сохраняем период
    period = df[2].loc[get_index_row(df, 1, 'period')]
    
    # задаем названия полей
    df.columns = df.iloc[start_index].apply(normalize_headers) # забираем название полей из файла
    # обрезаем верхнюю часть таблицы. она больше не нужна
    df = df.iloc[start_index+2:].reset_index(drop=True)
    # забираем окончание таблицы
    end_index = get_index_row(df, 'source', 'итого')
    # обрезаем таблицу снизу
    df = df.iloc[:end_index].reset_index(drop=True)
    # создаем базовый список полей, которые есть всегда вне зависимости от типа размещения
    standart_columns = ['source', 'site/ssp', 'placement', 'targeting', 'ad copy format', 'unit', 'rotation type', 
          'unit price', 'frequency', 'reach', 'impressions', 'clicks', 'ratecard price per period (rub, net)']
    
    # проверяем наличие Видео размещений. Если они есть, то используем дополнительные поля из таблицы
    # если Видео размещений нет, то добавляем дополнительно 2 поля с 0 (это нужно для нормализации общей таблицы
    if 'views' not in list(df.columns):
        df['views'] = 0
        df['vtr, %'] = 0.0
        
    standart_columns += ['views', 'vtr, %']
    # оставляем только нужные поля
    df = df[standart_columns]
    df['views'] = df['views'].apply(normalize_digits)
    df['vtr, %'] = df['vtr, %'].apply(normalize_digits)
    df['views'] = df['views'].astype('int')
    df['vtr, %'] = df['vtr, %'].astype('float')
    
     # переименовываем поля
    df = df.rename(columns={'ratecard price per period (rub, net)': 'budget_without_nds'})
    # добавляем поля с общей информацией
    df['supplier'] = network
    df['report_name'] = report_name
    df['sheet_name'] = 'Plan_Media'
    df['brand'] = brand
    df['period'] = period

    # парсим Гео и Соц. дем из поля targeting
    df['geo'] = df['targeting'].apply(lambda x: get_targetings(x, 'гео:','\n', flag='geo'))
    df['soc_dem'] = df['targeting'].apply(lambda x: get_targetings(x, 'ца:','\n', flag='soc'))
    
    # убираем знак рубля, если он есть в стоимости
    currecny_columns = ['unit']
    df[currecny_columns] = df[currecny_columns].apply(get_digits)
    
    # # добавляем рассчитываемые показатели
    # # df['budget_without_nds'] = ((df['unit quantity'] / 1000) * df['unit price']).astype('float')
    df['budget_without_nds'] = df['budget_without_nds'].astype('float')
    df['budget_nds'] =(df['budget_without_nds'] * 1.2).astype('float').round(2)
    # если в этих полях встречаются пустые ячейки, то заменяем их на 0
    df['vtr, %'] = df['vtr, %'].apply(replace_blank)
    df['views'] = df['views'].apply(replace_blank)
    
    # переставляем поля местами, чтобы все было единообразно
    df = df[base_cols]
    return df


# In[ ]:


# источник FirstData
# типы размещения Видео и Баннерная реклама
# Функция для обработки медиаплана 
# 1. Медиаплан для обработки находится на листе Plan_Media 
# 2. В столбике B  находится слово Brand справа от него в столбике C название Бренда
# 3. В столбике B  находится слово Period справа от него в столбике C название указан период медиаплана
# 4. В столбике B  должно находиться поле Source
# 5. Каждая таблица должна заканчиваться строкой итогов 
# 6. В столбике С находятся Таргетинги - Targeting by purchase 
# 7. В столбике I находится тип размещения (CPC, CPM)

def get_firstdata_mediaplan(data_link, network, report_name):
    tmp_dict = {}
    sheet_names = pd.ExcelFile(BytesIO(data_link))
    for sheet_name in sheet_names.sheet_names:
        if 'mediaplan' in sheet_name:
            df = pd.read_excel(BytesIO(data_link), sheet_name=sheet_name)
            # приводим в порядок название листа, чтобы его записать в новую таблицу
            sheet_name = normalize_headers(sheet_name)
            print(f'    {sheet_name}')
            # заполняем вниз название истоника
            df['Unnamed: 1'] = df['Unnamed: 1'].ffill()
            # заполняем вниз таргетинги
            df['Unnamed: 2'] = df['Unnamed: 2'].ffill()
            # заполяем вниз rotation type - здесь объединенная ячейка, и в этом поле нет названия
            # чтобы оно появилось, заранее протягиваем вниз это название
            df['Unnamed: 8'] = df['Unnamed: 8'].fillna('rotation type')
            # заполняем вниз объединенные ячейки
            df = df.fillna('')
            
            # сохраняем название бренда
            brand = df['Unnamed: 3'].loc[get_index_row(df, 'Unnamed: 2', 'brand')] 
            # сохраняем период
            period = df['Unnamed: 3'].loc[get_index_row(df, 'Unnamed: 2', 'период')]
            # забираем индекс начала таблицы
            start_index = get_index_row(df, 'Unnamed: 1', 'category')
            
            # задаем названия полей
            df.columns = df.iloc[start_index].apply(normalize_headers) # забираем название полей из файла
            # обрезаем верхнюю часть таблицы. она больше не нужна
            df = df.iloc[start_index+2:].reset_index(drop=True)
            # забираем окончание таблицы
            end_index = get_index_row(df, 'category', 'total')
            # обрезаем таблицу снизу
            df = df.iloc[:end_index].reset_index(drop=True)
            # создаем базовый список полей, которые есть всегда вне зависимости от типа размещения
            standart_columns = ['category', 'targeting by purchase', 'format', 'period', 
                            'price list cost (cost per unit) net ', 'rotation type',
                            'total price list cost net', 'reach forecast (uu)', 'frequency total till',
                            'impressions', 'clicks']
            # проверяем наличие Видео размещений. Если они есть, то используем дополнительные поля из таблицы
            # если Видео размещений нет, то добавляем дополнительно 2 поля с 0 (это нужно для нормализации общей таблицы     
            if 'vtr,%' not in list(df.columns):
                df['number of views'] = 0
                df['vtr,%'] = 0.0
                
            standart_columns += ['number of views', 'vtr,%']
            # оставляем только нужные поля
            df = df[standart_columns]
            # приводим названия полей к единому стандарту
            df = df.rename(columns={'category': 'source', 'targeting by purchase': 'targeting', 'format': 'ad copy format', 
                                     'price list cost (cost per unit) net ': 'unit price', 
                                    'frequency total till': 'frequency', 'reach forecast (uu)': 'reach', 
                                    'total price list cost net' :'budget_without_nds', 'number of views': 'views', 'vtr,%': 'vtr, %'})
    
            # некоторые типы размещений имеют объединенные строки
            # например Баннер и универсальный баннер - это 2 строки с объединенными ячейками по расходам, показам и тд.
            # поэтому создадим доп. поле, где соединим их названия в одну строку
            # создаем пустое поле
            df['merge_type_cells'] = ''
            # проходим через цикл по датаФрейму
            # в первой строке по определению не может быть данных, поэтому сохраняем там название формата
            # если это не первая и не посленяя строка, то нам нужно провести проверку
            # допустим мы находимся в строке номер 2
            # мы проверяем, что находится в строке номер 3 в поле rotation type (на первых шагах мы сделали заполнение вниз)
            # соответсвенно если ячейка была пустая, там появится надпись rotation type - так мы поймем, что это как раз объединенные данные
            # берем название формата из текущей строки и добавляем к нему название формата из следующей строки
            # во всех остальных случаях просто возвращаем название формата из текущей строки
            for i in range(len(df)):
                base_name = df['ad copy format'][i]
            
                if i < len(df)-1:
                    if df['rotation type'][i+1] == 'rotation type':
                        base_name = base_name + ' / ' + str(df['ad copy format'][i+1])
                else:
                    base_name = df['ad copy format'][i]
                df['merge_type_cells'][i] =  base_name
    
            # последняя строка является объединенной Сначала идет строка Баннеры - в ней все цифры
            # Второая строка Универсальные баннеры - в ней нет значений потому что поставщик считает, что это одно и тоже
            # мы убираем такие строки без данных
            df = df[df['impressions']!='']
            # передаем новое название формата в нужное нам поле
            df['ad copy format'] = df['merge_type_cells']
            df = df.drop('merge_type_cells', axis=1)
            # добавляем поля с общей информацией
            df['supplier'] = network
            df['report_name'] = report_name
            df['sheet_name'] = sheet_name
            df['brand'] = brand
            df['period'] = period
            df['site/ssp'] = ''
            df['placement'] = ''
            df['budget_nds'] =(df['budget_without_nds'] * 1.2).astype('float').round(2)
           
            # парсим Гео и Соц. дем из поля targeting
            df['geo'] = df['targeting'].apply(lambda x: get_targetings(x, 'гео:','\n', flag='geo'))
            df['soc_dem'] = df['targeting'].apply(lambda x: get_targetings(x, 'ца:','покупатели', flag='soc'))
            
            # если в этих полях встречаются пустые ячейки, то заменяем их на 0
            df['vtr, %'] = df['vtr, %'].apply(replace_blank)
            df['views'] = df['views'].apply(replace_blank)
            # переставляем поля местами, чтобы все было единообразно
            df = df[base_cols]
            tmp_dict[sheet_name] = df

    return pd.concat(tmp_dict, ignore_index=True)


# In[ ]:


# источник Hybrid
# типы размещения Видео и Баннерная реклама
# Функция для обработки медиаплана 
# 1. Медиаплан для обработки находится на листе Plan_Media 
# 2. В столбике B  находится слово Brand справа от него в столбике C название Бренда
# 3. В столбике B  находится слово Period справа от него в столбике C название указан период медиаплана
# 4. В столбике B  должно находиться поле Source
# 5. Каждая таблица должна заканчиваться строкой итогов 
# 6. В столбике С находятся Таргетинги - Targeting by purchase 
# 7. В столбике I находится тип размещения (CPC, CPM)

def get_hybrid_mediaplan(data_link, network, report_name):
    tmp_dict = {}
    sheet_names = pd.ExcelFile(BytesIO(data_link))
    for sheet_name in sheet_names.sheet_names:
       if 'медиаплан' in sheet_name.lower():
            df = pd.read_excel(BytesIO(data_link), sheet_name=sheet_name, header=None)
            sheet_name = normalize_headers(sheet_name)
            print(f'    {sheet_name}')
            # В столбике В находится строка итогов, по слову Итого мы определяем окончание таблицы с данными
            # но в некоторых случаях перед строкой итогов есть пустая строка
            # далее мы создадим проверку для поиска нужного нам окончания таблицы
            # сейчас пока что заполним пустые ячейки в этой таблице нулями
            df[1] = df[1].fillna('0')
            df = df.fillna('')
           
            # сохраняем название бренда
            brand = df[3].loc[get_index_row(df, 1, 'рекламодатель')] 
            # сохраняем период
            period = df[3].loc[get_index_row(df, 1, 'период')]
           # забираем индекс начала таблицы
            start_index = get_index_row(df, 1, 'тип')

            # задаем названия полей
            df.columns = df.iloc[start_index].apply(normalize_headers) # забираем название полей из файла
            # обрезаем верхнюю часть таблицы. она больше не нужна
            df = df.iloc[start_index+1:].reset_index(drop=True)
            # забираем окончание таблицы
            # создаем правило для проверки окончания таблицы 
            # если слово Итого имеет индекс строки больше, чем пустая строка, которую мы заполнили 0, то берем первый индекс ячейки с 0
            # иначе берем индекс строки с Итого
            total_index = get_index_row(df, 'тип трафика', 'итого') 
            check_index = get_index_row(df, 'тип трафика', '0')
            if total_index > check_index:
                end_index = check_index
            else:
                end_index = total_index
            # обрезаем таблицу снизу
            df = df.iloc[:end_index].reset_index(drop=True)
            # создаем базовый список полей, которые есть всегда вне зависимости от типа размещения
            standart_columns = ['тип трафика', 'формат', 'продукт', 'гео', 'единица', 
                                'цена за единицу, rub (без ндс)',
                                'стоимость, rub (без ндс)', 'показы', 
                                'охват', 'частота показа на пользователя']
            # проверяем наличие Видео размещений. Если они есть, то используем дополнительные поля из таблицы
            # создаем список уникальных форматов и преобразуем его в одну строку
            if 'vtr, %' not in list(df.columns):
                df['количество досмотров'] = 0
                df['vtr, %'] = 0.0
            if 'ssp' not in list(df.columns):
                df['ssp'] = ''
            if 'клики' not in list(df.columns):
                df['клики'] = 0
            standart_columns += ['количество досмотров', 'vtr, %', 'ssp', 'клики']
            # оставляем только нужныеинам поля
            df = df[standart_columns]
            # приводим названия полей к единому стандарту
            df = df.rename(columns={'тип трафика': 'placement', 'формат': 'ad copy format', 'продукт': 'targeting', 
                                    'гео': 'geo', 'ssp': 'site/ssp', 
                                    'единица': 'rotation type', 'цена за единицу, rub (без ндс)': 'unit price', 
                            'стоимость, rub (без ндс)': 'budget_without_nds',
                            'показы': 'impressions', 'клики': 'clicks', 'охват': 'reach',
                           'частота показа на пользователя': 'frequency', 'количество досмотров': 'views'})
    
            df['supplier'] = network
            df['source'] = network
            df['report_name'] = report_name
            df['sheet_name'] = sheet_name
            df['brand'] = brand
            df['period'] = period
            df['budget_nds'] =(df['budget_without_nds'] * 1.2).astype('float').round(2)
            # парсим Гео и Соц. дем из поля targeting
            df['soc_dem'] = df['targeting'].apply(lambda x: get_targetings(x, 'ца:','\n', flag='soc'))
           
            df['views'] = df['views'].apply(normalize_digits)
            df['vtr, %'] = df['vtr, %'].apply(normalize_digits)
            df = df[base_cols]
            tmp_dict[sheet_name] = df

    return pd.concat(tmp_dict, ignore_index=True)


# In[ ]:


# источник Mobidriven
# типы размещения Видео и Баннерная реклама
# Функция для обработки медиаплана 
# 1. Название листа, на котром находится Медиаплан должно содержать буквы МП 
# 2. В столбике А  находится слово Бренд справа от него в столбике В название Бренда
# 3. В столбике А  находится слово Период справа от него в столбике В название указан период медиаплана


def get_mobidriven_mediaplan(data_link, network, report_name):
    tmp_dict = {}
    sheet_names = pd.ExcelFile(BytesIO(data_link))
    for sheet_name in sheet_names.sheet_names:
        if 'мп' in sheet_name.lower():
            df = pd.read_excel(BytesIO(data_link), sheet_name=sheet_name, header=None)
            sheet_name = normalize_headers(sheet_name)
            print(f'    {sheet_name}')
            df[0] = df[0].fillna('0')
            df = df.fillna('')
            # сохраняем название бренда
            brand = df[1].loc[get_index_row(df, 0, 'клиент')]
            # сохраняем период
            period = df[1].loc[get_index_row(df, 0, 'период')]
            # сохраняем ЦА
            soc_dem = df[1].loc[get_index_row(df, 0, 'ца')]
            # забираем индекс начала таблицы
            start_index = get_index_row(df, 0, 'категория')
            # задаем названия полей
            df.columns = df.iloc[start_index].apply(normalize_headers) # забираем название полей из файла
            # обрезаем верхнюю часть таблицы. она больше не нужна
            df = df.iloc[start_index+2:].reset_index(drop=True)
            # обрезаем таблицу снизу
            end_index = get_index_row(df, 'категория / инструмент', '0')
            df = df.iloc[:end_index]
            df['формат'] = df.apply(get_timing, axis=1)

            # создаем базовый список полей, которые есть всегда вне зависимости от типа размещения
            standart_columns = ['категория / инструмент', 'таргетинги / сегменты', 'тип трафика', 'формат', 'гео', 'единица закупки', 
                            'цена за единицу', 'стоимость, руб, без учета ндс, 20%',
                            'показы', 'частота', 'охват', 'клики']
            # проверяем наличие Видео размещений. Если они есть, то используем дополнительные поля из таблицы
            # если Видео размещений нет, то добавляем дополнительно 2 поля с 0 (это нужно для нормализации общей таблицы
            if 'vtr %' not in list(df.columns):
                df['досмотры'] = 0
                df['vtr %'] = 0.0
        
            standart_columns += ['досмотры', 'vtr %']
            # оставляем только нужные поля
            df = df[standart_columns]
                
            # приводим названия полей к единому стандарту
            df = df.rename(columns={'категория / инструмент': 'source', 'тип трафика': 'placement', 'таргетинги / сегменты': 'targeting',
                                'формат': 'ad copy format', 'гео': 'geo', 'единица закупки': 'rotation type',
                       'цена за единицу': 'unit price', 'стоимость, руб, без учета ндс, 20%': 'budget_without_nds',
                       'показы': 'impressions', 'частота': 'frequency', 'охват': 'reach', 'клики': 'clicks', 'досмотры': 'views', 'vtr %': 'vtr, %'})
            df['supplier'] = network
            df['report_name'] = report_name
            df['sheet_name'] = sheet_name
            df['brand'] = brand
            df['period'] = period
            df['site/ssp'] = ''
            df['budget_nds'] =(df['budget_without_nds'] * 1.2).astype('float').round(2)
            # вызываем функцию для парсинга текста из поля targeting
            # значение каждого таргетинга записываем в отдельное поле датаФрейма
            df['soc_dem'] = soc_dem
            # если в этих полях встречаются пустые ячейки, то заменяем их на 0
            df['vtr, %'] = df['vtr, %'].apply(replace_blank)
            df['views'] = df['views'].apply(replace_blank)
            df = df[base_cols]
            tmp_dict[sheet_name] = df

    return pd.concat(tmp_dict, ignore_index=True)


# In[ ]:


# источник Roxot
# типы размещения Видео и Баннерная реклама
# Функция для обработки медиаплана 
# 1. Название листа, на котром находится Медиаплан должно содержать буквы BB 
# 2. В данные в таблице начинаются со столбика В
# 3. В столбике В находится название Клиент, справа от него в этой же строке в стобике С название клиента
# 4. В столбике В находится поле с названием Целевая аудитория
# 5. В столбике I  находится слово Частота и показатели частотности


def get_roxot_mediaplan(data_link, network, report_name):
    tmp_dict = {}
    sheet_names = pd.ExcelFile(BytesIO(data_link))
    for sheet_name in sheet_names.sheet_names:
        if 'bb' in sheet_name.lower():
            df = pd.read_excel(BytesIO(data_link), sheet_name=sheet_name, header=None)
            sheet_name = normalize_headers(sheet_name)
            print(f'    {sheet_name}')
            # заголовки в файле состоят из 2-х строк, поэтому нужно выполнить заполнение вниз на 1 строку
            # забираем название полей, в которых нужно сдвинуть строку вниз
            ffill_columns = [1, 2, 3, 4, 5, 6, 7]
            df[ffill_columns] = df[ffill_columns].ffill(limit=1) # заполняем вниз
            df[8] = df[8].fillna('0')
            df = df.fillna('')
            # сохраняем название бренда
            brand = df[2].loc[get_index_row(df, 1, 'клиент')]
            # т.к. мы выполнили заполнение вниз на 1 строку
            # у нас дублируются заголовки, поэтому мы берем второе вхождение забираем индекс начала таблицы
            start_index = get_index_row(df, 1, 'аудитория') + 1
            # задаем названия полей
            df.columns = df.iloc[start_index].apply(normalize_headers) # забираем название полей из файла
            # обрезаем верхнюю часть таблицы. она больше не нужна
            df = df.iloc[start_index+4:].reset_index(drop=True)
            # обрезаем таблицу снизу
            end_index = get_index_row(df, 'частота', '0')
            df = df.iloc[:end_index]
            # создаем базовый список полей, которые есть всегда вне зависимости от типа размещения
            standart_columns = ['медиаканал', 'форматы объявлений', 'инвентарь', 'период', 'таргетинг', 'частота', 
                            'охват, прогноз', 'показы', 'клики, прогноз', 'бюджет без ндс', 'avg. cpm']
            # проверяем наличие Видео размещений. Если они есть, то используем дополнительные поля из таблицы
            # если Видео размещений нет, то добавляем дополнительно 2 поля с 0 (это нужно для нормализации общей таблицы
            if 'vtr, %' not in list(df.columns):
                df['views'] = 0
                df['vtr, %'] = 0.0
            
            standart_columns += ['views', 'vtr, %']
            # оставляем только нужные поля
            df = df[standart_columns]
             # приводим названия полей к единому стандарту
            df = df.rename(columns={'медиаканал': 'source', 'форматы объявлений': 'ad copy format', 'инвентарь': 'placement', 'период': 'period',
                                    'таргетинг': 'targeting', 'частота': 'frequency', 'охват, прогноз': 'reach', 'показы': 'impressions',
                                    'клики, прогноз': 'clicks', 'avg. cpm': 'unit price', 'бюджет без ндс': 'budget_without_nds'})
            df['supplier'] = network
            df['report_name'] = report_name
            df['sheet_name'] = sheet_name
            df['brand'] = brand
            df['site/ssp'] = ''
            df['rotation type'] = 'CPM'
            df['budget_nds'] =(df['budget_without_nds'] * 1.2).astype('float').round(2)
            # парсим Гео и Соц. дем из поля targeting
            df['geo'] = df['targeting'].apply(lambda x: get_targetings(x, 'гео:','\n', flag='geo'))
            df['soc_dem'] = df['targeting'].apply(lambda x: get_targetings(x, 'соц.дем:','\n', flag='soc'))
            
            df = df[base_cols]
            tmp_dict[sheet_name] = df

    return pd.concat(tmp_dict, ignore_index=True)


# In[ ]:


# источник Segmento
# типы размещения Видео и Баннерная реклама
# Функция для обработки медиаплана 
# 1. Медиаплан для обработки находится на листе Расчет 
# 2. В данные в таблице начинаются со столбика В
# 3. В столбике В находится название РК
# 4. В столбике E находится название Рекламодатель, справа от него в этой же строке в столбике F название клиента


def get_segmento_mediaplan(data_link, network, report_name):
    sheet_name='Расчет'
    df = pd.read_excel(BytesIO(data_link), sheet_name=sheet_name, header=None)
    sheet_name = normalize_headers(sheet_name)
    # заголовки в файле состоят из 2-х строк, поэтому нужно выполнить заполнение вниз на 1 строку
    # забираем название полей, в которых нужно сдвинуть строку вниз
    ffill_columns = [1, 2, 3, 4]
    df[ffill_columns] = df[ffill_columns].ffill(limit=1) # заполняем вниз
    df = df.fillna('')
    # сохраняем название бренда
    brand = df[5].loc[get_index_row(df, 4, 'рекламодатель')]
    # т.к. мы выполнили заполнение вниз на 1 строку
    # у нас дублируются заголовки, поэтому мы берем второе вхождение забираем индекс начала таблицы
    start_index = get_index_row(df, 1, 'рк') + 1
    # задаем названия полей
    df.columns = df.iloc[start_index].apply(normalize_headers) # забираем название полей из файла
    # обрезаем верхнюю часть таблицы. она больше не нужна
    df = df.iloc[start_index+1:].reset_index(drop=True)
    # создаем базовый список полей, которые есть всегда вне зависимости от типа размещения
    standart_columns = ['рк', 'название технологии', 'продукт / формат размещения', 'период', 'соцдем', 'география', 'единица отгрузки (unit)', 
                    'бюджет (net)',  'прогноз средней частоты', 'стоимость единицы отгрузки (net)', 'охват в уникальных пользователях',
                      'показы', 'клики']
    # проверяем наличие Видео размещений. Если они есть, то используем дополнительные поля из таблицы
    # если Видео размещений нет, то добавляем дополнительно 2 поля с 0 (это нужно для нормализации общей таблицы     
    if '% просмотров' not in list(df.columns):
        df['просмотры'] = 0
        df['% просмотров'] = 0.0
        
    standart_columns += ['просмотры', '% просмотров']
    # оставляем только нужные поля
    df = df[standart_columns]
    # приводим названия полей к единому стандарту
    df = df.rename(columns={'рк': 'placement','название технологии': 'site/ssp', 'продукт / формат размещения': 'ad copy format', 'период': 'period',
        'соцдем': 'soc_dem', 'география': 'geo', 'единица отгрузки (unit)': 'rotation type', 'бюджет (net)': 'budget_without_nds',
         '% просмотров': 'vtr, %', 'прогноз средней частоты': 'frequency', 'стоимость единицы отгрузки (net)': 'unit price',
        'охват в уникальных пользователях': 'reach', 'показы': 'impressions', 'клики': 'clicks', 'просмотры': 'views'})

    df['supplier'] = network
    df['source'] = network
    df['report_name'] = report_name
    df['sheet_name'] = sheet_name
    df['brand'] = brand
    df['budget_nds'] =(df['budget_without_nds'] * 1.2).astype('float').round(2)
    # вызываем функцию, чтобы убрать из этих полей знак - иди пусто заменить на 0
    df['views'] = df['views'].apply(normalize_digits)
    df['vtr, %'] = df['vtr, %'].apply(normalize_digits)
    df['views'] = df['views'].astype('float').round(0)
    df['targeting'] = ''
    # создаем список текстоых полей, чтобы убрать из них лишние симовлы (перенос строки и тд)
    text_columns = ['period', 'soc_dem']
    df[text_columns] = df[text_columns].apply(normalize_text)
    df = df[base_cols]

    return df


# In[ ]:


# источник Weborama
# типы размещения Видео и Баннерная реклама
# Функция для обработки медиаплана 
# 1. Медиаплан для обработки находится на листе Расчет 
# 2. В данные в таблице начинаются со столбика В
# 3. В столбике В находится название РК
# 4. В столбике E находится название Рекламодатель, справа от него в этой же строке в столбике F название клиента

def get_weborama_mediaplan(data_link, network, report_name, extention):
    tmp_dict = {}
    sheet_names = pd.ExcelFile(BytesIO(data_link))
    # в этом файле присутствуют скрытые листы
    # нам нужно исключить их из парсинга
    # поэтому добавляем дополнительный блок с проверкой статуса листа
    sheets = get_sheets_list(sheet_names, extention)
    for sheet in sheets:
        sheet_name = check_excel_sheets(sheet, extention)
        if sheet_name:
            if 'banner' in sheet_name.lower() or 'video' in sheet_name.lower() \
            or 'баннер' in sheet_name.lower() or 'видео' in sheet_name.lower():
                df = pd.read_excel(BytesIO(data_link), sheet_name=sheet_name, header=None)
                
                sheet_name = normalize_headers(sheet_name)
                print(f'    {sheet_name}')
                
                # заголовки в файле состоят из 2-х строк, поэтому нужно выполнить заполнение вниз на 1 строку
                # забираем название полей, в которых нужно сдвинуть строку вниз
                ffill_columns = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
                df[ffill_columns] = df[ffill_columns].ffill(limit=1) # заполняем вниз
                
                # т.к. мы выполнили заполнение вниз на 1 строку
                # у нас дублируются заголовки, поэтому мы берем второе вхождение забираем индекс начала таблицы
                start_index = get_index_row(df, 1, 'период') + 1
                
                # забираем название полей из файла
                col_names_list = df.iloc[start_index].fillna('').apply(normalize_headers)
                pattern = 'показы'
                # получаем индекс колонки
                col_index = get_col_index(col_names_list, pattern, flag='equals')
                df[col_index] = df[col_index].fillna('0')
                
                df = df.fillna('')
                
                # сохраняем название бренда
                brand = df[1].loc[get_index_row(df, 1, 'клиент')]
                brand = brand[len('Клиент:'):].strip()
                # если целевая аудитория НЕ будет указана в самой таблице
                # то возьмем значение из описания НАД таблицей
                ca = df[1].loc[get_index_row(df, 1, 'ца')]
                ca = ca[len('ЦА:'):].strip()
                
                # т.к. одно из полей может менять название, нам нужно его к единому стандарту
                # сначала получим индекс колонки, которая нам нужна. она тоже может менять положение (может быть втоорой или третьей)
                pattern = 'сервис'
                # получаем индекс колонки
                col_index = get_col_index(col_names_list, pattern)
                # меняем название в нужнй ячейке
                df[col_index].loc[start_index] = 'source'
                # меняем назвние еще одного поля
                pattern = 'объем'
                 # получаем индекс колонки
                col_index = get_col_index(col_names_list, pattern)
                # меняем название в нужнй ячейке
                df[col_index].loc[start_index] = 'budget_without_nds'
                # меняем назвние еще одного поля
                pattern = 'частота'
                 # получаем индекс колонки
                col_index = get_col_index(col_names_list, pattern)
                # меняем название в нужнй ячейке
                df[col_index].loc[start_index] = 'frequency'
                
                # задаем названия полей
                df.columns = df.iloc[start_index].apply(normalize_headers) # забираем название полей из файла
                # обрезаем верхнюю часть таблицы. она больше не нужна
                df = df.iloc[start_index+1:].reset_index(drop=True)
                # обрезаем таблицу снизу
                end_index = get_index_row(df, 'показы', '0')
                df = df.iloc[:end_index]
                
                # создаем базовый список полей, которые есть всегда вне зависимости от типа размещения
                standart_columns = ['период', 'source', 'формат рекламы', 'гео', 'аудиторные данные перечислены через слэш (/)',
                        'стоимость за единицу (до ндс)', 'budget_without_nds', 'показы', 'охват', 'клики', 'frequency', 'модель закупки']

                # проверяем наличие Видео размещений. Если они есть, то используем дополнительные поля из таблицы
                # если Видео размещений нет, то добавляем дополнительно 2 поля с 0 (это нужно для нормализации общей таблицы     
                if 'vtr' not in list(df.columns):
                    df['views'] = 0
                    df['vtr'] = 0.0
                else:
                    df['views'] = df['vtr'].astype('float') * df['показы'].astype('float')

                if 'ца' not in list(df.columns):
                    df['ца'] = ca
    
                standart_columns += ['ца', 'views', 'vtr']
                # оставляем только нужные поля
                df = df[standart_columns]
                # приводим названия полей к единому стандарту
                df = df.rename(columns={'период': 'period', 'формат рекламы': 'ad copy format', 'гео': 'geo',
                                'аудиторные данные перечислены через слэш (/)': 'targeting', 'ца': 'soc_dem', 
                                'модель закупки': 'rotation type', 'стоимость за единицу (до ндс)': 'unit price',
                                'показы': 'impressions', 'охват': 'reach', 'клики': 'clicks', 'vtr': 'vtr, %'})

                df['supplier'] = network
                df['report_name'] = report_name
                df['sheet_name'] = sheet_name
                df['brand'] = brand
                df['site/ssp'] = ''
                df['placement'] = ''
                df['budget_nds'] =(df['budget_without_nds'] * 1.2).astype('float').round(2)
                
                df = df[base_cols]
                tmp_dict[sheet_name] = df

    return pd.concat(tmp_dict, ignore_index=True)
        


# In[ ]:


# создаем функцию для обработки данных в эксель файле
# в зависимости от источника парсинг будет отличаться
# на входе функция принимает
# -название отчета - по сути это название источника
# - ссылку для скачивания эксель файла
# - путь к файлу, чтобы его удалить после закачивания
def parse_yandex_responce(file_name, data_link, file_path, main_dict):
    
    # убираем расширение .xlsx из названия файла
    report_name = '.'.join(file_name.split('.')[:-1]) 
    report_name = report_name.lower().strip().replace('\n', ' ')
    print(report_name)
    
    # сохраняем расширение файла в отдельную переменную
    extention = file_name.split('.')[-1]
    
    if 'beeline' in report_name:
        network = 'beeline'
        main_dict[report_name] = get_beeline_mediaplan(data_link, network, report_name)

    if 'firstdata' in report_name:
        network = 'firstdata'
        main_dict[report_name] = get_firstdata_mediaplan(data_link, network, report_name)
        
    if 'hybrid' in report_name:
        network = 'hybrid'
        main_dict[report_name] = get_hybrid_mediaplan(data_link, network, report_name)

    if 'mobidriven' in report_name:
        network = 'mobidriven'
        main_dict[report_name] = get_mobidriven_mediaplan(data_link, network, report_name)
        
    if 'roxot' in report_name:
        network = 'roxot'
        main_dict[report_name] = get_roxot_mediaplan(data_link, network, report_name)
        
    if 'segmento' in report_name:
        network = 'segmento'
        main_dict[report_name] = get_segmento_mediaplan(data_link, network, report_name)

    if 'weborama' in report_name:
        network = 'weborama'
        main_dict[report_name] = get_weborama_mediaplan(data_link, network, report_name, extention)

    
    # в самом конце удаляем файл по этому источнику
    delete_yandex_disk_file(file_path)


# In[ ]:


# создаем функцию, которая забирает Excel файлы из указанной папки
# на входе она принимает след. параметры:
# main_folder - основная папка конкретного проекта 
# yandex_folders - вложенные папки (например - файлы Алексея(источник Яндекс) / файлы Стаса(источник Программатик) / файлы Полины(прочие источники)
# yandex_token - токен Яндекс (получаем заранее самостоятельно)
# flag - это ключевое слово, которое содержится в названии папки, чтобы можно было понять к кому она отностится
# именно эту папку мы и будем прасить
# так же принимаем на входе 2 словаря - Баннеры и Видео (в них сохраним все данные)
def get_data_from_ya_folder(yandex_folders, main_dict, flag='prog'):
    public_key = yandex_folders['public_key']  # из ответа Яндекс забираем public_key, чтобы использовать его для скачивания файлов

    for i in range(len(yandex_folders['_embedded']['items'])): # через цикл проходим по ответу Яндекса и забираем названия вложенных папок
        file_type = yandex_folders['_embedded']['items'][i]['type']
        if file_type=='dir':   # если находим файлы с типом dir (папка), то забираем путь к этой папке
            folder_path = yandex_folders['_embedded']['items'][i]['path']
            print(folder_path)
            if flag in folder_path.lower():
                yandex_responce = get_yandex_disk_responce(base_public_url, public_key, folder_path) # отправляем запрос, чтобы получить содержимое папки
        
                # Через цикл проходим по папке с файлами
                # Нас интересуют файлы эксель. Причем каждая экселька будет парситься по своему, т.к. они относятся к разным рекламным площадкам
                
                # Проходим через цикл по содержимому папки (отдельный флайт)
                for i in range(len(yandex_responce['_embedded']['items'])):
                    file_info = yandex_responce['_embedded']['items'][i]
                    if file_info['type']=='file':  # если документ является фалйом(не папкой или изображением), то забираем его название 
                        file_name = file_info['name'] # сохраняем название файла
                        if 'xls' in file_name: # еслит тип файла является xlsx, то уберем расширение и будем его использовать в качесвте названия отчета
                            file_path = file_info['path']
                            
                            # report_name = '.'.join(file_name.split('.')[:-1]) # убираем .xlsx из названия файла
                            # report_name = report_name.lower().strip().replace('\n', ' ')
                            # print(report_name)
                            
                            res_file_link = get_yandex_disk_responce(download_url, public_key, file_path) # получаем ссылку на скачивание отчета
                            download_response = requests.get(res_file_link['href'])

                            # return download_response, report_name
                            parse_yandex_responce(file_name, download_response.content, file_path, main_dict)
                                                  


# In[ ]:


# функция для нормализации заголовков в датаФрейме
# на входе принимает строка отдельного заголовка
# приводит в нижний регистр / обрезает пробелы / удаляет символ переноса строки / удаляем двойные пробелы
def normalize_headers(column):
    column = column.lower().strip().replace('\n', ' ')
    column = column.replace('*', '')
    column = re.sub(' +', ' ', column)

    return column


# In[ ]:


# функция для нормализации строк в датаФрейме
# на входе принимает поле со строковыми данными
# приводит в нижний регистр / обрезает пробелы / удаляет символ переноса строки
def normalize_text(column):
    return column.str.lower().str.strip().str.replace('\n', ' ')


# In[ ]:


# создаем функцию, которая заменяет - на 0, если тире присутствует в ячейке
# иначе ничего не меняет и возвращает исходное значение
def normalize_digits(column):
    column = str(column)
    if '-' in column:
        column = '0'
    return column


# In[ ]:


# если в поле с числом содержатся буквы - например 100руб.
# функция оставит только числа
# если в поле нет букв, только число, то вернется число
def get_digits(column):
    try:
        res = re.sub('\D', '', column)
    except:
        res = column
    return res


# In[ ]:


# создаем функцию для парсинга таргетингов из текста
# на вход она принимает ключевое слово - start_pattern -какой именно таргетинг мы ищем гео: / ца: / интересы:
# окончанием строки - end_pattern - передаем слово или символ, который будет считаться окончанием строки например -  \n или слово покупатели
# если таргетинг находится в конце текста, то возвращаем текст до конца
# на выходе функция текст таргетинга БЕЗ ключевого слова
def get_target_text(start_pattern, end_pattern, text):
    start_index = text.index(start_pattern)
    text = text[start_index:]
    end_index = text.index(end_pattern)
    target_text = text[len(start_pattern):end_index]

    target_text = target_text.strip().replace('\n', ' ')

    return target_text


# In[ ]:





# In[ ]:


# создаем функцию для записи названий таргетингов в поля датаФрейма
# на входе она принимает 4 параметра
# column - поле с текстом, который нужно распарсить
# start_pattern - ключевое слово начала текста, например 'гео:'
# end_pattern - символ или ключевое слово окночания текста, например '\n' или 'покупатели'
# flag - обозначение, какой таргетинг мы ищем geo или soc
# для каждого таргетинга отдельно вызываем функцию, чтобы достать нужный текст
# если таргеитнга не было в тексте, то вернутся пустые строки
def get_targetings(column, start_pattern, end_pattern, flag):
    result = ''
    text = column.lower().strip()
    if 'гео:' in text or 'ца:' in text or 'соц' in text:
        if flag == 'geo':
            result = get_target_text(start_pattern, end_pattern, text)
        if flag == 'soc':
            result = get_target_text(start_pattern, end_pattern, text)
            
    return result


# In[ ]:


# создаем функцию, которая проверяет - если ячейка пустая, то записываем 0
# иначе ничего не меняем
def replace_blank(column):
    value = str(column) 
    if value=='':
        value = '0.0'
    return value


# In[ ]:


# создаем функцию, чтобы найти индекс столбца
# на входе передаем след. аргументы 
# - названий полей
# - часть названия поля, которое будем искать
# - flag - условие назание поля содержит или должно быть равно нашему паттерну, по умолчанию 'contains'
def get_col_index(cols_list, pattern, flag='contains'):
    for num, name in enumerate(cols_list):
        if flag=='contains':
            if pattern in name:
                return num
                break
        else:
            if pattern==name:
                return num
                break


# In[ ]:


# создаем функцию, чтобы получить индекс строки с первым встречанием заданного паттерна
def get_index_row(df, col_name, flag):
    return list(df[df[col_name].str.lower().str.contains(flag, na=False)].index)[0] 


# In[ ]:


# создаем функцию, чтобы забрать хронометраж ролика, если он встречается в ячейке
def get_timing(row):
    result = str(row['формат'])
    timing = str(row['хронометраж ролика'])
    if len(timing) > 0:
        result += f' timing: {timing}'
    return result


# In[ ]:


# функция возвращает список листов
# на входе принимает 2 параметра
# - sheet_names (экель файл) - pd.ExcelFile(BytesIO(data_link))
# - extention - расширение файла 'xls' / 'xlsx'
def get_sheets_list(sheet_names, extention):
    if extention=='xls':
        sheets = sheet_names.book.sheets()       
    else:
        sheets = sheet_names.book.worksheets
        
    return sheets


# In[ ]:


# функция проверяет, если лист является открытым, то возвращаем его название
# на вход принимает 2 параметра
# - sheet - это объект, который мы получаем при переборе через цикл названий листов
# - extention - расширение файла 'xls' / 'xlsx'
def check_excel_sheets(sheet, extention):
    if extention=='xls':
        if sheet.visibility==0:
            return sheet.name
    else:
        if sheet.sheet_state=='visible':
            return sheet.title

