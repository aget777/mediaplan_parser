#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
import numpy as np
import os
import requests
from io import BytesIO
import json
import yadisk
from datetime import datetime, date, timedelta
from yandex_disk_func import *
import re


# In[ ]:


base_cols = ['supplier', 'report_name', 'sheet_name', 'brand', 'period', 'source', 'site/ssp', 'placement', 
                     'targeting', 'geo', 'soc_dem', 'ad copy format', 'rotation type', 
                    'unit quantity', 'unit price', 'frequency', 'reach', 'impressions', 'clicks', 
             'budget_without_nds', 'budget_nds', 'views', 'vtr, %']


# In[ ]:





# In[ ]:


# источник Beeline
# типы размещения Видео и Баннерная реклама
# Функция для обработки медиаплана 
# 1. Медиаплан для обработки находится на листе Plan_Media 
# 2. В столбике B  находится слово Brand справа от него В столбике C название Бренда
# 3. В столбике B  находится слово Period справа от него В столбике C название указан период медиаплана
# 4. В столбике B  должно находиться поле Source
# 5. Каждая таблица должна заканчиваться строкой итогов 

def get_beeline_mediaplan(data_link, network, report_name):
    df = pd.read_excel(BytesIO(data_link), sheet_name='Plan_Media')
    # заполняем вниз объединенные ячейки
    df['Unnamed: 1'] = df['Unnamed: 1'].ffill()
    df['Unnamed: 2'] = df['Unnamed: 2'].ffill()
    df['Unnamed: 3'] = df['Unnamed: 3'].ffill()
    df['Unnamed: 4'] = df['Unnamed: 4'].ffill()
    df = df.fillna('')
    # забираем индекс строки, где находится название Бренда
    brand_index = list(df[df['Unnamed: 1'].str.lower().str.contains('brand')].index)[0] 
    # сохраняем название бренда
    brand = df['Unnamed: 2'].loc[brand_index] 
    # забираем период медиаплана
    period = df['Unnamed: 2'].loc[brand_index+1] 
    # забираем индекс начала таблицы
    start_index = list(df[df['Unnamed: 1'].str.lower().str.contains('source')].index)[0]
    # задаем названия полей
    df.columns = df.iloc[start_index].apply(normalize_headers) # забираем название полей из файла
    # обрезаем верхнюю часть таблицы. она больше не нужна
    df = df.iloc[start_index+2:].reset_index(drop=True)
    # забираем окончание таблицы
    end_index = list(df[df['source'].str.lower().str.contains('итого')].index)[0]
    # обрезаем таблицу снизу
    df = df.iloc[:end_index].reset_index(drop=True)
    
    standart_columns = ['source', 'site/ssp', 'placement', 'targeting', 'ad copy format', 'unit', 'rotation type', 
         'unit quantity', 'unit price', 'frequency', 'reach', 'impressions', 'clicks', 'ratecard price per period (rub, net)']
    
    if 'views' in list(df.columns):
        standart_columns += ['views', 'vtr, %']
    else:
        df = df[standart_columns]
        df['views'] = 0
        df['vtr, %'] = 0.0
        standart_columns += ['views', 'vtr, %']

    df = df[standart_columns]
    df['views'] = df['views'].apply(normalize_digits)
    df['vtr, %'] = df['vtr, %'].apply(normalize_digits)
    df['views'] = df['views'].astype('int')
    df['vtr, %'] = df['vtr, %'].astype('float')
    
     # создаем список с названиями текстовых полей для их нормализации
    df = df.rename(columns={'ratecard price per period (rub, net)': 'budget_without_nds'})
    df['supplier'] = network
    df['report_name'] = report_name
    df['sheet_name'] = 'Plan_Media'
    df['brand'] = brand
    df['period'] = period
    df['geo'] = df['targeting'].apply(lambda x: get_targetings(x, 'beeline')[0])
    df['soc_dem'] = df['targeting'].apply(lambda x: get_targetings(x, 'beeline')[1])
    
    # убираем знак рубля, если он есть в стоимости
    currecny_columns = ['unit', 'unit quantity']
    df[currecny_columns] = df[currecny_columns].apply(get_digits)
    
    # # добавляем рассчитываемые показатели
    # # df['budget_without_nds'] = ((df['unit quantity'] / 1000) * df['unit price']).astype('float')
    df['budget_without_nds'] = df['budget_without_nds'].astype('float')
    df['budget_nds'] =(df['budget_without_nds'] * 1.2).astype('float').round(2)
    
    df['vtr, %'] = df['vtr, %'].apply(replace_blank)
    df['views'] = df['views'].apply(replace_blank)
    
    df = df[base_cols]
    return df


# In[ ]:


# источник FirstData
# типы размещения Видео и Баннерная реклама
# Функция для обработки медиаплана 
# 1. Медиаплан для обработки находится на листе Plan_Media 
# 2. В столбике B  находится слово Brand справа от него В столбике C название Бренда
# 3. В столбике B  находится слово Period справа от него В столбике C название указан период медиаплана
# 4. В столбике B  должно находиться поле Source
# 5. Каждая таблица должна заканчиваться строкой итогов 

def get_firstdata_mediaplan(data_link, network, report_name):
    tmp_dict = {}
    sheet_names = pd.ExcelFile(BytesIO(data_link))
    for sheet_name in sheet_names.sheet_names:
        if 'mediaplan' in sheet_name:
            df = pd.read_excel(BytesIO(data_link), sheet_name=sheet_name)
            # заполняем вниз название истоника
            sheet_name = normalize_headers(sheet_name)
            print(sheet_name)
            df['Unnamed: 1'] = df['Unnamed: 1'].ffill()
            # заполняем вниз таргетинги
            df['Unnamed: 2'] = df['Unnamed: 2'].ffill()
            # заполяем вниз rotation type - здесь объединенная ячейка, и в этом поле нет названия
            # чтобы оно появилось, заранее протягиваем вниз это название
            df['Unnamed: 8'] = df['Unnamed: 8'].fillna('rotation type')
            # заполняем вниз объединенные ячейки
            df = df.fillna('')
            # забираем индекс строки, где находится название Бренда
            brand_index = list(df[df['Unnamed: 2'].str.lower().str.contains('brand')].index)[0] 
            # сохраняем название бренда
            brand = df['Unnamed: 3'].loc[brand_index] 
            # забираем период медиаплана
            period = df['Unnamed: 3'].loc[brand_index+2] 
            # забираем индекс начала таблицы
            start_index = list(df[df['Unnamed: 1'].str.lower().str.contains('category')].index)[0]
            # задаем названия полей
            df.columns = df.iloc[start_index].apply(normalize_headers) # забираем название полей из файла
            # обрезаем верхнюю часть таблицы. она больше не нужна
            df = df.iloc[start_index+2:].reset_index(drop=True)
            # забираем окончание таблицы
            end_index =list(df[df['category'].str.lower().str.contains('total')].index)[0]
            # обрезаем таблицу снизу
            df = df.iloc[:end_index].reset_index(drop=True)
            # создаем базовый список полей, которые есть всегда вне зависимости от типа размещения
            standart_columns = ['category', 'targeting by purchase', 'format', 'quantity of units', 'period', 
                            'price list cost (cost per unit) net ', 'rotation type',
                            'total price list cost net', 'reach forecast (uu)', 'frequency total till',
                            'impressions', 'clicks']
            # проверяем наличие Видео размещений. Если они есть, то используем дополнительные поля из таблицы
            # создаем список уникальных форматов и преобразуем его в одну строку
            
            if 'vtr,%' in list(df.columns):
               standart_columns += ['number of views', 'vtr,%']
            else:
                df = df[standart_columns]
                df['number of views'] = 0
                df['vtr,%'] = 0.0
                standart_columns += ['number of views', 'vtr,%']
                
            df = df[standart_columns]
            df = df.rename(columns={'category': 'source', 'targeting by purchase': 'targeting', 'format': 'ad copy format', 
                                    'quantity of units': 'unit quantity', 'price list cost (cost per unit) net ': 'unit price', 
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
    
            # забираем окончание таблицы
            end_index = list(df[df['unit quantity']==''].index)[0]
            # обрезаем таблицу снизу
            df = df.iloc[:end_index].reset_index(drop=True)
            # передаем новое название формата в нужное нам поле
            df['ad copy format'] = df['merge_type_cells']
            df = df.drop('merge_type_cells', axis=1)
        
            df['supplier'] = network
            df['report_name'] = report_name
            df['sheet_name'] = sheet_name
            df['brand'] = brand
            df['period'] = period
            df['site/ssp'] = ''
            df['placement'] = ''
            df['budget_nds'] =(df['budget_without_nds'] * 1.2).astype('float').round(2)
            # вызываем функцию для парсинга текста из поля targeting
            # значение каждого таргетинга записываем в отдельное поле датаФрейма
            df['geo'] = df['targeting'].apply(lambda x: get_targetings(x, 'firstdata')[0])
            df['soc_dem'] = df['targeting'].apply(lambda x: get_targetings(x, 'firstdata')[1])
            
            df['vtr, %'] = df['vtr, %'].apply(replace_blank)
            df['views'] = df['views'].apply(replace_blank)
            
            df = df[base_cols]
            tmp_dict[sheet_name] = df

    return pd.concat(tmp_dict, ignore_index=True)


# In[ ]:





# In[ ]:


# создаем функцию для обработки данных в эксель файле
# в зависимости от источника парсинг будет отличаться
# на входе функция принимает
# -название отчета - по сути это название источника
# - ссылку для скачивания эксель файла
# - путь к файлу, чтобы его удалить после закачивания
def parse_yandex_responce(report_name, data_link, main_folder, file_path, yandex_token, main_dict):
    
    if 'beeline' in report_name:
        network = 'beeline'
        main_dict[report_name] = get_beeline_mediaplan(data_link, network, report_name)

    if 'firstdata' in report_name:
        network = 'firstdata'
        main_dict[report_name] = get_firstdata_mediaplan(data_link, network, report_name)

    # в самом конце удаляем файл по этому источнику
    delete_yandex_disk_file(main_folder, file_path, yandex_token)


# In[1]:


# создаем функцию, которая забирает Excel файлы из указанной папки
# на входе она принимает след. параметры:
# main_folder - основная папка конкретного проекта 
# yandex_folders - вложенные папки (например - файлы Алексея(источник Яндекс) / файлы Стаса(источник Программатик) / файлы Полины(прочие источники)
# yandex_token - токен Яндекс (получаем заранее самостоятельно)
# flag - это ключевое слово, которое содержится в названии папки, чтобы можно было понять к кому она отностится
# именно эту папку мы и будем прасить
# так же принимаем на входе 2 словаря - Баннеры и Видео (в них сохраним все данные)
def get_data_from_ya_folder(main_folder, yandex_folders, yandex_token, main_dict, flag='prog'):
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
                            
                            report_name = '.'.join(file_name.split('.')[:-1]) # убираем .xlsx из названия файла
                            report_name = report_name.lower().strip().replace('\n', ' ')
                            print(report_name)
                            
                            res_file_link = get_yandex_disk_responce(download_url, public_key, file_path) # получаем ссылку на скачивание отчета
                            download_response = requests.get(res_file_link['href'])

                            # return download_response, report_name
                            parse_yandex_responce(report_name, download_response.content, main_folder, file_path, yandex_token, main_dict)
                                                  


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


# In[2]:


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


# создаем функцию для записи названий таргетингов в поля датаФрейма
# на входе она принимает 2 параметра
# column - поле с текстом, который нужно распарсить
# source - название источника (какому поставщику принадлежит таблица - таким образом мы понимаем, какие правила парсинга применяются
# для каждого таргетинга отдельно вызываем функцию, чтобы достать нужный текст
# на выходе возвращаем список с текстом для каждого таргетинга
# если таргеитнга не было в тексте, то вернутся пустые строки
def get_targetings(column, source):
    geo = ''
    soc_dem = ''
    text = column.lower().strip()
    if 'гео:' in text or 'ца:' in text:
        if source == 'firstdata':
            geo = get_target_text('гео:', '\n', text)
            soc_dem = get_target_text('ца:', 'покупатели', text)
        if source == 'beeline':
            geo = get_target_text('гео:', '\n', text)
            soc_dem = get_target_text('ца:', '\n', text)

    return [geo, soc_dem]


# In[ ]:


def replace_blank(column):
    value = str(column) 
    if value=='':
        value = '0.0'
    return value

