#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
import pandas as pd
from pypdf import PdfReader
import config
from yandex_disk_func import *
from parse_functions import *
from google_connector import *
from date_time_func import *
import re


# In[ ]:





# In[2]:


ads_obj_list = ['Дзен', 'Моб. приложение', 'Сайт', 'Сообщество ВК', 'Mini app', 'Видео и трансляции', 'Каталог товаров: магазин на маркетплейсе',
'Каталог товаров: моб. приложение', 'Каталог товаров: сайт', 'Каталог товаров: сообщество', 'Клипы в соцсетях', 'Лид-форма', 'Личная страница',
 'Музыка', 'Опросы', 'Сообщество ОК', ' Nativeroll', 'Аудиореклама', 'Баннеры', 'Видео out-stream', 'Карусель', 'Мультиформатные размещения',
'Преролл', 'Реклама в ленте соцсетей', 'Тизер', 'Истории и клипы']


# In[50]:


format_list = ['OCPM', 'CPM', 'CPC', 'CPV']


# In[ ]:





# In[ ]:





# In[ ]:





# In[57]:


def parse_pdf_benchmarks(data_link):
    main_dict = {}
    # открываем PDF документ
    reader = PdfReader(BytesIO(data_link))
    # получаем кол-во страниц в документе
    # проходим через цикл по всем страницам
    print(len(reader.pages))
    for num in range(len(reader.pages)):
        # берем отдельную страницу 
        page = reader.pages[num]
        # забираем из нее содержание
        text = page.extract_text()
        # пробуем выполнить парсинг данных
        # - если получится, то вернем датаФрейм
        # - иначе пропускаем страницу, на которой возникает ошибка парсинга
        try:
            # забираем заголовок таблицы на странице (здесь указывается тип рекламы и категории - VK Реклама в категории авто)
            report_name_index = text.find('\n')
            report_name = text[:report_name_index].lower()
            # забираем период в отношении, которого действуют бенчмарки
            period_row_index = text.find('\n', text.find('\n')+1)
            period_row = text[report_name_index+1: period_row_index]
            first_digit_period = re.search('(\d+)', period_row).group()
            first_digit_index = period_row.find(first_digit_period)
            period = period_row[first_digit_index:]
            
        except:
            pass
        # если в заголовке страницы содержится инфо о отм, что на странице присутствует инфо о бенчмарках, то продолжаем парсинг
        if 'бенчмарки' in report_name and ('vk' in report_name or 'mytarget' in report_name or 'вконтакте' in report_name):
            df_tmp = pd.DataFrame()
            if 'vk' in report_name:
                print(report_name)
                source = 'vk_ads'
                df_tmp = get_parse_vk_benchmarks(text, report_name, period, source)
            #     # main_dict[report_name] = df_tmp
            if 'mytarget' in report_name or 'вконтакте':
                print(report_name)
                if 'mytarget' in report_name:
                    source = 'mytarget'
                else:
                    source = 'vkontakte'
                df_tmp = get_parse_mytarget_benchmarks(text, report_name, period, source)
            if not df_tmp.empty:
                main_dict[report_name] = df_tmp

    return pd.concat(main_dict, ignore_index=True)


# In[ ]:





# In[ ]:





# In[ ]:





# In[45]:


def get_parse_vk_benchmarks(text, report_name, period, source):
    # забираем из текста заголовки таблицы
    headers_pattern = 'НДСCPM без НДСCTR'
    headers_index = text.find(headers_pattern)
    len_pattern = len(headers_pattern)
    
    # оставляем данные для парсинга БЕЗ заголовков
    data_text = text[headers_index+len(headers_pattern):]
    # Разделяем текст по разделителю, формируем список из строк таблицы
    data_list = data_text.split('%')
    # если в список попали пустые элементы, то избавляемся от них
    data_list = [elem for elem in data_list if elem != '']
    # приведем каждую строку в порядок и сохраним в итоговый список
    # создаем итоговый список
    row_list = []
    # проходим через цикл по массиву необработанных строк
    for num, row in enumerate(data_list):
        try:
            row = row.replace('₽', '') # удаляем знак рубля
            # теперь нужно найти, где начинаются числа - сами бенчмарки
            # но ингода в скобочках указано html 5 баннер - это сбивает парсинг
            # добавляем еще одну проверку - если в строке встречается скобочка, то ищем цифры в тексте после скобочки
            if ')' in row:
                scip_index = row.find(')')
                first_digit = re.search('(\d+)', row[scip_index:]).group() #через регулярку ищем первую цифру в тексте
            else:
                first_digit = re.search('(\d+)', row).group()
                
            first_digit_index = row.find(first_digit) # находим индекс первой цифры в тексте
            digit_list = row[first_digit_index:].split('  ') # оставляем строку начиная с первой цифры и преобразуем ее в список
            # забираем текст ДО первой цифры 
            # - это Объект рекламы и Цель рекламы
            adds_name = row[:first_digit_index].strip()
            # теперь из этой строки нужно выделить Цель рекламы
            # у нас есть заранее готовый список с названиями Объектов рекламы - ads_obj_list
            # если название объекта рекламы содержится в строке (оно НЕ может НЕ содержаться)
            # то мы забираем весь текст ПОСЛЕ него - это и есть Цель рекламы
            for ads_obj in ads_obj_list:
                if ads_obj in adds_name:
                    # adds_name_index = adds_name.find(ads_obj)
                    adds_name_short = adds_name[len(ads_obj):].strip()
                    ads_obj_name = ads_obj
            # добавляем в список с цифрами 
            # - название категории, период, объект рекламы, цель рекламы
            if 'категории' in report_name:
                cat_index = report_name.find('категории')
                category = report_name[cat_index+len('категории'):].strip()
            digit_list.append(category)
            digit_list.append(period)
            digit_list.append(ads_obj_name)
            digit_list.append(adds_name_short)
            # print(digit_list)
            # добавляем список в итоговый список
            row_list.append(digit_list)

        except:
            pass
        df_tmp = pd.DataFrame(row_list, columns=['cpc без ндс', 'cpm без ндс', 'ctr %', 'категория', 'период', 'объект рекламы', 'цель рекламы'])
        df_tmp['source'] = source
        df_tmp['format_type'] = ''
        # print(df_tmp)
    return df_tmp


# In[54]:


def get_parse_mytarget_benchmarks(text, report_name, period, source):
    # забираем из текста заголовки таблицы
    headers_pattern = 'НДСCPM без НДСCTR'
    headers_index = text.find(headers_pattern)
    len_pattern = len(headers_pattern)
     # оставляем данные для парсинга БЕЗ заголовков
    data_text = text[headers_index+len(headers_pattern):]
    data_text = data_text.replace('100%x', '100x')
    data_text = data_text.replace('x100%', 'x100')
    # если в типах рекламы есть Аудиореклама, и она находится в самом начале таблицы, 
    # нам нужно добавить символ % - как окончание строки. в базовом варианте он отсутствует
    check_audio = 'Аудиореклама'
    if check_audio in data_text:
        if check_audio == data_text[:len(check_audio)]:
            currency_index = data_text.find('₽ ')
            data_text = data_text[:currency_index+2] + '%' + data_text[currency_index+2:]
        else:
            check_audio_index = data_text.find(check_audio)
            currency_index = data_text[check_audio_index: ].find('₽ ')
            final_index = check_audio_index + currency_index+2
            data_text = data_text[:final_index] + '%' + data_text[final_index:]
    # Разделяем текст по разделителю, формируем список из строк таблицы
    data_list = data_text.split('%')
    # если в список попали пустые элементы, то избавляемся от них
    data_list = [elem for elem in data_list if elem != '']
    # print(data_list)
    # приведем каждую строку в порядок и сохраним в итоговый список
    # создаем итоговый список
    # создаем итоговый список
    row_list = []
    # проходим через цикл по массиву необработанных строк
    for num, row in enumerate(data_list):
        try:
            row = row.replace('₽', '') # удаляем знак рубля
            # print(row)
            for type in format_list:
                if type in row:
                    scip_index = row.find(type)
                    first_digit = re.search('(\d+)', row[scip_index+len(type):]).group() #через регулярку ищем первую цифру в тексте
                    format_type = type
                    break
            first_digit_index = row.find(first_digit) # находим индекс первой цифры в тексте
            digit_list = row[first_digit_index:].split('  ') # оставляем строку начиная с первой цифры и преобразуем ее в список
            digit_list = [elem for elem in digit_list if elem != '']
            
            # забираем текст ДО первой цифры 
            # - это Объект рекламы и Цель рекламы
            adds_name = row[:first_digit_index].strip()
            if check_audio in adds_name:
                digit_list.insert(0, 0)
                digit_list.insert(2, 0)
            # print(digit_list)
            # теперь из этой строки нужно выделить Цель рекламы
            # у нас есть заранее готовый список с названиями Объектов рекламы - ads_obj_list
            # если название объекта рекламы содержится в строке (оно НЕ может НЕ содержаться)
            # то мы забираем весь текст ПОСЛЕ него - это и есть Цель рекламы
            for ads_obj in ads_obj_list:
                if ads_obj in adds_name:
                    # adds_name_index = adds_name.find(ads_obj)
                    adds_name_short = adds_name[len(ads_obj):].strip()
                    adds_name_short = adds_name[:-len(format_type)].strip()
                    ads_obj_name = ads_obj
            # добавляем в список с цифрами 
            # - название категории, период, объект рекламы, цель рекламы
            if 'категории' in report_name:
                cat_index = report_name.find('категории')
                category = report_name[cat_index+len('категории'):].strip()
            digit_list.append(category)
            digit_list.append(period)
            digit_list.append(ads_obj_name)
            digit_list.append(adds_name_short)
            digit_list.append(format_type)
            # print(digit_list)
            # добавляем список в итоговый список
            row_list.append(digit_list)
        except:
            pass

    df_tmp = pd.DataFrame(row_list, columns=['cpc без ндс', 'cpm без ндс', 'ctr %', 'категория', 'период', 
                                             'объект рекламы', 'цель рекламы', 'format_type'])
    df_tmp['source'] = source
    return df_tmp


# In[ ]:





# In[ ]:




