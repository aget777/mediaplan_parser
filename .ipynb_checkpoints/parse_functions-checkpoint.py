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


# In[1]:


# создаем функцию, которая забирает Excel файлы из указанной папки
# на входе она принимает след. параметры:
# main_folder - основная папка конкретного проекта 
# yandex_folders - вложенные папки (например - файлы Алексея(источник Яндекс) / файлы Стаса(источник Программатик) / файлы Полины(прочие источники)
# yandex_token - токен Яндекс (получаем заранее самостоятельно)
# flag - это ключевое слово, которое содержится в названии папки, чтобы можно было понять к кому она отностится
# именно эту папку мы и будем прасить
def get_data_from_ya_folder(main_folder, yandex_folders, yandex_token, flag='prog'):
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
                            print(report_name)
                            
                            res_file_link = get_yandex_disk_responce(download_url, public_key, file_path) # получаем ссылку на скачивание отчета
                            download_response = requests.get(res_file_link['href'])

