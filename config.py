#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import os
file_path = r'C:\Users\o.bogomolov\Desktop\Jupyter_notebook\35_mediaplan_parser'

yandex_file = 'yandex_token.txt'
yandex_token =  open(os.path.join(file_path, yandex_file), encoding='utf-8').read()

# указываем путь к основной папке, в которой храняться папки с флайтами
gmail = 'ads@ichance.ru'
main_folder = '/34_mediaplan_parse_files'
public_key = 'https://disk.yandex.ru/d/Q3E26dG_YeS6PA' # обычная ссылка на доступ к папке одного данного ФЛАЙТА из личного кабинета
# токен гугл
credentials_file = 'mediaplan-parser-aaac8df334b7.json'
service = os.path.join(file_path, credentials_file)

