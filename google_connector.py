#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import gspread
from oauth2client.service_account import ServiceAccountCredentials

import config


# In[ ]:


# функция для того, чтобы создать подключение к Гугл докс
def create_connection(service_file):
    client = None
    scope = [
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/drive.file",
    ]
    try:
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            service_file, scope
        )
        client = gspread.authorize(credentials)
        print("Connection established successfully...")
    except Exception as e:
        print(e)
    return client


# In[ ]:


# функция для загрузки данных в гугл таблицу
def export_dataframe_to_google_sheet(worksheet, df):
    try:
        worksheet.update(
            [df.columns.values.tolist()] + df.values.tolist(),
            
        )
        print("DataFrame exported successfully...")
    except Exception as e:
        print(e)


# In[ ]:




