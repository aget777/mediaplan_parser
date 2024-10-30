#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
from datetime import date
from datetime import datetime
import calendar


# In[ ]:





# In[ ]:





# In[3]:


def create_date_table(start, end):
   df = pd.DataFrame({"date": pd.date_range(start, end)})
   df["week_day"] = df.date.dt.day_name()
   df['num_day_week'] = df['date'].dt.dayofweek
   df["day"] = df.date.dt.day
   df["month"] = df.date.dt.month
   df["week_num"] = df['date'].dt.isocalendar().week
   # df["quarter"] = df.date.dt.quarter
   df["year"] = df.date.dt.year
   df.insert(0, 'date_id', (df.year.astype(str) + df.month.astype(str).str.zfill(2) + df.day.astype(str).str.zfill(2)).astype(int))
   return df


# In[ ]:





# In[4]:


# создаем функцию, чтобы раздать флаг для начала и конца недели
# на вход она принимает 
#  - поле из датаФрейма с номером дня недели (понедельник =0, воскресенье=6)
# - номер дня недели, с которого мы хотим, чтобы начиналась наша неделя
# на выходе функция возвращает список - флаг дня начала недели и флаг дня оконяания недели
def get_start_end_week(num_day_week, start_day=0):
    end_of_week = 6
    start_flag = ''
    end_flag = ''
    if start_day != 0:
        end_of_week = start_day-1
    
    if num_day_week==start_day:
        start_flag = 0
    if num_day_week==end_of_week:
        end_flag = 1
    return [start_flag, end_flag]


# In[5]:


# создаем функцию, чтобы определить к какому месяцу относится неделя
# если начало и окончание недели в одном месяце, то возвращаем его номер
# если кол-во дней от кастомного начала недели до конца месяца меньше 3, то записываем такую неделю на след. месяц
# иначе возвращаем номер текущего месяца
def get_custom_month(row):
    custom_mon = ''
    year = row['year']
    start_date = row['start_custom_week']
    end_date = row['end_custom_week']
    # получаем номер месяца для начала и окончания недели
    start_mon = start_date.month
    end_mon = end_date.month
    if start_mon==end_mon:
        custom_mon = start_mon
    else:
        # получаем последнее число месяца для даты начала недели
        last_month_day = calendar.monthrange(year, start_mon)[1]
        # преобразуем это число в дату
        end_month_date = date(year, start_mon, last_month_day)
        start_date = start_date.date() 
        # находим разницу между окончанием месяца и началом недели
        date_diff = (end_month_date - start_date).days
        # если кол-во дней меньше 3, то относим такую неделю к след. месяцу
        if date_diff < 3:
            custom_mon = end_mon
        else:
            custom_mon = start_mon
    return custom_mon


# In[39]:


# функция для создания календаря с разбивкой по неделям
# на входе опционально принимает 2 параметра
# - год (можо передать строкой '2024' или числом 2024
# - номер дня недели, который считается началом (понедельник =0, воскресенье=6)
def get_mediaplan_calendar(cur_year='', start_day_num=0):
    # если текущий год не задан, то создаем его исходя из сегодняшней даты
    if cur_year=='':
        cur_year = datetime.now().year
    else:
        cur_year = int(cur_year)
        
    start = datetime(cur_year, 1, 1).strftime('%Y-%m-%d')
    end = datetime(cur_year, 12, 31).strftime('%Y-%m-%d')
    print(f'start: {start}, end: {end}')
    # создаем базовый календарь
    df = create_date_table(start, end)
    # вызываем функцию, чтобы раздать флаги начала и окночания недели
    df['custom_start_week'] = df['num_day_week'].apply(lambda x: get_start_end_week(x, start_day_num)[0])
    df['custom_end_week'] = df['num_day_week'].apply(lambda x: get_start_end_week(x, start_day_num)[1])
    
    # оставляем только начало и конец недели
    df = df[(df['custom_start_week']==0) | (df['custom_end_week']==1)].reset_index(drop=True)
    
    # если год начинается с конца недели, то убираем эту сттрочку
    if df.loc[0]['custom_end_week'] == 1:
        df = df.iloc[1:].reset_index(drop=True)
    if df.loc[len(df)-1]['custom_start_week'] == 0:
        df = df.iloc[:len(df)-1].reset_index(drop=True)

    # т.к. начало и окончание недели идут парами по 2 строки
    # первая строка - начало недели / вторая окончание
    # для второй строки мы присваиваем номер недели, который нахдится в предыдущей строке
    df['new_week_num'] = df['week_num'].shift(1)
    df = df.fillna(1)

    # переносим начало и окончание недели в отдельные поля
    df['start_custom_week'] = df.groupby('new_week_num')['date'].transform('min')
    df['end_custom_week'] = df.groupby('new_week_num')['date'].transform('max')
    # т.к. начало и окончание недели записаны в одной строке
    # мы можем избавиться от второй такой же дублирующей строки
    df = df.drop_duplicates(subset=['start_custom_week', 'start_custom_week']).reset_index(drop=True)
    # оставляем только нужные поля
    df = df[['date', 'year', 'start_custom_week', 'end_custom_week']]
    # определяем номер месяца, к которому можно отнести эту неделю
    df['custom_mon_num'] = df.apply(get_custom_month, axis=1)
    # переворачиваем дату, чтобы сначала шел день.месяц.год
    df['start_custom_week'] = df['start_custom_week'].apply(lambda x: x.date().strftime('%d.%m.%Y'))
    df['end_custom_week'] = df['end_custom_week'].apply(lambda x: x.date().strftime('%d.%m.%Y'))
    # # создаем поле с началом и окончанием недели в одной строке
    df['week_period'] = df['start_custom_week'].astype('str').apply(lambda x: str(x)[:5]) + \
    ' - ' + df['end_custom_week'].astype('str').apply(lambda x: str(x)[:5])
    # оставляем нужные поля
    df = df[['week_period', 'custom_mon_num']]
    # транспонируем датаФрейм
    df = df.transpose()

    return df


# In[ ]:





# In[ ]:




