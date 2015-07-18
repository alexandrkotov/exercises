#!/usr/bin/python3
""" Домашняя работа Александра Котова.
    Реализовано на регулярных выражениях, скрипт простой, без функций и классов.
   
   Яндекс.Погода

Есть публичный url со списком городов:
http://weather.yandex.ru/static/cities.xml

Для этих городов можно получить данные о погоде, подставив id города в шаблон:
http://export.yandex.ru/weather-ng/forecasts/<id города>.xml

Необходимо написать скрипт, который:
1. Создает файл базы данных SQLite со структурой данных (если файла 
   базы данных не существует)
2. Скачивает и парсит XML со списком городов
3. Выводит список стран из файла и предлагает пользователю выбрать страну
4. Скачивает XML файлы погоды в городах выбранной страны
5. Парсит последовательно каждый из файлов и добавляет данные о погоде в базу
   данных. Если данные для данного города и данного дня есть в базе - обновить
   температуру в существующей записи.

При повторном запуске скрипта:
- используется уже скачанный файл с городами
- используется созданная база данных, новые данные добавляются и обновляются

"""

import os
import re
import sqlite3
import urllib.request
#from xml.etree import ElementTree as ET
#from collections import OrderedDict, namedtuple
import glob

# 1. ===========================================================================
db_filename = 'weather.db'

if not os.path.exists(db_filename):
    with sqlite3.connect(db_filename) as conn:
        conn.execute("""
            create table weather (
                id_town     integer,
                town        varchar(255),
                date        date,
                temp_day    integer,
                temp_night  integer
            );
            """) 
    conn.close()
    
# 2. ===========================================================================          
country_list = []
town_list_url = 'http://weather.yandex.ru/static/cities.xml'
dest_dir = os.path.join(r'weather') 
file_name = os.path.join(dest_dir,'cities.xml')

if not os.path.exists(dest_dir): 
    os.makedirs(dest_dir) 

names = glob.glob(os.path.join(dest_dir,'*.xml'))
for f in names:
    if f not in file_name:
        os.remove(f)

urllib.request.urlretrieve(town_list_url, file_name)

f = open(file_name, 'r', encoding='utf8')
s = f.read()
reg_ex_country = re.compile(r'country name="([А-Яа-я ]*)"?')
country_list = reg_ex_country.findall(s)

# 3. ===========================================================================
print(country_list)
user_input = None
while user_input not in country_list:
    user_input = input('Выберите страну (вводить надо полное название): ')
print('Выбрана страна {}'.format(user_input))

# 4. ===========================================================================
city_list = []
# Два шаблона, сначала вырезается страна, потом города в вырезанной стране.
reg_ex_one_country = re.compile(r''.join(
    ('<country name="',user_input, '">\s*(.*?)</country>')),re.DOTALL)

reg_ex_city = re.compile(r'<city id="(\d*?)".*?>(.*?)</city>')

city_list = reg_ex_city.findall(reg_ex_one_country.findall(s)[0]) 
print('Города: {}'.format(list(map((lambda x: x[1]), city_list))))

city_list_err_download = []
city_count = 0

for city in city_list:
    file_name = ''.join((dest_dir, '\\', city[0], '.', city[1], '.xml'))
    city_id_url = (
        'http://export.yandex.ru/weather-ng/forecasts/' + city[0] + '.xml')
    try:
        urllib.request.urlretrieve(city_id_url, file_name)
        city_count += 1
    except:
        print('Данные по {} недоступны'.format(city[1]))
        city_list_err_download.append(city[1])
        continue
print('Получены данные по {} городам'.format(city_count))
print('Ошибки загрузки по {} городам: {}'.format(
                          len(city_list_err_download), city_list_err_download))

# 5. ===========================================================================
temperature = []
new_record = 0
updated_record = 0
reg_ex_temperature = re.compile(r"""

                        <day.date=(["\d-]*)>.*?    # дата
                        (day_short">)?\s           # средняя дневная
                        <temperature>([\d-]*?)<.*? #
                        (night_short">)?\s         # средняя ночная
                        <temperature>([\d-]*?)<.*? #
                        
                                 """, re.DOTALL | re.VERBOSE)
for current_city in city_list:
    try:
        file_name = ''.join(
            (dest_dir, '\\', current_city[0], '.', current_city[1], '.xml'))
        fc = open(file_name, 'r', encoding='utf8')
        # Возвращает список кортежей.
        # В кортеже [0] - дата, [2] - средняя дневная, [4] - средняя ночная.        
        temperature = reg_ex_temperature.findall(fc.read())
        # Данные из кортежей списка записываются в БД.
        with sqlite3.connect(db_filename) as conn:
            c = conn.cursor()
            for i in range(len(temperature)):
                # Проверка существующих записей по городу и дате.                
                sql_str = """
                           select * from weather where town={} and date={}
                           """.format(
                            ''.join(('"', current_city[1], '"')),
                            temperature[i][0])      
                check_exist_town_date=list(c.execute(sql_str))

                if check_exist_town_date:
                    updated_record += 1
                    sql_str = """
                        update weather set temp_day={}, temp_night={}
                            where town={} and date={} 
                                 """.format(
                                temperature[i][2],
                                temperature[i][4],
                                ''.join(('"', current_city[1], '"')),
                                temperature[i][0])
                else:    
                    new_record += 1
                    sql_str = """
                        insert into weather 
                            (id_town, town, date, temp_day, temp_night) 
                                 VALUES ({},{},{},{},{})""".format(
                                current_city[0],
                                ''.join(('"', current_city[1], '"')),
                                temperature[i][0],
                                temperature[i][2],
                                temperature[i][4])                
                c.execute(sql_str)                
    except:
        continue 

print('Обновлено записей в базе данных: {:6}'.format(updated_record))
print('Новых записей в базе данных: {:10}'.format(new_record))
c = sqlite3.connect(db_filename).cursor()
all_db_rec = list(c.execute("select Count(*) from [main].[weather]"))
print('Всего записей в базе данных: {:10}'.format(all_db_rec[0][0]))
