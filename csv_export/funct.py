import os
import cv2
import MySQLdb                  # импортируем модуль для работы с БД MySql
import MetaTrader5 as mt5       # импортируем модуль для подключения к MetaTrader5            # импортируем модуль pandas для вывода полученных данных в табличной форме
import time, datetime
import pytz                     # импортируем модуль pytz для работы с таймзоной
import numpy as np
import pandas as pd
import apimoex
import datetime
import requests
import time

def get_moex_data(start_date, end_date, ticker, market):
    """Загружает исторические данные с MOEX ISS API с использованием apimoex."""
    try:
        # 1. Формируем параметры запроса
        start = start_date
        end = end_date
        with requests.Session() as session:
            # 2. Получаем исторические данные (apimoex сама управляет сессией)
            if market == "stocks":
                data = apimoex.get_market_candles(session=session, security=ticker, interval=24, start=start,
                                              end=end, columns=None)
            elif market == "indexes":
                data = apimoex.get_market_candles(session=session, security=ticker, interval=24, start=start,
                                              end=end, columns=None, market="index")
            elif market == "futures":
                data = apimoex.get_market_candles(session=session, security=ticker, interval=24, start=start,
                                              end=end, columns=None, market = "forts", engine = "futures")
        # 3. Преобразуем данные в DataFrame
        df = pd.DataFrame(data)
        df = df.rename(columns={'begin': 'time'})
        df["time"] = pd.to_datetime(df["time"], format='%Y-%m-%d %H:%M:%S')
        df["time"] = df["time"].dt.strftime('%Y-%m-%d')  # Используем .dt.strftime
        return df.iloc[:, :-1]

    except Exception as e:

        print(f"Ошибка при загрузке данных: {e}")
        return None

today = datetime.date.today().strftime("%Y-%m-%d")
print(today)

tickers = ['SRH2_2012', 'SRM2_2012', 'SRU2_2012', 'SRZ2_2012', 'SRH3_2013', 'SRM3_2013',
           'SRU3_2013', 'SRZ3_2013', 'SRH4_2014', 'SRM4_2014', 'SRU4_2014', 'SRZ4_2014',
           'SRH5_2015', 'SRM5_2015', 'SRU5_2015', 'SRZ5_2015', 'SRH6_2016', 'SRM6_2016',
           'SRU6_2016', 'SRZ6_2016', 'SRH7_2017', 'SRM7_2017', 'SRU7_2017', 'SRZ7_2017',
           'SRH8_2018', 'SRM8_2018', 'SRU8_2018', 'SRZ8_2018', 'SRH9', 'SRM9', 'SRU9', 'SRZ9',
           'SRH0', 'SRM0', 'SRU0', 'SRZ0', 'SRH1', 'SRM1', 'SRU1', 'SRZ1', 'SRH2', 'SRM2', 'SRU2', 'SRZ2', 'SRH3', 'SRM3', 'SRU3',
           'SRZ3', 'SRH4', 'SRM4', 'SRU4', 'SRZ4', 'SRH5', 'SRM5', 'SRU5']

#df = get_moex_data(start_date = None, end_date = today, market = "futures", ticker = "SRM5")
#print(df.to_markdown(index=True))

#Добавить обработку выходных в функцию по нахождению актуального фьючерса

#tickers = ['BRF2_2012', 'BRG2_2012', 'BRH2_2012', 'BRJ2_2012', 'BRK2_2012', 'BRM2_2012',
#           'BRN2_2012', 'BRQ2_2012', 'BRU2_2012', 'BRV2_2012', 'BRX2_2012', 'BRZ2_2012',
#           'BRF3_2013', 'BRG3_2013', 'BRH3_2013', 'BRJ3_2013', 'BRK3_2013', 'BRM3_2013',
#           'BRN3_2013', 'BRQ3_2013', 'BRU3_2013', 'BRV3_2013', 'BRX3_2013', 'BRZ3_2013',
#           'BRF4_2014', 'BRG4_2014', 'BRH4_2014', 'BRJ4_2014', 'BRK4_2014', 'BRM4_2014',
#           'BRN4_2014', 'BRQ4_2014', 'BRU4_2014', 'BRV4_2014', 'BRX4_2014', 'BRZ4_2014',
#           'BRF5_2015', 'BRG5_2015', 'BRH5_2015', 'BRJ5_2015', 'BRK5_2015', 'BRM5_2015',
#           'BRN5_2015', 'BRQ5_2015', 'BRU5_2015', 'BRV5_2015', 'BRX5_2015', 'BRZ5_2015',
#           'BRF6_2016', 'BRG6_2016', 'BRH6_2016', 'BRJ6_2016', 'BRK6_2016', 'BRM6_2016',
#           'BRN6_2016', 'BRQ6_2016', 'BRU6_2016', 'BRV6_2016', 'BRX6_2016', 'BRZ6_2016',
#           'BRF7_2017', 'BRG7_2017', 'BRH7_2017', 'BRJ7_2017', 'BRK7_2017', 'BRM7_2017',
#           'BRN7_2017', 'BRQ7_2017', 'BRU7_2017', 'BRV7_2017', 'BRX7_2017', 'BRZ7_2017',
#           'BRG8_2018', 'BRH8_2018', 'BRJ8_2018', 'BRK8_2018', 'BRM8_2018', #BRF8_2018
#           'BRN8_2018', 'BRQ8_2018', 'BRU8_2018', 'BRV8_2018', 'BRX8_2018', 'BRZ8_2018',
#           'BRF9', 'BRG9', 'BRH9', 'BRJ9', 'BRK9', 'BRM9', 'BRN9', 'BRQ9', 'BRU9',
#           'BRV9', 'BRX9', 'BRZ9', 'BRF0', 'BRG0', 'BRH0', 'BRJ0', 'BRK0', 'BRM0', 'BRN0', 'BRQ0',
#           'BRU0', 'BRV0', 'BRX0', 'BRZ0', 'BRF1', 'BRG1', 'BRH1', 'BRJ1', 'BRK1', 'BRM1', 'BRN1',
#           'BRQ1', 'BRU1', 'BRV1', 'BRX1', 'BRZ1', 'BRF2', 'BRG2', 'BRH2', 'BRJ2', 'BRK2', 'BRM2',
#           'BRN2', 'BRQ2', 'BRU2', 'BRV2', 'BRX2', 'BRZ2', 'BRF3', 'BRG3', 'BRH3', 'BRJ3', 'BRK3',
#           'BRM3', 'BRN3', 'BRQ3', 'BRU3', 'BRV3', 'BRX3', 'BRZ3', 'BRF4', 'BRG4', 'BRH4', 'BRJ4',
#           'BRK4', 'BRM4', 'BRN4', 'BRQ4', 'BRU4', 'BRV4', 'BRX4', 'BRZ4', 'BRF5', 'BRG5', 'BRH5',
#           'BRJ5', 'BRK5', 'BRM5', 'BRN5', 'BRQ5', 'BRU5', 'BRV5', 'BRX5', 'BRZ5']

#df = get_moex_data(start_date=None, end_date=today, market="futures", ticker='BRF2_2012')
#print(df.to_markdown(index=True))

#for ticker in tickers:
#    df = get_moex_data(start_date=None, end_date=today, market="futures", ticker=ticker)
#    # df = pd.concat((df1[:62], df), ignore_index=True)
#    print(df.to_markdown(index=True))


#df1 = get_moex_data(start_date = None, end_date = today, market="futures", ticker = "SRH2_2012")
#df2 = get_moex_data(start_date = None, end_date = today, market="futures", ticker = "SRM2_2012")

#df = get_moex_data(start_date = None, end_date = today, market = "futures", ticker = "USDRUBF")
##df = pd.concat((df1[:62], df), ignore_index=True)
#print(df.to_markdown(index=True))

def find_active_future(start_date, tickers, end_date):
    """
    Находит первый фьючерс из списка, в котором есть указанная дата.

    Args:
        tickers (list): Список тикеров фьючерсов.
        end_date (str): Дата, которую нужно найти (в формате 'YYYY-MM-DD').

    Returns:
        str: Тикер первого фьючерса, содержащего указанную дату, или None, если ни один фьючерс не содержит дату.
    """
    for ticker in tickers:
        try:
            rates_frame = get_moex_data(start_date, end_date, ticker,
                                             "futures")  # Изменил START_DATE на константу
            # Преобразуем столбец 'time' в datetime, если он еще не в этом формате
            rates_frame['time'] = pd.to_datetime(rates_frame['time'])

            # Преобразуем end_date в datetime для сравнения
            end_date_dt = pd.to_datetime(end_date)

            desired_time = datetime.time(10, 0, 0)
            current_time = datetime.datetime.now().time()

            end_date_d = end_date_dt

            if current_time < desired_time:
                end_date_d = end_date_dt - datetime.timedelta(seconds=86400)

            if datetime.datetime.now().weekday() == 5:
                end_date_d = end_date_dt - datetime.timedelta(seconds=86400)

            if datetime.datetime.now().weekday() == 6:
                end_date_d = end_date_dt - datetime.timedelta(seconds=(86400 * 2))
            # Проверяем наличие даты в DataFrame
            if end_date_d in rates_frame[
                'time'].values:  # Проверяем наличие datetime объекта в Series
                return ticker  # Возвращаем тикер, если дата найдена

        except Exception as e:
            continue  # Переходим к следующему тикеру в случае ошибки

    print(f"Дата {end_date} не найдена ни в одном из тикеров")
    return None  # Возвращаем None, если дата не найдена ни в одном тикере


active_future = find_active_future(None, tickers, today)
print(active_future, "- Должен: SRM5")
"""
def concat_futures(df1, df2):
    last_time1 = df1['time'].iloc[-1]

    index2 = (df2['time'] == last_time1).idxmax()

    df = pd.concat((df1[:-1], df2[index2:]), ignore_index=True)
    return df

for i in range(len(tickers)-1):
    df1 = get_moex_data(start_date=None, end_date=today, market="futures", ticker=tickers[i])
    df2 = get_moex_data(start_date=None, end_date=today, market="futures", ticker=tickers[i+1])
    print(tickers[i])
    if i == 0:
        df = concat_futures(df1, df2)
    elif i != 0 and df['time'].iloc[-1] != today:
        df = concat_futures(df, df2)

#print(df.to_markdown(index=True))

#print(df.to_markdown(index=True))
filename = ('https://raw.githubusercontent.com/Denis91530/AI_2/master/csv_export/SBER_new_D1.csv')

df1 = pd.read_csv(filename, sep = ",")

df1['Date'] = pd.to_datetime(df1['Date'], format='%Y-%m-%d')

df1['Date'] = pd.to_datetime(df1['Date'], format='%Y-%m-%d').dt.strftime('%Y-%m-%d')
df['time'] = pd.to_datetime(df['time'], format='%Y-%m-%d').dt.strftime('%Y-%m-%d')

last_time3 = df['time'].iloc[0]
last_time4 = df1['Date'].iloc[-1]


index3 = (df1['Date'] == last_time3).idxmax()
index4 = (df['time'] == last_time4).idxmax()

df4 = df1.loc[index3:]
df = df.loc[:index4]
df4.reset_index(drop=True, inplace=True)
df.reset_index(drop=True, inplace=True)


df4['Date'] = pd.to_datetime(df4['Date'], format='%Y-%m-%d').dt.strftime('%Y-%m-%d')
df['time'] = pd.to_datetime(df['time'], format='%Y-%m-%d').dt.strftime('%Y-%m-%d')

for n in ['2022-01-07', '2022-02-23', '2019-03-08', '2014-06-13', '2017-05-01']:
    index1 = (df['time'] == n).idxmax()
    df = df.drop(index1)

for h in df['time']:
    if pd.to_datetime('2022-02-25', format='%Y-%m-%d') < pd.to_datetime(h, format='%Y-%m-%d') < pd.to_datetime('2022-03-24', format='%Y-%m-%d'):
        index1 = (df['time'] == h).idxmax()
        df = df.drop(index1)

def insert_missing_dates(df, missing_dates, data_to_insert):

    # Убедимся, что количество дат соответствует количеству данных
    if len(missing_dates) != len(data_to_insert):
        raise ValueError("Количество дат и данных для вставки не совпадают.")

    # 1. Преобразуем существующую колонку time в datetime
    df['time'] = pd.to_datetime(df['time'])

    # 2. Создаем новые строки в виде DataFrame
    new_rows = []
    for i in range(len(missing_dates)):
        date_str = missing_dates[i]
        data = data_to_insert[i]
        # Заполняем все колонки
        new_row = pd.DataFrame([data])  # Создаем DataFrame из словаря
        new_row['time'] = pd.to_datetime([date_str])  # Преобразуем дату в datetime
        new_rows.append(new_row)

    # 3. Объединяем новые строки с существующим DataFrame
    df = pd.concat([df, *new_rows], ignore_index=True)

    # 4. Сортируем DataFrame по дате
    df = df.sort_values(by='time').reset_index(drop=True)

    # 5. Возвращаем исходный формат time
    df['time'] = df['time'].dt.strftime('%Y-%m-%d')

    return df


missing_dates = ['2013-03-14', '2013-09-26', '2013-10-09', '2013-11-08', '2013-11-20']

# Вы знаете, какие данные должны быть вставлены для этих дат (замените на реальные значения)
data_to_insert = [
    {'open':109.07, 'close':108.97, 'high': 109.18, 'low': 107.55, 'value': 0, 'volume': 89176, 'time': '2013-03-14'},
    {'open': 109.3, 'close': 108.7, 'high': 109.64, 'low': 107.92, 'value': 0, 'volume': 67945, 'time': '2013-09-26'},
    {'open': 110.24, 'close': 108.61, 'high': 110.54, 'low': 108.15, 'value': 0, 'volume': 57387, 'time': '2013-10-09'},
    {'open': 104.06, 'close': 103.58, 'high': 104.09, 'low': 102.99, 'value': 0, 'volume': 52135, 'time': '2013-11-08'},
    {'open': 107.79, 'close': 106.68, 'high': 108.35, 'low': 106.52, 'value': 0, 'volume': 39746, 'time': '2013-11-20'}
]


# Применяем функцию для вставки строк
df = insert_missing_dates(df, missing_dates, data_to_insert)


df.reset_index(drop=True, inplace=True)
for i in range(1, len(df)):
    if df["time"].iloc[i-1] == df4["Date"].iloc[i]:
        print(i)

#for n in ['2013-12-14', '2014-06-13', '2019-03-08', '2022-01-07', '2017-05-01', '2022-02-23', '2022-02-28']:
#    index1 = (df['time'] == n).idxmax()
#    df = df.drop(index1)

print(df4[479-5:479+5].to_markdown(index=True))
print(df[479-5:479+5].to_markdown(index=True))

print(len(df), len(df4))

for i in df4['Date']:
    if pd.to_datetime(i) > pd.to_datetime('2025-02-28'):
        if (pd.to_datetime(i).weekday() == 5 and ((pd.to_datetime(i) - datetime.timedelta(seconds = 86400)).weekday() == 4)
            and ((pd.to_datetime(i) + datetime.timedelta(seconds = (86400*2))).weekday() == 0)):

            index0 = (df4['Date'] == i).idxmax()
            df4 = df4.drop(index0)

        elif (pd.to_datetime(i).weekday() == 6 and ((pd.to_datetime(i) - datetime.timedelta(seconds = (86400*2))).weekday() == 4)
              and ((pd.to_datetime(i) + datetime.timedelta(seconds = 86400)).weekday() == 0)):

            index0 = (df4['Date'] == i).idxmax()
            df4 = df4.drop(index0)

def check_index_is_consecutive(df):
  return df.index.equals(pd.RangeIndex(start=0, stop=len(df)))


df.reset_index(drop=True, inplace=True)
df4.reset_index(drop=True, inplace=True)
print(f"Индексы последовательны: {check_index_is_consecutive(df)}")
print(f"Индексы последовательны: {check_index_is_consecutive(df4)}")

print(len(df), len(df4))

for i in range(1, len(df)):
    if df["time"].iloc[i] != df4["Date"].iloc[i]:
        print(i)
print("ОПА11111111111111111111111")
#print(df4.to_markdown(index=True))

#for i in range(len(df)):
#    if df["time"].iloc[i-1] == df4["Date"].iloc[i]:
#        print(i)

#aa = []
#for i in [2, 3, 4, 5, 6, 7, 8]:
#    for t in ["H", "M", "U", "Z"]:
#        aa.append(f"SR{t}{i}_201{i}")
#aa = []
#for i in [2, 3, 4, 5, 6, 7, 8]:
#    for t in ["F", "G", "H", "J", "K", "M", "N", "Q", "U", "V", "X", "Z"]:
#        aa.append(f"SR{t}{i}_201{i}")
#print(aa)

#print(aa)
#print(df4[305:315].to_markdown(index=True))
#print(df[305:315].to_markdown(index=True))

def insert_missing_dates(df, missing_dates, data_to_insert):

    # Убедимся, что количество дат соответствует количеству данных
    if len(missing_dates) != len(data_to_insert):
        raise ValueError("Количество дат и данных для вставки не совпадают.")

    # 1. Преобразуем существующую колонку time в datetime
    df['time'] = pd.to_datetime(df['time'])

    # 2. Создаем новые строки в виде DataFrame
    new_rows = []
    for i in range(len(missing_dates)):
        date_str = missing_dates[i]
        data = data_to_insert[i]
        # Заполняем все колонки
        new_row = pd.DataFrame([data])  # Создаем DataFrame из словаря
        new_row['time'] = pd.to_datetime([date_str])  # Преобразуем дату в datetime
        new_rows.append(new_row)

    # 3. Объединяем новые строки с существующим DataFrame
    df = pd.concat([df, *new_rows], ignore_index=True)

    # 4. Сортируем DataFrame по дате
    df = df.sort_values(by='time').reset_index(drop=True)

    # 5. Возвращаем исходный формат time
    df['time'] = df['time'].dt.strftime('%Y-%m-%d')

    return df


# Пример использования:
# Предположим, что у вас есть DataFrame df с колонками 'open', 'close', 'high', 'low', 'value', 'volume', 'time'
# Вы определили, что у вас пропущены две даты:
missing_dates = ['2013-03-14', '2013-09-26', '2013-10-09', '2013-11-08', '2013-11-20']

# Вы знаете, какие данные должны быть вставлены для этих дат (замените на реальные значения)
data_to_insert = [
    {'open': 10304, 'close': 10341, 'high': 10399, 'low': 10233, 'value': 0, 'volume': 412376, 'time': '2013-03-14'},
    {'open': 10328, 'close': 10270, 'high': 10389, 'low': 10208, 'value': 0, 'volume': 605678, 'time': '2013-09-26'},
    {'open': 10308, 'close': 10279, 'high': 10406, 'low': 10204, 'value': 0, 'volume': 549267, 'time': '2013-10-09'},
    {'open': 10383, 'close': 10229, 'high': 10393, 'low': 10206, 'value': 0, 'volume': 621349, 'time': '2013-11-08'},
    {'open': 10656, 'close': 10621, 'high': 10697, 'low': 10541, 'value': 0, 'volume': 621349, 'time': '2013-11-20'}
]


# Применяем функцию для вставки строк
#df = insert_missing_dates(df, missing_dates, data_to_insert)

# Удаляем строку с индексом

for n in ['2013-12-14', '2014-06-13', '2019-03-08', '2022-01-07', '2017-05-01', '2022-02-23', '2022-02-28']:
    index1 = (df['time'] == n).idxmax()
    df = df.drop(index1)

# Сбрасываем индексы
df = insert_missing_dates(df, missing_dates, data_to_insert)
df = df.reset_index(drop=True)
#df = insert_missing_dates(df, missing_dates, data_to_insert)
#print(df4[2530:2540].to_markdown(index=True))
#print(df[2530:2540].to_markdown(index=True))
#
#print(df4[1815:1825].to_markdown(index=True))
#print(df[1815:1825].to_markdown(index=True))
#

#for i in range(len(df)):
#    if df["time"].iloc[i] == df4["Date"].iloc[i]:
#        print(i)

for i in df4['Date']:
    if pd.to_datetime(i) > pd.to_datetime('2025-02-28'):
        if (pd.to_datetime(i).weekday() == 5 and ((pd.to_datetime(i) - datetime.timedelta(seconds = 86400)).weekday() == 4)
            and ((pd.to_datetime(i) + datetime.timedelta(seconds = (86400*2))).weekday() == 0)):

            index0 = (df4['Date'] == i).idxmax()
            df4 = df4.drop(index0)

        elif (pd.to_datetime(i).weekday() == 6 and ((pd.to_datetime(i) - datetime.timedelta(seconds = (86400*2))).weekday() == 4)
              and ((pd.to_datetime(i) + datetime.timedelta(seconds = 86400)).weekday() == 0)):

            index0 = (df4['Date'] == i).idxmax()
            df4 = df4.drop(index0)


for i in range(len(df)):
    if df["time"].iloc[i] != df4["Date"].iloc[i]:
        print(i)
#print(df4[3325-5:3325+5].to_markdown(index=True))
#print(df[3325-5:3325+5].to_markdown(index=True))

df4 = df4.reset_index(drop=True)
print(len(df), len(df4))

def check_index_is_consecutive(df):
  return df.index.equals(pd.RangeIndex(start=0, stop=len(df)))

print(f"Индексы последовательны: {check_index_is_consecutive(df)}")
print(f"Индексы последовательны: {check_index_is_consecutive(df4)}")
"""

TICKER = 'BRENTF'

if TICKER == "BRENTF":

    tickers = ['BRF2_2012', 'BRG2_2012', 'BRH2_2012', 'BRJ2_2012', 'BRK2_2012', 'BRM2_2012',
           'BRN2_2012', 'BRQ2_2012', 'BRU2_2012', 'BRV2_2012', 'BRX2_2012', 'BRZ2_2012',
           'BRF3_2013', 'BRG3_2013', 'BRH3_2013', 'BRJ3_2013', 'BRK3_2013', 'BRM3_2013',
           'BRN3_2013', 'BRQ3_2013', 'BRU3_2013', 'BRV3_2013', 'BRX3_2013', 'BRZ3_2013',
           'BRF4_2014', 'BRG4_2014', 'BRH4_2014', 'BRJ4_2014', 'BRK4_2014', 'BRM4_2014',
           'BRN4_2014', 'BRQ4_2014', 'BRU4_2014', 'BRV4_2014', 'BRX4_2014', 'BRZ4_2014',
           'BRF5_2015', 'BRG5_2015', 'BRH5_2015', 'BRJ5_2015', 'BRK5_2015', 'BRM5_2015',
           'BRN5_2015', 'BRQ5_2015', 'BRU5_2015', 'BRV5_2015', 'BRX5_2015', 'BRZ5_2015',
           'BRF6_2016', 'BRG6_2016', 'BRH6_2016', 'BRJ6_2016', 'BRK6_2016', 'BRM6_2016',
           'BRN6_2016', 'BRQ6_2016', 'BRU6_2016', 'BRV6_2016', 'BRX6_2016', 'BRZ6_2016',
           'BRF7_2017', 'BRG7_2017', 'BRH7_2017', 'BRJ7_2017', 'BRK7_2017', 'BRM7_2017',
           'BRN7_2017', 'BRQ7_2017', 'BRU7_2017', 'BRV7_2017', 'BRX7_2017', 'BRZ7_2017',
           'BRG8_2018', 'BRH8_2018', 'BRJ8_2018', 'BRK8_2018', 'BRM8_2018',  # BRF8_2018
           'BRN8_2018', 'BRQ8_2018', 'BRU8_2018', 'BRV8_2018', 'BRX8_2018', 'BRZ8_2018',
           'BRF9', 'BRG9', 'BRH9', 'BRJ9', 'BRK9', 'BRM9', 'BRN9', 'BRQ9', 'BRU9',
           'BRV9', 'BRX9', 'BRZ9', 'BRF0', 'BRG0', 'BRH0', 'BRJ0', 'BRK0', 'BRM0', 'BRN0', 'BRQ0',
           'BRU0', 'BRV0', 'BRX0', 'BRZ0', 'BRF1', 'BRG1', 'BRH1', 'BRJ1', 'BRK1', 'BRM1', 'BRN1',
           'BRQ1', 'BRU1', 'BRV1', 'BRX1', 'BRZ1', 'BRF2', 'BRG2', 'BRH2', 'BRJ2', 'BRK2', 'BRM2',
           'BRN2', 'BRQ2', 'BRU2', 'BRV2', 'BRX2', 'BRZ2', 'BRF3', 'BRG3', 'BRH3', 'BRJ3', 'BRK3',
           'BRM3', 'BRN3', 'BRQ3', 'BRU3', 'BRV3', 'BRX3', 'BRZ3', 'BRF4', 'BRG4', 'BRH4', 'BRJ4',
           'BRK4', 'BRM4', 'BRN4', 'BRQ4', 'BRU4', 'BRV4', 'BRX4', 'BRZ4', 'BRF5', 'BRG5', 'BRH5',
           'BRJ5', 'BRK5', 'BRM5', 'BRN5', 'BRQ5', 'BRU5', 'BRV5', 'BRX5', 'BRZ5']


    def find_active_future(start_date, tickers, end_date):
        """
        Находит первый фьючерс из списка, в котором есть указанная дата.

        Args:
            tickers (list): Список тикеров фьючерсов.
            end_date (str): Дата, которую нужно найти (в формате 'YYYY-MM-DD').

        Returns:
            str: Тикер первого фьючерса, содержащего указанную дату, или None, если ни один фьючерс не содержит дату.
        """
        for ticker in tickers:
            try:
                rates_frame = get_moex_data(start_date, end_date, ticker,
                                            "futures")  # Изменил START_DATE на константу
                # Преобразуем столбец 'time' в datetime, если он еще не в этом формате
                rates_frame['time'] = pd.to_datetime(rates_frame['time'])

                # Преобразуем end_date в datetime для сравнения
                end_date_dt = pd.to_datetime(end_date)

                desired_time = datetime.time(10, 0, 0)
                current_time = datetime.datetime.now().time()

                end_date_d = end_date_dt

                if current_time < desired_time:
                    end_date_d = end_date_dt - datetime.timedelta(seconds=86400)

                if datetime.datetime.now().weekday() == 5:
                    end_date_d = end_date_dt - datetime.timedelta(seconds=86400)

                if datetime.datetime.now().weekday() == 6:
                    end_date_d = end_date_dt - datetime.timedelta(seconds=(86400 * 2))
                # Проверяем наличие даты в DataFrame
                if end_date_d in rates_frame[
                    'time'].values:  # Проверяем наличие datetime объекта в Series
                    return ticker  # Возвращаем тикер, если дата найдена

            except Exception as e:
                continue  # Переходим к следующему тикеру в случае ошибки

        print(f"Дата {end_date} не найдена ни в одном из тикеров")
        return None  # Возвращаем None, если дата не найдена ни в одном тикере


    active_future = find_active_future(None, tickers, today)
#    if rows[0][0] != None:
#        def find_active_future(start_date, tickers, end_date):
#            """
#            Находит первый фьючерс из списка, в котором есть указанная дата.
#
#            Args:
#                tickers (list): Список тикеров фьючерсов.
#                end_date (str): Дата, которую нужно найти (в формате 'YYYY-MM-DD').
#
#            Returns:
#                str: Тикер первого фьючерса, содержащего указанную дату, или None, если ни один фьючерс не содержит дату.
#            """
#            for ticker in tickers:
#                try:
#                    rates_frame = self.get_moex_data(start_date, end_date, ticker,
#                                                     "futures")  # Изменил START_DATE на константу
#                    # Преобразуем столбец 'time' в datetime, если он еще не в этом формате
#                    rates_frame['time'] = pd.to_datetime(rates_frame['time'])
#
#                    # Преобразуем end_date в datetime для сравнения
#                    end_date_dt = pd.to_datetime(end_date)
#
#                    desired_time = datetime.time(10, 0, 0)
#                    current_time = datetime.datetime.now().time()
#
#                    end_date_d = end_date_dt
#                    if current_time < desired_time:
#                        end_date_d = end_date_dt - datetime.timedelta(seconds=86400)
#
#                    if datetime.datetime.now().weekday() == 5:
#                        end_date_d = end_date_dt - datetime.timedelta(seconds=86400)
#
#                    if datetime.datetime.now().weekday() == 6:
#                        end_date_d = end_date_dt - datetime.timedelta(seconds=(86400 * 2))
#
#                    # Проверяем наличие даты в DataFrame
#                    if end_date_d in rates_frame['time'].values:  # Проверяем наличие datetime объекта в Series
#                        return ticker  # Возвращаем тикер, если дата найдена
#
#                except Exception as e:
#                    continue  # Переходим к следующему тикеру в случае ошибки
#
#            print(f"Дата {end_date} не найдена ни в одном из тикеров")
#            return None  # Возвращаем None, если дата не найдена ни в одном тикере
#
#
#        active_future = find_active_future(START_DATE, tickers, self.END_DATE)
#        rates_frame = self.get_moex_data(START_DATE, self.END_DATE, active_future, self.market)
#    else:
    def concat_futures(df1, df2):
        last_time1 = df1['time'].iloc[-1]

        index2 = (df2['time'] == last_time1).idxmax()

        df = pd.concat((df1[:-1], df2[index2:]), ignore_index=True)
        return df

    i = 0
    while tickers[i] != active_future:
        df1 = get_moex_data(None, today, tickers[i], "futures")
        df2 = get_moex_data(None, today, tickers[i + 1], "futures")

        if i == 0:
            df = concat_futures(df1, df2)
        elif i != 0:
            df = concat_futures(df, df2)
        i = i + 1


    df.reset_index(drop=True, inplace=True)

    df['time'] = pd.to_datetime(df['time'], format='%Y-%m-%d').dt.strftime('%Y-%m-%d')

    for n in ['2022-01-07', '2022-02-23', '2019-03-08', '2014-06-13', '2017-05-01']:
        index1 = (df['time'] == n).idxmax()
        df = df.drop(index1)

    for h in df['time']:
        if pd.to_datetime('2022-02-25', format='%Y-%m-%d') < pd.to_datetime(h, format='%Y-%m-%d') < pd.to_datetime(
                '2022-03-24', format='%Y-%m-%d'):
            index1 = (df['time'] == h).idxmax()
            df = df.drop(index1)


    def insert_missing_dates(df, missing_dates, data_to_insert):

        # Убедимся, что количество дат соответствует количеству данных
        if len(missing_dates) != len(data_to_insert):
            raise ValueError("Количество дат и данных для вставки не совпадают.")

        # 1. Преобразуем существующую колонку time в datetime
        df['time'] = pd.to_datetime(df['time'])

        # 2. Создаем новые строки в виде DataFrame
        new_rows = []
        for i in range(len(missing_dates)):
            date_str = missing_dates[i]
            data = data_to_insert[i]
            # Заполняем все колонки
            new_row = pd.DataFrame([data])  # Создаем DataFrame из словаря
            new_row['time'] = pd.to_datetime([date_str])  # Преобразуем дату в datetime
            new_rows.append(new_row)

        # 3. Объединяем новые строки с существующим DataFrame
        df = pd.concat([df, *new_rows], ignore_index=True)

        # 4. Сортируем DataFrame по дате
        df = df.sort_values(by='time').reset_index(drop=True)

        # 5. Возвращаем исходный формат time
        df['time'] = df['time'].dt.strftime('%Y-%m-%d')

        return df


    missing_dates = ['2013-03-14', '2013-09-26', '2013-10-09', '2013-11-08', '2013-11-20']

    # Вы знаете, какие данные должны быть вставлены для этих дат (замените на реальные значения)
    data_to_insert = [
        {'open': 109.07, 'close': 108.97, 'high': 109.18, 'low': 107.55, 'value': 0, 'volume': 89176,
         'time': '2013-03-14'},
        {'open': 109.3, 'close': 108.7, 'high': 109.64, 'low': 107.92, 'value': 0, 'volume': 67945,
         'time': '2013-09-26'},
        {'open': 110.24, 'close': 108.61, 'high': 110.54, 'low': 108.15, 'value': 0, 'volume': 57387,
         'time': '2013-10-09'},
        {'open': 104.06, 'close': 103.58, 'high': 104.09, 'low': 102.99, 'value': 0, 'volume': 52135,
         'time': '2013-11-08'},
        {'open': 107.79, 'close': 106.68, 'high': 108.35, 'low': 106.52, 'value': 0, 'volume': 39746,
         'time': '2013-11-20'}
    ]

    # Применяем функцию для вставки строк
    df = insert_missing_dates(df, missing_dates, data_to_insert)

    df = df.reset_index(drop=True)

    df['time'] = pd.to_datetime(df['time'], format='%Y-%m-%d')

    rates_frame = df

filename = ('https://raw.githubusercontent.com/Denis91530/AI_2/master/csv_export/SBER_new_D1.csv')

df1 = pd.read_csv(filename, sep = ",")

df1['Date'] = pd.to_datetime(df1['Date'], format='%Y-%m-%d')

df1['Date'] = pd.to_datetime(df1['Date'], format='%Y-%m-%d').dt.strftime('%Y-%m-%d')
df['time'] = pd.to_datetime(df['time'], format='%Y-%m-%d').dt.strftime('%Y-%m-%d')

last_time3 = df['time'].iloc[0]
last_time4 = df1['Date'].iloc[-1]


index3 = (df1['Date'] == last_time3).idxmax()
index4 = (df['time'] == last_time4).idxmax()

df4 = df1.loc[index3:]
df = df.loc[:index4]
df4.reset_index(drop=True, inplace=True)
df.reset_index(drop=True, inplace=True)


df4['Date'] = pd.to_datetime(df4['Date'], format='%Y-%m-%d').dt.strftime('%Y-%m-%d')
df['time'] = pd.to_datetime(df['time'], format='%Y-%m-%d').dt.strftime('%Y-%m-%d')

for i in df4['Date']:
    if pd.to_datetime(i) > pd.to_datetime('2025-02-28'):
        if (pd.to_datetime(i).weekday() == 5 and ((pd.to_datetime(i) - datetime.timedelta(seconds = 86400)).weekday() == 4)
            and ((pd.to_datetime(i) + datetime.timedelta(seconds = (86400*2))).weekday() == 0)):

            index0 = (df4['Date'] == i).idxmax()
            df4 = df4.drop(index0)

        elif (pd.to_datetime(i).weekday() == 6 and ((pd.to_datetime(i) - datetime.timedelta(seconds = (86400*2))).weekday() == 4)
              and ((pd.to_datetime(i) + datetime.timedelta(seconds = 86400)).weekday() == 0)):

            index0 = (df4['Date'] == i).idxmax()
            df4 = df4.drop(index0)

df4 = df4.reset_index(drop=True)
print(len(df), len(df4), len(rates_frame))

print(rates_frame.to_markdown(index=True))

def check_index_is_consecutive(df):
  return df.index.equals(pd.RangeIndex(start=0, stop=len(df)))

print(f"Индексы последовательны: {check_index_is_consecutive(df)}")
print(f"Индексы последовательны: {check_index_is_consecutive(rates_frame)}")


for i in range(1, len(df)):
    if df["time"].iloc[i] != df4["Date"].iloc[i]:
        print(i)
print("ОПА11111111111111111111111")