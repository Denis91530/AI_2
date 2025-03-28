#Сделать, чтобы новая функция правильно работала с таблицей (поизменять имена)


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


class SharesDataLoader():
    """A class for loading shares data from MetaTrader5"""

    def __init__(self, share_name, ticker, market):
        self.share_name = share_name
        self.conn = None
        self.cursor = None
        self.connection_to_db = False
        self.how_many_bars_max = 50000
        # --- Настройка ---
        self.TICKER = ticker  # Тикер акции
        self.START_DATE = None
        self.END_DATE = datetime.date.today().strftime("%Y-%m-%d")  # Сегодняшняя дата
        self.market = market
        # --- Конец настройки ---

        self.timezone = pytz.timezone("Etc/UTC")    # установим таймзону в UTC
        # создадим объект datetime в таймзоне UTC, чтобы не применялось смещение локальной таймзоны
        # self._utc_till = datetime.datetime.now(self.timezone)# datetime.datetime(2021, 10, 10, tzinfo=self.timezone)

    def connect_to_metatrader5(self, path):
        mt5.initialize(path=path)
        # установим подключение к терминалу MetaTrader 5
        if not mt5.initialize():
            print("Нет подключения к MetaTrader5, код ошибки =", mt5.last_error())
            # завершим подключение к терминалу MetaTrader 5
            mt5.shutdown()
            quit()
        else:
            print("Успешное подключение к MetaTrader5")

    def disconnect_from_metatrader5(self):
        # Close connection
        if self.connection_to_db: self.conn.close(); print("Успешное отключение от базы данных")
        mt5.shutdown()
        print("Успешное отключение от MetaTrader5")

    def connect_to_db(self, host, user, passwd, db):
        try:
            self.conn = MySQLdb.connect(host=host, user=user, passwd=passwd, db=db)
            self.cursor = self.conn.cursor()
            self.connection_to_db = True
            print("Успешное подключение к базе данных")
        except MySQLdb.Error as ex:
            print("Нет подключения к базе данных, код ошибки =", ex)
            quit()

    def is_file_csv_exists(self, ticker, timeframe, how_many_bars_max, how_many_bars_update, export_dir, by_timeframes=False):
        _timeframe = "D1"
        if timeframe == mt5.TIMEFRAME_MN1:  _timeframe = "MN1"
        if timeframe == mt5.TIMEFRAME_W1:   _timeframe = "W1"
        if timeframe == mt5.TIMEFRAME_D1:   _timeframe = "D1"
        if timeframe == mt5.TIMEFRAME_H4:   _timeframe = "H4"
        if timeframe == mt5.TIMEFRAME_H1:   _timeframe = "H1"
        if timeframe == mt5.TIMEFRAME_M30:  _timeframe = "M30"
        if timeframe == mt5.TIMEFRAME_M15:  _timeframe = "M15"
        if timeframe == mt5.TIMEFRAME_M10:  _timeframe = "M10"
        if timeframe == mt5.TIMEFRAME_M5:   _timeframe = "M5"
        if timeframe == mt5.TIMEFRAME_M1:   _timeframe = "M1"

        if by_timeframes:
            export_dir = os.path.join(export_dir, _timeframe)
        filename = os.path.join(export_dir, ticker + "_new_" + _timeframe + ".csv")
        is_file_exists = os.path.isfile(filename)  # Существует ли файл
        if is_file_exists:
            return how_many_bars_update
        return how_many_bars_max

    def get_moex_data(self, start_date, end_date, ticker, market):
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
                                                      end=end, columns=None, market="forts", engine="futures")
            # 3. Преобразуем данные в DataFrame
            df = pd.DataFrame(data)
            df = df.rename(columns={'begin': 'time'})
            df["time"] = pd.to_datetime(df["time"], format='%Y-%m-%d %H:%M:%S')
            df["time"] = df["time"].dt.strftime('%Y-%m-%d')  # Используем .dt.strftime
            return df.iloc[:, :-1]

        except Exception as e:

#            print(f"Ошибка при загрузке данных: {e}")
            return None

    def get_share_data_from_db(self, ticker, timeframe, how_many_bars):
        if timeframe == mt5.TIMEFRAME_MN1:  timeframe = "MN1"
        if timeframe == mt5.TIMEFRAME_W1:   timeframe = "W1"
        if timeframe == mt5.TIMEFRAME_D1:   timeframe = "D1"
        if timeframe == mt5.TIMEFRAME_H4:   timeframe = "H4"
        if timeframe == mt5.TIMEFRAME_H1:   timeframe = "H1"
        if timeframe == mt5.TIMEFRAME_M30:  timeframe = "M30"
        if timeframe == mt5.TIMEFRAME_M15:  timeframe = "M15"
        if timeframe == mt5.TIMEFRAME_M10:  timeframe = "M10"
        if timeframe == mt5.TIMEFRAME_M5:   timeframe = "M5"
        if timeframe == mt5.TIMEFRAME_M1:   timeframe = "M1"

        table_name = ticker + "_new_" + timeframe
        self.cursor.execute(
            "SELECT time, open, high, low, close, volume, value FROM `" + table_name + "`" + " ORDER BY time DESC LIMIT " + str(how_many_bars)
        )

        # Get all data from table
        rows = self.cursor.fetchall()
        dataframe = pd.DataFrame(rows, columns=["Date", "Open", "High", "Low", "Close", "Volume", "Value"])
        print(dataframe)
        dataframe = dataframe[::-1].reset_index(drop=True)  # Reverse Ordering of DataFrame Rows + Reset index
        #print(dataframe.dtypes)
        return dataframe

    def export_to_csv_from_df(self, ticker, timeframe, data, export_dir, by_timeframes=False):
        _timeframe = "D1"
        if timeframe == mt5.TIMEFRAME_MN1:  _timeframe = "MN1"
        if timeframe == mt5.TIMEFRAME_W1:   _timeframe = "W1"
        if timeframe == mt5.TIMEFRAME_D1:   _timeframe = "D1"
        if timeframe == mt5.TIMEFRAME_H4:   _timeframe = "H4"
        if timeframe == mt5.TIMEFRAME_H1:   _timeframe = "H1"
        if timeframe == mt5.TIMEFRAME_M30:  _timeframe = "M30"
        if timeframe == mt5.TIMEFRAME_M15:  _timeframe = "M15"
        if timeframe == mt5.TIMEFRAME_M10:  _timeframe = "M10"
        if timeframe == mt5.TIMEFRAME_M5:   _timeframe = "M5"
        if timeframe == mt5.TIMEFRAME_M1:   _timeframe = "M1"

        # print(ticker)
        # print(data)
        data = data[["time", "open", "high", "low", "close", "volume", "value"]]
        data.rename(columns={"time": "datetime"}, inplace=True)

        if not os.path.exists(export_dir): os.makedirs(export_dir)
        if by_timeframes:
            export_dir = os.path.join(export_dir, _timeframe)
            if not os.path.exists(export_dir): os.makedirs(export_dir)

        filename = os.path.join(export_dir, ticker+"_new_"+_timeframe+".csv")
        is_file_exists = os.path.isfile(filename)
        print(is_file_exists)# Существует ли файл
        if is_file_exists:
            # если файл есть, то объединяем
            data_from_file = pd.read_csv(filename)  # Считываем файл в DataFrame
            if _timeframe in ['D1', 'W1', 'MN1']:
                data_from_file["datetime"] = pd.to_datetime(data_from_file["datetime"], format='%Y-%m-%d')  # Переводим индекс в формат datetime
            else:
                data_from_file["datetime"] = pd.to_datetime(data_from_file["datetime"], format='%Y-%m-%d %H:%M:%S')  # Переводим индекс в формат datetime

            _last_date_from_csv = data_from_file["datetime"].iloc[-1]

            _first_date_from_data = data["datetime"].iloc[0]
            _last_date_from_data = data["datetime"].iloc[-1]

            if _last_date_from_data == _last_date_from_csv:
                print(f"- нечего обновлять по тикеру {ticker} {_timeframe} ...")
                return

            j = 0
            for j in range(0, len(data)):
                _date_from_data = data["datetime"].iloc[j]
                if _date_from_data > _last_date_from_csv:
                    data = data.iloc[j:]
                    break

            print(f'- считан csv файл по тикеру {ticker} {_timeframe}: {data_from_file["datetime"].iloc[0]} - {data_from_file["datetime"].iloc[-1]} \t size: {len(data_from_file)}')
            data = pd.concat([data_from_file, data])  # Объединяем файл и данные

        data["volume"] = data["volume"].astype('int32')
        print(f'- записаны данные в csv файл по тикеру {ticker} {_timeframe}: {data["datetime"].iloc[0]} - {data["datetime"].iloc[-1]} \t size: {len(data)}')
        data.to_csv(filename, index=False, encoding='utf-8')

    def export_to_csv(self, ticker, timeframe, how_many_bars, export_dir):
        _timeframe = "D1"
        if timeframe == mt5.TIMEFRAME_MN1:  _timeframe = "MN1"
        if timeframe == mt5.TIMEFRAME_W1:   _timeframe = "W1"
        if timeframe == mt5.TIMEFRAME_D1:   _timeframe = "D1"
        if timeframe == mt5.TIMEFRAME_H4:   _timeframe = "H4"
        if timeframe == mt5.TIMEFRAME_H1:   _timeframe = "H1"
        if timeframe == mt5.TIMEFRAME_M30:  _timeframe = "M30"
        if timeframe == mt5.TIMEFRAME_M15:  _timeframe = "M15"
        if timeframe == mt5.TIMEFRAME_M10:  _timeframe = "M10"
        if timeframe == mt5.TIMEFRAME_M5:   _timeframe = "M5"
        if timeframe == mt5.TIMEFRAME_M1:   _timeframe = "M1"

        table_name = ticker + "_new_" + _timeframe
        self.cursor.execute(
            "SELECT time, open, high, low, close, volume, value FROM `" + table_name + "`"
        )

        # Get all data from table
        rows = self.cursor.fetchall()
        dataframe = pd.DataFrame(rows, columns=["Date", "Open", "High", "Low", "Close", "Volume", "Value"])
        dataframe = dataframe[::-1].reset_index(drop=True)  # Reverse Ordering of DataFrame Rows + Reset index

        if not os.path.exists(export_dir): os.makedirs(export_dir)
        dataframe.to_csv(os.path.join(export_dir, ticker+"_new_"+_timeframe+".csv"), index=False, encoding='utf-8')

    def execute_with_reconnect(self, query, params=None, max_attempts=3):
        for attempt in range(max_attempts):
            try:
                if params:
                    self.cursor.execute(query, params)
                else:
                    self.cursor.execute(query)
                return
            except MySQLdb.OperationalError as e:
                if e.args[0] in (2006, 2013):
                    print(f"Ошибка подключения (attempt {attempt + 1}/{max_attempts}): {e}")
                    try:
                        self.connect_to_db(host="127.0.0.1",
                                           user="root",
                                           passwd="DEN123",
                                           db="bd_for_action")
                        self.cursor = self.conn.cursor()
                        print("Переподключение к базе данных...")
                    except MySQLdb.Error as re_error:
                        print(f"Ошибка переподключения к базе данных: {re_error}")
                        if attempt == max_attempts - 1:
                            raise
                        else:
                            print("Прошла попытка №", attempt)
                else:
                    raise

    def always_get_share_data(self, ticker, timeframe):
        _timeframe = "D1"
        how_many_bars = 0
        time_in_seconds_bar = 0
        if timeframe == mt5.TIMEFRAME_D1:   time_in_seconds_bar = 86400  # 60*60*24
        if timeframe == mt5.TIMEFRAME_H4:   time_in_seconds_bar = 14400  # 60*60*4
        if timeframe == mt5.TIMEFRAME_H1:   time_in_seconds_bar = 3600  # 60*60
        if timeframe == mt5.TIMEFRAME_M30:  time_in_seconds_bar = 1800  # 60*30
        if timeframe == mt5.TIMEFRAME_M15:  time_in_seconds_bar = 900  # 60*15
        if timeframe == mt5.TIMEFRAME_M10:  time_in_seconds_bar = 600  # 60*10
        if timeframe == mt5.TIMEFRAME_M5:   time_in_seconds_bar = 300  # 60*5
        if timeframe == mt5.TIMEFRAME_M1:   time_in_seconds_bar = 60  # 60

        if timeframe == mt5.TIMEFRAME_MN1:  _timeframe = "MN1"
        if timeframe == mt5.TIMEFRAME_W1:   _timeframe = "W1"
        if timeframe == mt5.TIMEFRAME_D1:   _timeframe = "D1"
        if timeframe == mt5.TIMEFRAME_H4:   _timeframe = "H4"
        if timeframe == mt5.TIMEFRAME_H1:   _timeframe = "H1"
        if timeframe == mt5.TIMEFRAME_M30:  _timeframe = "M30"
        if timeframe == mt5.TIMEFRAME_M15:  _timeframe = "M15"
        if timeframe == mt5.TIMEFRAME_M10:  _timeframe = "M10"
        if timeframe == mt5.TIMEFRAME_M5:   _timeframe = "M5"
        if timeframe == mt5.TIMEFRAME_M1:   _timeframe = "M1"


        table_name = ticker + "_new_" + _timeframe
        print("Название таблицы:", table_name)
        # ----------------------- Обновление истории -----------------------
        while True:

            # let's execute our query to db
            self.execute_with_reconnect(
                "SELECT max(time) FROM `" + table_name + "`"
            )
            if self.execute_with_reconnect("SELECT max(time) FROM `" + table_name + "`"):
                print("Таблица <", table_name, "> есть")

            # Get all data from table
            rows = self.cursor.fetchall()
            last_bar_time = 0

            if rows[0][0] == None:
                START_DATE = self.START_DATE
            else:
                last_bar_time = rows[0][0] + datetime.timedelta(seconds=time_in_seconds_bar)
                print("Последнее обновление было в", last_bar_time)
                START_DATE = last_bar_time - datetime.timedelta(seconds=time_in_seconds_bar * 7)

                # calc missed bars
                today = datetime.datetime.now()
                num_bars_to_load = ((today - last_bar_time).total_seconds()) // time_in_seconds_bar + 1
                print("Новых баров:", num_bars_to_load)
                how_many_bars = int(num_bars_to_load)
            # получим данные по завтрашний день
            utc_till = datetime.datetime.now() + datetime.timedelta(seconds=time_in_seconds_bar)
            print("Время до которого сейчас скачаются бары в новый датафрейм:", utc_till)
#            rates = mt5.copy_rates_from(ticker, timeframe, utc_till, how_many_bars)
            # создадим из полученных данных DataFrame
            if self.market == "futures":

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
                            rates_frame = self.get_moex_data(start_date, end_date, ticker,
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

                def concat_futures(df1, df2):
                    last_time1 = df1['time'].iloc[-1]

                    index2 = (df2['time'] == last_time1).idxmax()

                    df = pd.concat((df1[:-1], df2[index2:]), ignore_index=True)
                    return df

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

                if self.TICKER == "SBERF":
                    tickers = ['SRH2_2012', 'SRM2_2012', 'SRU2_2012', 'SRZ2_2012', 'SRH3_2013', 'SRM3_2013',
                               'SRU3_2013', 'SRZ3_2013', 'SRH4_2014', 'SRM4_2014', 'SRU4_2014', 'SRZ4_2014',
                               'SRH5_2015', 'SRM5_2015', 'SRU5_2015', 'SRZ5_2015', 'SRH6_2016', 'SRM6_2016',
                               'SRU6_2016', 'SRZ6_2016', 'SRH7_2017', 'SRM7_2017', 'SRU7_2017', 'SRZ7_2017',
                               'SRH8_2018', 'SRM8_2018', 'SRU8_2018', 'SRZ8_2018', 'SRH9', 'SRM9', 'SRU9', 'SRZ9',
                               'SRH0', 'SRM0', 'SRU0', 'SRZ0', 'SRH1', 'SRM1', 'SRU1', 'SRZ1', 'SRH2', 'SRM2', 'SRU2',
                               'SRZ2', 'SRH3', 'SRM3', 'SRU3',
                               'SRZ3', 'SRH4', 'SRM4', 'SRU4', 'SRZ4', 'SRH5', 'SRM5', 'SRU5']

                    active_future = find_active_future(START_DATE, tickers, self.END_DATE)

                    if rows[0][0] != None:
                        rates_frame = self.get_moex_data(START_DATE, self.END_DATE, active_future, self.market)

                    else:

                        i = 0
                        while tickers[i] != active_future:
                            df1 = self.get_moex_data(START_DATE, self.END_DATE, tickers[i], self.market)
                            df2 = self.get_moex_data(START_DATE, self.END_DATE, tickers[i+1], self.market)

                            if i == 0:
                                df = concat_futures(df1, df2)
                            elif i != 0:
                                df = concat_futures(df, df2)
                            i = i + 1

                        df.reset_index(drop=True, inplace=True)

                        df['time'] = pd.to_datetime(df['time'], format='%Y-%m-%d').dt.strftime('%Y-%m-%d')

                        missing_dates = ['2013-03-14', '2013-09-26', '2013-10-09', '2013-11-08', '2013-11-20']

                        # Вы знаете, какие данные должны быть вставлены для этих дат (замените на реальные значения)
                        data_to_insert = [
                            {'open': 10304, 'close': 10341, 'high': 10399, 'low': 10233, 'value': 0, 'volume': 412376,
                             'time': '2013-03-14'},
                            {'open': 10328, 'close': 10270, 'high': 10389, 'low': 10208, 'value': 0, 'volume': 605678,
                             'time': '2013-09-26'},
                            {'open': 10308, 'close': 10279, 'high': 10406, 'low': 10204, 'value': 0, 'volume': 549267,
                             'time': '2013-10-09'},
                            {'open': 10383, 'close': 10229, 'high': 10393, 'low': 10206, 'value': 0, 'volume': 621349,
                             'time': '2013-11-08'},
                            {'open': 10656, 'close': 10621, 'high': 10697, 'low': 10541, 'value': 0, 'volume': 621349,
                             'time': '2013-11-20'}
                        ]

                        for n in ['2013-12-14', '2014-06-13', '2019-03-08', '2022-01-07', '2017-05-01', '2022-02-23',
                                  '2022-02-28']:
                            index1 = (df['time'] == n).idxmax()
                            df = df.drop(index1)

                        df = insert_missing_dates(df, missing_dates, data_to_insert)
                        df = df.reset_index(drop=True)


                        df['time'] = pd.to_datetime(df['time'], format='%Y-%m-%d')

                        rates_frame = df

                elif self.TICKER == "BRENTF":

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


                    active_future = find_active_future(START_DATE, tickers, self.END_DATE)

                    if rows[0][0] != None:
                        rates_frame = self.get_moex_data(START_DATE, self.END_DATE, active_future, self.market)

                    else:

                        i = 0
                        while tickers[i] != active_future:
                            df1 = self.get_moex_data(START_DATE, self.END_DATE, tickers[i], self.market)
                            df2 = self.get_moex_data(START_DATE, self.END_DATE, tickers[i + 1], self.market)

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


            else:
                rates_frame = self.get_moex_data(START_DATE, self.END_DATE, self.TICKER, self.market)
            # сконвертируем время в виде секунд в формат datetime
            if len(rates_frame.index):
#                rates_frame['time'] = pd.to_datetime(rates_frame['time'], unit='s')
                rates_frame['time'] = pd.to_datetime(rates_frame['time'])

            # выведем данные
            print("\nВесь новый датафрейм с данными:")
            print(rates_frame)

            desired_time = datetime.time(10, 0, 0)
            current_time = datetime.datetime.now().time()
            a = datetime.datetime.now().weekday()

            if current_time < desired_time or a == 5 or a == 6:
                # Код, если текущее время меньше 10:00
                print("\nСейчас время до 10:00, биржа закрыта")
                print("\nТолько скачанный датафрейм с данными из нового:")

                for i in range(len(rates_frame.index)):
                    _time = rates_frame.at[i, "time"]
                    _open = rates_frame.at[i, "open"]
                    _high = rates_frame.at[i, "high"]
                    _low = rates_frame.at[i, "low"]
                    _close = rates_frame.at[i, "close"]
                    _volume = rates_frame.at[i, "volume"]
                    _value = rates_frame.at[i, "value"]

                    print(i, _time, _open, _high, _low, _close, _volume, _value)
                    if ((rows[0][0] != None) and (_time >= last_bar_time)) or ((rows[0][0] == None)):
                        # let's insert row in table
                        self.execute_with_reconnect(
                            "INSERT INTO `" + table_name + "` (time, open, high, low, close, volume, value) "
                                                           "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                            (_time, _open, _high, _low, _close, _volume, _value))
            else:
                # Код, если текущее время больше или равно 10:00
                print("\nСейчас время после 10:00, биржа открыта")
                print("\nТолько скачанный датафрейм с данными из нового:")
                for i in range(len(rates_frame.index) - 1):  # Последний бар не берем -1 т.к. он еще формируется
                    _time = rates_frame.at[i, "time"]
                    _open = rates_frame.at[i, "open"]
                    _high = rates_frame.at[i, "high"]
                    _low = rates_frame.at[i, "low"]
                    _close = rates_frame.at[i, "close"]
                    _volume = rates_frame.at[i, "volume"]
                    _value = rates_frame.at[i, "value"]

                    print(i, _time, _open, _high, _low, _close, _volume, _value)


                    if ((rows[0][0] != None) and (_time >= last_bar_time)) or ((rows[0][0] == None)):
                        # let's insert row in table
                        self.execute_with_reconnect(
                            "INSERT INTO `" + table_name + "` (time, open, high, low, close, volume, value) "
                                                           "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                            (_time, _open, _high, _low, _close, _volume, _value))

            # to commit changes to db!!!
            # run this command:
            self.conn.commit()
            if current_time < desired_time or a == 5 or a == 6:
                last_bar_time = rates_frame.at[len(rates_frame.index) - 1, "time"] + datetime.timedelta(seconds = time_in_seconds_bar)
            elif a == 0 and current_time > desired_time:
                last_bar_time = rates_frame.at[len(rates_frame.index) - 2, "time"] + datetime.timedelta(seconds = time_in_seconds_bar)
            else:
                last_bar_time = rates_frame.at[len(rates_frame.index) - 1, "time"]

            print("\nВремя загрузки последнего бара:", last_bar_time)
            if a == 0 or a == 5 or a == 6:
                next_bar_time = last_bar_time + datetime.timedelta(seconds=time_in_seconds_bar * 3)
            else:
                next_bar_time = last_bar_time + datetime.timedelta(seconds=time_in_seconds_bar)
            print("Время загрузки следующего бара:", next_bar_time)
            print("Время сейчас:", datetime.datetime.now())

            if next_bar_time > datetime.datetime.now():
                break
            else:
                print("Не получилось выйти из цикла")

        # ----------------------- Обновление в реальном времени -----------------------
        from time import sleep
        t = 1
        while True:

            # ----------------------- Создание таблицы в реальном времени -----------------------

            export_dir = "csv_export"

            _timeframe = "D1"
            if timeframe == mt5.TIMEFRAME_MN1:  _timeframe = "MN1"
            if timeframe == mt5.TIMEFRAME_W1:   _timeframe = "W1"
            if timeframe == mt5.TIMEFRAME_D1:   _timeframe = "D1"
            if timeframe == mt5.TIMEFRAME_H4:   _timeframe = "H4"
            if timeframe == mt5.TIMEFRAME_H1:   _timeframe = "H1"
            if timeframe == mt5.TIMEFRAME_M30:  _timeframe = "M30"
            if timeframe == mt5.TIMEFRAME_M15:  _timeframe = "M15"
            if timeframe == mt5.TIMEFRAME_M10:  _timeframe = "M10"
            if timeframe == mt5.TIMEFRAME_M5:   _timeframe = "M5"
            if timeframe == mt5.TIMEFRAME_M1:   _timeframe = "M1"

            table_name = ticker + "_new_" + _timeframe
            self.execute_with_reconnect(
                "SELECT time, open, high, low, close, volume, value FROM `" + table_name + "`"
            )

            # Get all data from table
            rows = self.cursor.fetchall()
            dataframe = pd.DataFrame(rows, columns=["Date", "Open", "High", "Low", "Close", "Volume", "Value"])
            # dataframe = dataframe[::-1].reset_index(drop=True)  # Reverse Ordering of DataFrame Rows + Reset index

            if not os.path.exists(export_dir): os.makedirs(export_dir)
            dataframe.to_csv(os.path.join(export_dir, ticker + "_new_" + _timeframe + ".csv"), index=False,
                             encoding='utf-8')
            # ----------------------- Создание таблицы в реальном времени -----------------------

            a = datetime.datetime.now().weekday()
            if a == 0 or a == 5 or a == 6:
                next_bar_time = last_bar_time + datetime.timedelta(seconds=time_in_seconds_bar * 3)
            else:
                next_bar_time = last_bar_time + datetime.timedelta(seconds=time_in_seconds_bar)
            wait_for_calculated = int((next_bar_time - datetime.datetime.now()).total_seconds())
            print("\nДля проверки:\nВремя загрузки последнего бара:", last_bar_time, "| Время загрузки следующего бара:",
                  next_bar_time)
            print("Осталось ждать:", wait_for_calculated, "секунд")

            # cv2.waitKey(abs(wait_for_calculated*1000+500)) # 500 milsec delay
            for sec in range(abs(wait_for_calculated)):
                if ((sec + 1) % 25 == 0):
                    print(wait_for_calculated - sec)
                else:
                    print(wait_for_calculated - sec, end=" ")
                sleep(t)

            # add new data to table
            # print(datetime.datetime.now())
            # check_last_bar_writed_to_db = get_last_bar_time(cursor)
            # print(check_last_bar_writed_to_db)
            # if (last_bar_time == check_last_bar_writed_to_db):
            #     print("Ok")
            # else:
            #     print("Failed write to DB!")
            # ...

            # calc missed bars
            today = datetime.datetime.now()
            num_bars_to_load = ((today - last_bar_time).total_seconds()) // time_in_seconds_bar + 5  # берем +5 бар назад
            print(num_bars_to_load)

            how_many_bars = int(num_bars_to_load)

            # получим данные по завтрашний день
            utc_till = datetime.datetime.now() + datetime.timedelta(seconds=time_in_seconds_bar)
            print(utc_till)

            # exit(1)

            check_we_have_next_bar_loaded = False
            while not check_we_have_next_bar_loaded:
 #               rates = mt5.copy_rates_from(ticker, timeframe, utc_till, how_many_bars)
                today1 = datetime.date.today().strftime("%Y-%m-%d")
                # создадим из полученных данных DataFrame
                start_moex = last_bar_time - datetime.timedelta(seconds=time_in_seconds_bar * 5)
                rates_frame = self.get_moex_data(start_moex, today1, self.TICKER)
                # сконвертируем время в виде секунд в формат datetime
                if len(rates_frame.index):
                    rates_frame['time'] = pd.to_datetime(rates_frame['time'])

                # проверка, что есть данные следующей свечи
                for i in range(len(rates_frame.index)):
                    _time = rates_frame.at[i, "time"]
                    print(i)
                    print(len(rates_frame.index))
                    print(_time)
                    print(last_bar_time)
                    if _time > (last_bar_time - datetime.timedelta(seconds = time_in_seconds_bar)):
                        check_we_have_next_bar_loaded = True
                        print("Были получены данные следующей свечи из Metatrader 5")
                    else:
                        print("Данные следующей свечи не были получены из Metatrader 5, сейчас будет следующая попытка")
                        sleep(t)  # 1000 milsec delay

            # выведем данные
            print("\nВыведем датафрейм с данными")
            print(rates_frame)
            for i in range(len(rates_frame.index)):
                _time = rates_frame.at[i, "time"]
                _open = rates_frame.at[i, "open"]
                _high = rates_frame.at[i, "high"]
                _low = rates_frame.at[i, "low"]
                _close = rates_frame.at[i, "close"]
                _volume = rates_frame.at[i, "volume"]
                _value = rates_frame.at[i, "value"]
                print(i, _time, _open, _high, _low, _close, _volume, _value)

                if _time >= last_bar_time and _time < next_bar_time:
                    # let's insert row in table
                    self.execute_with_reconnect(
                          "INSERT INTO `" + table_name + "` (time, open, high, low, close, volume, value) "
                                                         "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                          (_time, _open, _high, _low, _close, _volume, _value))
            # to commit changes to db!!!
            # run this command:
            self.conn.commit()

            last_bar_time = next_bar_time

        # ----------------------- Обновление в реальном времени -----------------------

        pass
