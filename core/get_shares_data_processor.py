import os
import cv2
import MySQLdb                  # импортируем модуль для работы с БД MySql
import MetaTrader5 as mt5       # импортируем модуль для подключения к MetaTrader5
import pandas as pd             # импортируем модуль pandas для вывода полученных данных в табличной форме
import time, datetime
import pytz                     # импортируем модуль pytz для работы с таймзоной

class SharesDataLoader():
    """A class for loading shares data from MetaTrader5"""

    def __init__(self, share_name):
        self.share_name = share_name
        self.conn = None
        self.cursor = None
        self.connection_to_db = False
        self.how_many_bars_max = 50000

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
        filename = os.path.join(export_dir, ticker + "_" + _timeframe + ".csv")
        is_file_exists = os.path.isfile(filename)  # Существует ли файл
        if is_file_exists:
            return how_many_bars_update
        return how_many_bars_max

    def get_share_data(self, ticker, timeframe, utc_till, how_many_bars, remove_today_bars=False):
        rates = mt5.copy_rates_from(ticker, timeframe, utc_till, how_many_bars)
        # создадим из полученных данных DataFrame
        rates_frame = pd.DataFrame(rates)
        # сконвертируем время в виде секунд в формат datetime
        if len(rates_frame.index):
            rates_frame['time'] = pd.to_datetime(rates_frame['time'], unit='s')

            if remove_today_bars:  # обрезка данных до now
                now = datetime.datetime.fromisoformat(datetime.datetime.now().strftime('%Y-%m-%d') + " " + "00:00")
                rates_frame.set_index('time', inplace=True)
                indexes = rates_frame.index.tolist()
                for j in range(1, len(indexes) + 1):
                    if indexes[len(indexes) - j] < now:
                        # print("ok j = ", j)
                        # df_new = df_new[len(indexes) - j + 1:]      # хвост данных
                        rates_frame = rates_frame[:len(indexes) - j + 1]
                        break
                rates_frame.reset_index(inplace=True)
                # print(rates_frame)

        return rates_frame

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

        table_name = ticker + "_" + timeframe
        self.cursor.execute(
            "SELECT time, open, high, low, close, volume FROM `" + table_name + "`" + " ORDER BY time DESC LIMIT " + str(how_many_bars)
        )

        # Get all data from table
        rows = self.cursor.fetchall()
        dataframe = pd.DataFrame(rows, columns=["Date", "Open", "High", "Low", "Close", "Volume"])
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
        data = data[["time", "open", "high", "low", "close", "real_volume"]]
        data.rename(columns={"time": "datetime", "real_volume": "volume"}, inplace=True)

        if not os.path.exists(export_dir): os.makedirs(export_dir)
        if by_timeframes:
            export_dir = os.path.join(export_dir, _timeframe)
            if not os.path.exists(export_dir): os.makedirs(export_dir)

        filename = os.path.join(export_dir, ticker+"_"+_timeframe+".csv")
        is_file_exists = os.path.isfile(filename)  # Существует ли файл
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

        table_name = ticker + "_" + _timeframe
        self.cursor.execute(
            "SELECT time, open, high, low, close, tick_volume FROM `" + table_name + "`"
        )

        # Get all data from table
        rows = self.cursor.fetchall()
        dataframe = pd.DataFrame(rows, columns=["Date", "Open", "High", "Low", "Close", "Tick_Volume"])
        dataframe = dataframe[::-1].reset_index(drop=True)  # Reverse Ordering of DataFrame Rows + Reset index

        if not os.path.exists(export_dir): os.makedirs(export_dir)
        dataframe.to_csv(os.path.join(export_dir, ticker+"_"+_timeframe+".csv"), index=False, encoding='utf-8')

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

        table_name = ticker + "_" + _timeframe
        print("Название таблицы:", table_name)
        # ----------------------- Обновление истории -----------------------
        while True:
            # let's execute our query to db
            self.cursor.execute(
                "SELECT max(time) FROM `" + table_name + "`"
            )
            if self.cursor.execute( "SELECT max(time) FROM `" + table_name + "`"):
                print("Таблица <", table_name, "> есть")

            # Get all data from table
            rows = self.cursor.fetchall()
            last_bar_time = 0

            if rows[0][0] == None:
                how_many_bars = self.how_many_bars_max
            else:
                last_bar_time = rows[0][0] + datetime.timedelta(seconds=time_in_seconds_bar)
                print("Последнее обновление было в", last_bar_time)

                # calc missed bars
                today = datetime.datetime.now()
                num_bars_to_load = ((today - last_bar_time).total_seconds()) // time_in_seconds_bar + 1
                print("Новых баров:", num_bars_to_load)
                how_many_bars = int(num_bars_to_load)
            # получим данные по завтрашний день
            utc_till = datetime.datetime.now() + datetime.timedelta(seconds=time_in_seconds_bar)
            print("Время до которого сейчас скачаются бары в новый датафрейм:", utc_till)
            rates = mt5.copy_rates_from(ticker, timeframe, utc_till, how_many_bars)
            # создадим из полученных данных DataFrame
            rates_frame = pd.DataFrame(rates)
            # сконвертируем время в виде секунд в формат datetime
            if len(rates_frame.index):
                rates_frame['time'] = pd.to_datetime(rates_frame['time'], unit='s')

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
                    _tick_volume = rates_frame.at[i, "tick_volume"]
                    _real_volume = rates_frame.at[i, "real_volume"]

                    print(i, _time, _open, _high, _low, _close, _tick_volume, _real_volume)
                    if ((rows[0][0] != None) and (_time >= last_bar_time)) or ((rows[0][0] == None)):
                        # let's insert row in table
                        self.cursor.execute(
                            "INSERT INTO `" + table_name + "` (time, open, high, low, close, volume, tick_volume) "
                                                           "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                            (_time, _open, _high, _low, _close, _real_volume, _tick_volume))
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
                    _tick_volume = rates_frame.at[i, "tick_volume"]
                    _real_volume = rates_frame.at[i, "real_volume"]

                    print(i, _time, _open, _high, _low, _close, _tick_volume, _real_volume)


                    if ((rows[0][0] != None) and (_time >= last_bar_time)) or ((rows[0][0] == None)):
                        # let's insert row in table
                        self.cursor.execute(
                            "INSERT INTO `" + table_name + "` (time, open, high, low, close, volume, tick_volume) "
                                                        "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                            (_time, _open, _high, _low, _close, _real_volume, _tick_volume))

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

            table_name = ticker + "_" + _timeframe
            self.cursor.execute(
                "SELECT time, open, high, low, close, tick_volume FROM `" + table_name + "`"
            )

            # Get all data from table
            rows = self.cursor.fetchall()
            dataframe = pd.DataFrame(rows, columns=["Date", "Open", "High", "Low", "Close", "Tick_Volume"])
            # dataframe = dataframe[::-1].reset_index(drop=True)  # Reverse Ordering of DataFrame Rows + Reset index

            if not os.path.exists(export_dir): os.makedirs(export_dir)
            dataframe.to_csv(os.path.join(export_dir, ticker + "_" + _timeframe + ".csv"), index=False,
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
                rates = mt5.copy_rates_from(ticker, timeframe, utc_till, how_many_bars)

                # создадим из полученных данных DataFrame
                rates_frame = pd.DataFrame(rates)
                # сконвертируем время в виде секунд в формат datetime
                if len(rates_frame.index):
                    rates_frame['time'] = pd.to_datetime(rates_frame['time'], unit='s')

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
                _tick_volume = rates_frame.at[i, "tick_volume"]
                _real_volume = rates_frame.at[i, "real_volume"]
                print(i, _time, _open, _high, _low, _close, _tick_volume, _real_volume)

                if _time >= last_bar_time and _time < next_bar_time:
                    # let's insert row in table
                    self.cursor.execute(
                        "INSERT INTO `" + table_name + "` (time, open, high, low, close, volume, tick_volume) "
                                                        "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                        (_time, _open, _high, _low, _close, _real_volume, _tick_volume))

            # to commit changes to db!!!
            # run this command:
            self.conn.commit()

            last_bar_time = next_bar_time

        # ----------------------- Обновление в реальном времени -----------------------

        pass
