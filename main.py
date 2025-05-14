#pip install opencv-python mysqlclient MetaTrader5 pandas pytz

from core.get_shares_data_processor import SharesDataLoader
import MetaTrader5 as mt5     # импортируем модуль для подключения к MetaTrader5
import datetime
from threading import Thread    # для поточной закачки разных датафреймов
import cv2
import pandas as pd
import MySQLdb                  # импортируем модуль для работы с БД MySql # импортируем модуль для подключения к MetaTrader5
import pandas as pd             # импортируем модуль pandas для вывода полученных данных в табличной форме
import time, datetime
import pytz

pd.set_option('display.max_columns', 500) # сколько столбцов показываем
pd.set_option('display.width', 1500)      # макс. ширина таблицы для показа

def main():
    number = int(input("Введите номер актива, данные по которому хотите скачать -->"))
    if number == 1: # SBER(1) SBERP(2) BRENTF(3) SBERF(4) RTSI(5) IMOEX(6)
        ticker = "SBER"         # stocks indexes futures
        market = "stocks"
    elif number == 2:
        ticker = "SBERP"
        market = "stocks"
    elif number == 3:
        ticker = "BRENTF"
        market = "futures"
    elif number == 4:
        ticker = "SBERF"
        market = "futures"
    elif number == 5:
        ticker = "RTSI"
        market = "indexes"
    elif number == 6:
        ticker = "IMOEX"
        market = "indexes"
    elif number == 7:
        ticker = "VTBR"
        market = "stocks"
    elif number == 8:
        ticker = "BSPB"
        market = "stocks"
    elif number == 9:
        ticker = "MGNT"
        market = "stocks"
    elif number == 10:
        ticker = "MTSS"
        market = "stocks"
    elif number == 11:
        ticker = "PLZL"
        market = "stocks"
    elif number == 12:
        ticker = "PHOR"
        market = "stocks"
    timeframe = mt5.TIMEFRAME_D1
    how_many_bars = 70000
    load_data = SharesDataLoader(ticker, ticker, market)
#    load_data.connect_to_metatrader5(path=f"C:\Program Files\MetaTrader 5\terminal64.exe")
    load_data.connect_to_db( host="127.0.0.1",
                            user="root",
                            passwd="DEN123",
                            db="bd_for_action")
    load_data.always_get_share_data(ticker=ticker, timeframe=timeframe)

    load_data.disconnect_from_metatrader5()
    #load_data.export_to_csv(ticker=ticker, timeframe=timeframe, how_many_bars=how_many_bars, export_dir="csv_export")

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
