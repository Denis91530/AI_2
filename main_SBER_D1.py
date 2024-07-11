#pip install opencv-python mysqlclient MetaTrader5 pandas pytz

from core.get_shares_data_processor import SharesDataLoader
import MetaTrader5 as mt5     # импортируем модуль для подключения к MetaTrader5
import datetime
from threading import Thread    # для поточной закачки разных датафреймов
import cv2
import pandas as pd
import os
import cv2
import MySQLdb                  # импортируем модуль для работы с БД MySql    # импортируем модуль для подключения к MetaTrader5
import pandas as pd             # импортируем модуль pandas для вывода полученных данных в табличной форме
import time, datetime
import pytz

pd.set_option('display.max_columns', 500) # сколько столбцов показываем
pd.set_option('display.width', 1500)      # макс. ширина таблицы для показа


def main():
    ticker = "_LKOH"
    timeframe = mt5.TIMEFRAME_D1
    how_many_bars = 70000
    load_data = SharesDataLoader("_LKOH")
    load_data.connect_to_metatrader5(path=f"C:\Program Files\MetaTrader 5\terminal64.exe")
    load_data.connect_to_db( host="127.0.0.1",
                            user="root",
                            passwd="DEN123",
                            db="bd_for_action")
    load_data.always_get_share_data(ticker=ticker, timeframe=timeframe)

    load_data.disconnect_from_metatrader5()
    load_data.export_to_csv(ticker=ticker, timeframe=timeframe, how_many_bars=how_many_bars, export_dir="csv_export")

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
