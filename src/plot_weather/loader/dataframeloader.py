import logging
from datetime import datetime, timedelta
from io import StringIO
from typing import List, Optional, Tuple

import pandas as pd
from psycopg2.extensions import connection

from plot_weather.dao.weatherdao import WeatherDao
from plot_weather.util.date_util import FMT_ISO8601

"""　WeatherDaoからDataFrameを生成するモジュール　"""

# 気象データカラム
COL_TIME: str = "measurement_time"
COL_TEMP_OUT: str = "temp_out"
COL_TEMP_IN: str = "temp_in"
COL_HUMID: str = "humid"
COL_PRESSURE: str = "pressure"
# CSVバッファのヘッダー
HEADER: str = f'"{COL_TIME}","{COL_TEMP_OUT}","{COL_TEMP_IN}","{COL_HUMID}","{COL_PRESSURE}"'


def _csvToStringIO(
        tuple_list: List[Tuple[str, float, float, float, float]]) -> StringIO:
    str_buffer = StringIO()
    str_buffer.write(HEADER + "\n")

    for (m_time, temp_in, temp_out, humid, pressure) in tuple_list:
        line = f'"{m_time}",{temp_in},{temp_out},{humid},{pressure}\n'
        str_buffer.write(line)

    # StringIO need Set first position
    str_buffer.seek(0)
    return str_buffer


def loadTodayDataFrame(
        conn: connection, device_name: str, today_iso8601: str,
        logger: Optional[Optional[logging.Logger]] = None,
        logger_debug: bool = False
) -> Tuple[int, Optional[pd.DataFrame]]:
    """
    当日の観測データのDataFrameを取得
    :param conn: psycopg2 connection
    :param device_name: デバイス名
    :param today_iso8601: 当日(最終登録日) ※ISO8601形式の文字列
    :param logger: app_logger
    :param logger_debug: デバック出力可否 default False
    :return: レコード有り(件数, DataFrame), レコード無し(0, None)
    """
    dao: WeatherDao = WeatherDao(conn, logger=logger)
    data_list: List[Tuple[str, float, float, float, float]] = dao.getTodayData(
        device_name, today_iso8601
    )
    rec_count: int = len(data_list)
    if rec_count == 0:
        # 該当レコード無し
        return rec_count, None

    csv_buffer: StringIO = _csvToStringIO(data_list)
    df: pd.DataFrame = pd.read_csv(csv_buffer, header=0, parse_dates=[COL_TIME])
    # 測定時刻をデータフレームのインデックスに設定
    df.index = df[COL_TIME]
    if logger is not None and logger_debug:
        logger.debug(f"{df}")
    return rec_count, df


def loadMonthDataFrame(
        conn: connection, device_name: str, year_month: str,
        logger: Optional[logging.Logger] = None, logger_debug: bool = False
) -> Tuple[int, Optional[pd.DataFrame]]:
    """
    指定された検索年月の観測データのDataFrameを取得
    :param conn: psycopg2 connection
    :param device_name: デバイス名
    :param year_month: 検索年月
    :param logger: app_logger
    :param logger_debug: デバック出力可否 default False
    :return: レコード有り(件数, DataFrame)、レコード無し(0, None)
    """
    dao: WeatherDao = WeatherDao(conn, logger=logger)
    data_list: List[Tuple[str, float, float, float, float]] = dao.getMonthData(
        device_name, year_month
    )
    rec_count: int = len(data_list)
    if rec_count == 0:
        return rec_count, None

    csv_buffer: StringIO = _csvToStringIO(data_list)
    df: pd.DataFrame = pd.read_csv(csv_buffer, header=0, parse_dates=[COL_TIME])
    df.index = df[COL_TIME]
    if logger is not None and logger_debug:
        logger.debug(f"{df}")
    return rec_count, df


def loadBeforeDaysRangeDataFrame(
        conn: connection, device_name: str, end_date: str, before_days: int,
        logger: Optional[logging.Logger] = None, logger_debug: bool = False
) -> Tuple[int, Optional[pd.DataFrame]]:
    """
    指定された期間の観測データのDataFrameを取得
    :param conn: psycopg2 connection
    :param device_name: デバイス名
    :param end_date: 検索終了日 ※ISO8601形式文字列
    :param before_days: N日 ※検索終了日からN日以前
    :param logger: app_logger
    :param logger_debug: デバック出力可否 default False
    :return: レコード有り(件数, DataFrame)、レコード無し(0, None)
    """
    dt_end: datetime = datetime.strptime(end_date, FMT_ISO8601)
    dt_from: datetime = dt_end - timedelta(days=before_days)
    from_date: str = dt_from.strftime(FMT_ISO8601)
    to_date: str = dt_end.strftime(FMT_ISO8601)
    if logger is not None and logger_debug:
        logger.debug(f"from_date: {from_date}, to_date: {to_date}")

    dao: WeatherDao = WeatherDao(conn, logger=logger)
    data_list: List[Tuple[str, float, float, float, float]] = dao.getFromToRangeData(
        device_name, from_date, to_date
    )
    rec_count: int = len(data_list)
    if rec_count == 0:
        return rec_count, None

    csv_buffer: StringIO = _csvToStringIO(data_list)
    df: pd.DataFrame = pd.read_csv(csv_buffer, header=0, parse_dates=[COL_TIME])
    df.index = df[COL_TIME]
    if logger is not None and logger_debug:
        logger.debug(f"{df}")
    return rec_count, df
